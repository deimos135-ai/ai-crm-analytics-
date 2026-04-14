#!/usr/bin/env python3
"""
Bitrix24 -> OpenAI Transcribe -> Telegram monitor

v2:
- стійкий checklist через criterion_key
- м'яка валідація coaching.one_sentence_tip
- skip повного QA при слабкому транскрипті
- price objection у weekly report
"""

import csv
import json
import os
import pathlib
import re
import time
import traceback
import typing as t
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests


# -------------------- Config --------------------
STATE_FILE = os.getenv("STATE_FILE", "b24_monitor_state.json")
BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
LIMIT_LAST = int(os.getenv("LIMIT_LAST", "10"))

LANGUAGE_HINT = (os.getenv("LANGUAGE_HINT") or "uk").strip().lower()

OPENAI_ANALYSIS_MODEL = os.getenv("OPENAI_ANALYSIS_MODEL", "gpt-4o")
OPENAI_TRANSCRIBE_MODEL = os.getenv("OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe")

TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "90"))
OPENAI_MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "2"))

MAX_AUDIO_MB = int(os.getenv("MAX_AUDIO_MB", "25"))
MAX_TRANSCRIPT_CHARS = int(os.getenv("MAX_TRANSCRIPT_CHARS", "7000"))
MIN_TRANSCRIPT_TRUST_FOR_FULL_QA = int(os.getenv("MIN_TRANSCRIPT_TRUST_FOR_FULL_QA", "45"))

ONLY_INCOMING = (os.getenv("ONLY_INCOMING", "true").lower() == "true")
INCOMING_CODE = os.getenv("INCOMING_CODE", "1")
MIN_DURATION_SEC = int(os.getenv("MIN_DURATION_SEC", "10"))

WEEKLY_TZ = os.getenv("WEEKLY_TZ", "Europe/Kyiv")
WEEKLY_REPORT_DAY = os.getenv("WEEKLY_REPORT_DAY", "Fri")
WEEKLY_REPORT_HOUR = int(os.getenv("WEEKLY_REPORT_HOUR", "18"))
WEEKLY_KEEP_DAYS = int(os.getenv("WEEKLY_KEEP_DAYS", "35"))

CALLS_FILE = os.getenv("CALLS_FILE", "calls_week.jsonl")
WEEKLY_STATE_FILE = os.getenv("WEEKLY_STATE_FILE", "weekly_state.json")
CSV_FILENAME = os.getenv("WEEKLY_CSV_NAME", "weekly_calls.csv")

SHOW_EVIDENCE_IN_TG = (os.getenv("SHOW_EVIDENCE_IN_TG", "false").lower() == "true")
PROCESSED_KEEP = int(os.getenv("PROCESSED_KEEP", "800"))

if BITRIX_WEBHOOK_BASE and not BITRIX_WEBHOOK_BASE.endswith("/"):
    BITRIX_WEBHOOK_BASE += "/"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "ai-crm-analytics/3.1"})


# -------------------- QA constants --------------------
QA_CRITERIA = [
    ("greeting_intro", "Привітався та представився"),
    ("clarified_issue", "Чітко з’ясував суть звернення"),
    ("polite_supportive", "Використовував ввічливі та підтримувальні формулювання"),
    ("professional_focus", "Говорив професійно та по суті"),
    ("gave_solution", "Правильно надав відповідь / рішення"),
    ("next_steps_deadline", "Пояснив наступні дії або строки"),
    ("offered_extra_help", "Запропонував допомогу наприкінці"),
    ("closed_politely", "Подякував і завершив розмову коректно"),
]

QA_LABELS = [label for _, label in QA_CRITERIA]
QA_KEYS = [key for key, _ in QA_CRITERIA]

ALLOWED_TAGS = [
    "Технічні проблеми",
    "Тарифи",
    "Дорого / заперечення по ціні",
    "Підключення",
    "Платежі / рахунок",
    "Скарги",
    "Повторні звернення",
    "Ризик відтоку / утримання",
    "Інформаційні звернення",
    "Продаж / допродаж",
    "Організаційні питання",
]


# -------------------- Data --------------------
@dataclass
class CallItem:
    id: str
    call_id: str
    call_start: str
    duration: t.Optional[int]
    record_url: t.Optional[str]
    crm_entity_type: t.Optional[str]
    crm_entity_id: t.Optional[str]
    crm_activity_id: t.Optional[str]
    phone_number: t.Optional[str]
    call_type: t.Optional[str]


# -------------------- Utils --------------------
def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _strip_html(s: str) -> str:
    s = (s or "")
    return s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _to_bool_text_ua(v: t.Optional[bool]) -> str:
    if v is True:
        return "так"
    if v is False:
        return "ні"
    return "невідомо"


def _mask_phone(phone: str) -> str:
    p = re.sub(r"\D", "", phone or "")
    if len(p) >= 4:
        return f"+***{p[-4:]}"
    return phone or "—"


def _require_env(name: str) -> bool:
    val = os.getenv(name, "")
    if not val:
        print(f"[monitor] WARN missing env {name}", flush=True)
        return False
    return True


def _sleep_backoff(attempt: int) -> None:
    time.sleep(1.2 * (attempt + 1))


def post_with_retry(
    url: str,
    *,
    headers: t.Optional[dict] = None,
    json_body: t.Optional[dict] = None,
    data: t.Optional[dict] = None,
    files: t.Optional[dict] = None,
    timeout: int = 60,
    retries: int = 2,
) -> requests.Response:
    last_err = None
    for attempt in range(retries + 1):
        try:
            resp = SESSION.post(
                url,
                headers=headers,
                json=json_body,
                data=data,
                files=files,
                timeout=timeout,
            )
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(
                    f"Retryable status: {resp.status_code}: {resp.text[:1200]}",
                    response=resp,
                )
            return resp
        except Exception as e:
            last_err = e
            if attempt >= retries:
                raise
            _sleep_backoff(attempt)
    raise last_err


def http_post_json(url: str, payload: dict) -> dict:
    resp = post_with_retry(
        url,
        json_body=payload,
        timeout=TIMEOUT,
        retries=OPENAI_MAX_RETRIES,
    )
    if resp.status_code >= 400:
        print(f"[http] {resp.status_code} POST {url} -> {resp.text[:2000]}", flush=True)
        resp.raise_for_status()
    return resp.json()


# -------------------- Trust metrics --------------------
def compute_transcript_trust(transcript: str, duration_sec: t.Optional[int]) -> int:
    if not transcript:
        return 0

    words = len(re.findall(r"[A-Za-zА-Яа-яІіЇїЄє0-9']+", transcript))
    if not duration_sec or duration_sec <= 0:
        return int(round(_clamp((len(transcript) / 1200) * 100, 20, 95)))

    minutes = duration_sec / 60.0
    expected = max(30, int(minutes * 110))
    ratio = _clamp(words / expected, 0.0, 1.2)

    trust = 100 * _clamp(ratio / 0.9, 0.0, 1.0)
    return int(round(trust))


def compute_analysis_trust(analysis_obj: dict) -> int:
    cl = analysis_obj.get("checklist") or []
    if not isinstance(cl, list) or not cl:
        return 0

    confs = []
    keys = set()

    for it in cl:
        if not isinstance(it, dict):
            continue
        ck = it.get("criterion_key")
        if isinstance(ck, str):
            keys.add(ck)
        conf = it.get("confidence")
        if isinstance(conf, (int, float)):
            confs.append(float(conf))

    if len(keys) != len(QA_KEYS):
        return 0
    if not confs:
        return 0

    avg = sum(confs) / len(confs)
    return int(round(100 * _clamp(avg, 0.0, 1.0)))


def compute_overall_trust(transcript_trust: int, analysis_trust: int) -> int:
    return min(int(transcript_trust), int(analysis_trust))


def trust_badge(overall: int) -> tuple[str, str]:
    if overall >= 85:
        return "✅", "висока"
    if overall >= 60:
        return "⚠️", "середня"
    return "❌", "низька"


# -------------------- Bitrix24 --------------------
def b24_vox_get_total() -> int:
    url = f"{BITRIX_WEBHOOK_BASE}voximplant.statistic.get.json"
    data = {"ORDER": {"CALL_START_DATE": "DESC"}, "LIMIT": 1}
    js = http_post_json(url, data)
    res = js.get("result") or js
    total = None
    if isinstance(res, dict):
        total = res.get("total")
    if total is None:
        total = js.get("total")
    if total is None:
        raise RuntimeError(f"Can't find 'total' in response: {js}")
    return int(total)


def b24_vox_get_latest(limit: int) -> t.List[CallItem]:
    total = b24_vox_get_total()
    start = max(total - limit, 0)
    url = f"{BITRIX_WEBHOOK_BASE}voximplant.statistic.get.json"
    data = {"ORDER": {"CALL_START_DATE": "DESC"}, "LIMIT": limit, "start": start}
    js = http_post_json(url, data)
    res = js.get("result") or js

    items: t.List[dict] = []
    if isinstance(res, dict):
        for v in res.values():
            if isinstance(v, dict) and "CALL_ID" in v:
                items.append(v)
        if not items and isinstance(res.get("items"), list):
            items = res["items"]
    elif isinstance(res, list):
        items = res

    result: t.List[CallItem] = []
    for it in items:
        try:
            result.append(
                CallItem(
                    id=str(it.get("ID")),
                    call_id=str(it.get("CALL_ID")),
                    call_start=str(it.get("CALL_START_DATE")),
                    duration=int(it.get("CALL_DURATION"))
                    if it.get("CALL_DURATION") not in (None, "", "empty")
                    else None,
                    record_url=(it.get("CALL_RECORD_URL") if it.get("CALL_RECORD_URL") not in (None, "", "empty") else None),
                    crm_entity_type=(it.get("CRM_ENTITY_TYPE") or None),
                    crm_entity_id=(it.get("CRM_ENTITY_ID") or None),
                    crm_activity_id=(it.get("CRM_ACTIVITY_ID") or None),
                    phone_number=(it.get("PHONE_NUMBER") or None),
                    call_type=(str(it.get("CALL_TYPE")) if it.get("CALL_TYPE") not in (None, "", "empty") else None),
                )
            )
        except Exception:
            continue

    result = [
        r
        for r in result
        if (r.duration and r.duration >= MIN_DURATION_SEC and r.record_url)
        and (not ONLY_INCOMING or (r.call_type == INCOMING_CODE))
    ]
    result = sorted(result, key=lambda x: x.call_start, reverse=True)[:limit]
    return result


# -------------------- CRM helpers --------------------
def _portal_base_from_webhook() -> str:
    try:
        return BITRIX_WEBHOOK_BASE.split("/rest/")[0].rstrip("/") + "/"
    except Exception:
        return BITRIX_WEBHOOK_BASE


def b24_get_entity_name(entity_type: str, entity_id: str) -> str:
    if not entity_type or not entity_id:
        return "—"
    et = entity_type.upper()
    method = None
    if et == "CONTACT":
        method = "crm.contact.get.json"
    elif et == "LEAD":
        method = "crm.lead.get.json"
    elif et == "COMPANY":
        method = "crm.company.get.json"
    else:
        return "—"

    try:
        js = http_post_json(f"{BITRIX_WEBHOOK_BASE}{method}", {"ID": str(entity_id)})
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        print(f"[b24] name fetch failed {code}: {e}", flush=True)
        return "—"

    data = js.get("result", {}) or {}
    parts = []
    for k in ("NAME", "SECOND_NAME", "LAST_NAME"):
        v = data.get(k)
        if v:
            parts.append(str(v).strip())
    name = " ".join(parts).strip()
    if not name:
        name = str(data.get("TITLE", "")).strip() or "—"
    return name


def b24_entity_link(entity_type: str, entity_id: str, activity_id: t.Optional[str] = None) -> str:
    base = _portal_base_from_webhook()
    et = (entity_type or "").upper()
    path_map = {
        "CONTACT": "crm/contact/details/",
        "LEAD": "crm/lead/details/",
        "DEAL": "crm/deal/details/",
        "COMPANY": "crm/company/details/",
    }
    if activity_id:
        return f"{base}crm/activity/?open_view={activity_id}"
    path = path_map.get(et)
    return f"{base}{path}{entity_id}/" if path and entity_id else base


# -------------------- Audio fetch --------------------
def fetch_audio(url: str, max_mb: int = 25) -> tuple[bytes, str, str]:
    headers = {"Accept": "*/*"}
    max_bytes = max_mb * 1024 * 1024

    with SESSION.get(url, headers=headers, stream=True, allow_redirects=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        mime = (r.headers.get("Content-Type", "").split(";")[0].strip().lower())
        clen = r.headers.get("Content-Length")

        if clen is not None:
            try:
                size_bytes = int(clen)
                if size_bytes > max_bytes:
                    raise RuntimeError(f"Audio too large: {size_bytes} bytes > {max_mb}MB limit")
            except Exception:
                pass

        buf = bytearray()
        for chunk in r.iter_content(chunk_size=65536):
            if not chunk:
                continue
            buf.extend(chunk)
            if len(buf) > max_bytes:
                raise RuntimeError(f"Audio exceeded {max_mb}MB during download")

    data = bytes(buf)

    if not mime or mime in ("text/html", "application/xml", "text/plain"):
        lower = url.lower()
        if lower.endswith(".mp3"):
            mime = "audio/mpeg"
        elif lower.endswith(".wav"):
            mime = "audio/wav"
        elif lower.endswith(".m4a"):
            mime = "audio/mp4"
        else:
            if len(data) < 1024:
                raise RuntimeError(f"Unexpected content-type '{mime}' and tiny body ({len(data)} bytes)")
            mime = "audio/mpeg"

    if len(data) < 400:
        raise RuntimeError(f"Downloaded audio too small: {len(data)} bytes")

    filename = "audio"
    if ".mp3" in url.lower():
        filename += ".mp3"
    elif ".wav" in url.lower():
        filename += ".wav"
    elif ".m4a" in url.lower():
        filename += ".m4a"
    else:
        ext = {
            "audio/mpeg": ".mp3",
            "audio/wav": ".wav",
            "audio/x-wav": ".wav",
            "audio/mp4": ".m4a",
            "audio/x-m4a": ".m4a",
            "audio/aac": ".aac",
            "audio/ogg": ".ogg",
            "audio/webm": ".webm",
        }.get(mime, ".mp3")
        filename += ext

    return data, mime, filename


# -------------------- OpenAI: Transcription --------------------
def transcribe_audio(audio_bytes: bytes, filename: str = "audio.mp3", mime: str = "audio/mpeg") -> str:
    url = "https://api.openai.com/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}

    initial_prompt = (
        "Транскрибуй українською мовою. "
        "Зберігай природну українську орфографію. "
        "Коректно розпізнавай слова: тариф, рахунок, підключення, заявка, майстер, "
        "роутер, інтернет, швидкість, договір, абонент, номер, оплата."
    )

    files = {"file": (filename, audio_bytes, mime)}
    data = {
        "model": OPENAI_TRANSCRIBE_MODEL,
        "language": LANGUAGE_HINT or "uk",
        "temperature": 0,
        "prompt": initial_prompt,
    }

    r = post_with_retry(
        url,
        headers=headers,
        data=data,
        files=files,
        timeout=OPENAI_TIMEOUT,
        retries=OPENAI_MAX_RETRIES,
    )

    if r.status_code >= 400:
        try:
            err = r.text
        except Exception:
            err = "<no body>"
        raise requests.HTTPError(f"OpenAI audio/transcriptions {r.status_code}: {err}", response=r)

    return (r.json().get("text", "") or "").strip()


# -------------------- Transcript helpers --------------------
def _trim_segment(s: str, limit: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= limit else s[:limit].rstrip() + "…"


def _segment_transcript(text: str) -> dict:
    ttxt = (text or "").strip()
    if not ttxt:
        return {"intro": "", "middle": "", "outro": ""}

    if MAX_TRANSCRIPT_CHARS > 0 and len(ttxt) > MAX_TRANSCRIPT_CHARS:
        ttxt = ttxt[:MAX_TRANSCRIPT_CHARS].rstrip() + "…"

    n = len(ttxt)
    intro_len = min(1800, n)
    outro_len = min(1800, max(0, n - intro_len))

    intro = ttxt[:intro_len].strip()
    outro = ttxt[-outro_len:].strip() if outro_len > 0 else ttxt[-min(900, n):].strip()

    if n <= intro_len + outro_len + 50:
        middle = ttxt[intro_len:].strip()
    else:
        mid_start = max(intro_len, (n // 2) - 1200)
        mid_end = min(n - outro_len, (n // 2) + 1200)
        middle = ttxt[mid_start:mid_end].strip()

    return {
        "intro": _trim_segment(intro, 1800),
        "middle": _trim_segment(middle, 2400),
        "outro": _trim_segment(outro, 1800),
    }


# -------------------- OpenAI: Analysis --------------------
def analyze_and_summarize(transcript: str, call_duration_sec: t.Optional[int] = None) -> tuple[str, str, str, int, dict]:
    if not transcript:
        return (
            "Немає транскрипту для аналізу.",
            "Немає даних для резюме.",
            "Інформаційні звернення",
            0,
            {"error": "empty_transcript"},
        )

    seg = _segment_transcript(transcript)

    def _build_messages(fix_note: str = "") -> list[dict]:
        developer = (
            "Ти — провідний QA-аналітик кол-центру. "
            "Відповідай ТІЛЬКИ УКРАЇНСЬКОЮ. "
            "ПОВЕРТАЙ СУВОРО JSON без тексту поза JSON. "
            "НЕ вигадуй факти: якщо ознаки немає в тексті або вона нечітка — став 0. "
            "Оцінюй лише те, що прямо видно з тексту транскрипту. "
            "Не роби висновків про інтонацію, агресивний тон, перебивання або емоційне забарвлення голосу, якщо цього немає в словах. "
            "Оцінка по кожному критерію тільки 0 або 1. "
            "1 = критерій чітко виконаний і є підтвердження в тексті. "
            "0 = не виконаний, сумнівний або бракує доказів. "
            "Якщо не впевнений — став 0. "
            "Поверни рівно такий JSON-об'єкт:\n"
            "{\n"
            "  \"facts\": {\n"
            "    \"operator_greeted\": true,\n"
            "    \"operator_introduced_self\": true,\n"
            "    \"clarified_issue\": true,\n"
            "    \"used_polite_supportive_phrases\": true,\n"
            "    \"spoke_professionally\": true,\n"
            "    \"gave_solution\": true,\n"
            "    \"mentioned_deadline\": false,\n"
            "    \"offered_extra_help\": false,\n"
            "    \"closed_politely\": true\n"
            "  },\n"
            "  \"checklist\": [\n"
            "    {\"criterion_key\":\"greeting_intro\",\"score\":0,\"note\":\"...\",\"evidence\":\"...\",\"confidence\":0.0},\n"
            "    {\"criterion_key\":\"clarified_issue\",\"score\":0,\"note\":\"...\",\"evidence\":\"...\",\"confidence\":0.0},\n"
            "    {\"criterion_key\":\"polite_supportive\",\"score\":0,\"note\":\"...\",\"evidence\":\"...\",\"confidence\":0.0},\n"
            "    {\"criterion_key\":\"professional_focus\",\"score\":0,\"note\":\"...\",\"evidence\":\"...\",\"confidence\":0.0},\n"
            "    {\"criterion_key\":\"gave_solution\",\"score\":0,\"note\":\"...\",\"evidence\":\"...\",\"confidence\":0.0},\n"
            "    {\"criterion_key\":\"next_steps_deadline\",\"score\":0,\"note\":\"...\",\"evidence\":\"...\",\"confidence\":0.0},\n"
            "    {\"criterion_key\":\"offered_extra_help\",\"score\":0,\"note\":\"...\",\"evidence\":\"...\",\"confidence\":0.0},\n"
            "    {\"criterion_key\":\"closed_politely\",\"score\":0,\"note\":\"...\",\"evidence\":\"...\",\"confidence\":0.0}\n"
            "  ],\n"
            "  \"summary\": \"...\",\n"
            "  \"tag\": \"...\",\n"
            "  \"root_reason\": \"...\",\n"
            "  \"resolved_on_first_contact\": true,\n"
            "  \"repeat_contact_signal\": false,\n"
            "  \"price_objection\": false,\n"
            "  \"price_objection_note\": \"...\",\n"
            "  \"churn_risk\": \"low\",\n"
            "  \"customer_emotion\": \"neutral\",\n"
            "  \"next_step_promised\": \"...\",\n"
            "  \"deadline_promised\": \"...\",\n"
            "  \"coaching\": {\n"
            "    \"top_issues\": [\"...\", \"...\"],\n"
            "    \"one_sentence_tip\": \"...\"\n"
            "  },\n"
            "  \"risk_flags\": [\"...\"]\n"
            "}\n"
            "Для кожного елемента checklist ОБОВ'ЯЗКОВО вкажи правильний criterion_key. "
            "Не змінюй назви ключів. "
            "У checklist мають бути всі 8 criterion_key рівно один раз. "
            "Порядок елементів у checklist може бути будь-який, але ключі не можна пропускати або дублювати. "
            f"Дозволені tag: {', '.join(ALLOWED_TAGS)}. "
            "Пріоритет tag при змішаних темах: "
            "1) Ризик відтоку / утримання, "
            "2) Дорого / заперечення по ціні, "
            "3) Повторні звернення, "
            "4) Скарги, "
            "5) Продаж / допродаж, "
            "6) Підключення, "
            "7) Тарифи, "
            "8) Платежі / рахунок, "
            "9) Організаційні питання, "
            "10) Інформаційні звернення, "
            "11) Технічні проблеми. "
            "Якщо клієнт скаржиться на майстра, оператора, довге вирішення або сервіс — tag = 'Скарги'. "
            "Якщо клієнт прямо каже, що звертається повторно з того ж питання — tag = 'Повторні звернення'. "
            "Якщо клієнт говорить про відключення, конкурента, розірвання договору, утримання — tag = 'Ризик відтоку / утримання'. "
            "Якщо клієнт каже, що йому дорого, не влаштовує вартість, хоче дешевший тариф, просить знижку, "
            "не готовий платити стільки або порівнює ціну з дешевшими альтернативами — "
            "tag = 'Дорого / заперечення по ціні', а також price_objection = true. "
            + (f" ДОДАТКОВО: {fix_note}" if fix_note else "")
        )

        user = f"""
Зроби аналіз ВХІДНОГО дзвінка за 8 критеріями у заданому форматі.

Критерії:
1) greeting_intro — Привітався та представився
2) clarified_issue — Чітко з’ясував суть звернення
3) polite_supportive — Використовував ввічливі та підтримувальні формулювання
4) professional_focus — Говорив професійно та по суті
5) gave_solution — Правильно надав відповідь / рішення
6) next_steps_deadline — Пояснив наступні дії або строки
7) offered_extra_help — Запропонував допомогу наприкінці
8) closed_politely — Подякував і завершив розмову коректно

Правила:
- називати компанію НЕ обов’язково
- не оцінюй інтонацію або перебивання, якщо цього не видно з тексту
- якщо даних недостатньо — став 0
- summary у форматі:
  "Клієнт звернувся з [коротка причина]. Оператор [що зробив / яке рішення запропонував]."

Контекст:
- Тривалість дзвінка (сек): {call_duration_sec if call_duration_sec is not None else "невідомо"}

Транскрипт:
INTRO:
---
{seg["intro"]}
---
MIDDLE:
---
{seg["middle"]}
---
OUTRO:
---
{seg["outro"]}
---
"""
        return [
            {"role": "developer", "content": developer},
            {"role": "user", "content": user},
        ]

    def _call_openai(messages: list[dict]) -> dict:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }

        payload: dict[str, t.Any] = {
            "model": OPENAI_ANALYSIS_MODEL,
            "messages": messages,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }

        model_name = (OPENAI_ANALYSIS_MODEL or "").lower()
        if model_name.startswith("gpt-5") or model_name.startswith("o"):
            payload["max_completion_tokens"] = 1600
        else:
            payload["max_tokens"] = 1600

        r = post_with_retry(
            url,
            headers=headers,
            json_body=payload,
            timeout=OPENAI_TIMEOUT,
            retries=OPENAI_MAX_RETRIES,
        )

        if r.status_code >= 400:
            try:
                err_body = r.text
            except Exception:
                err_body = "<no body>"
            raise requests.HTTPError(
                f"OpenAI chat/completions {r.status_code}: {err_body[:2500]}",
                response=r,
            )

        content = r.json()["choices"][0]["message"]["content"]
        return json.loads(content)

    def _normalize_coaching(obj: dict) -> None:
        coaching = obj.get("coaching")
        if not isinstance(coaching, dict):
            obj["coaching"] = {
                "top_issues": ["Немає суттєвих зауважень", "—"],
                "one_sentence_tip": "Працюйте за структурою розмови та чітко фіксуйте наступні дії.",
            }
            return

        top_issues = coaching.get("top_issues")
        if not isinstance(top_issues, list) or len(top_issues) != 2 or not all(isinstance(x, str) for x in top_issues):
            coaching["top_issues"] = ["Немає суттєвих зауважень", "—"]

        tip = coaching.get("one_sentence_tip")
        if not isinstance(tip, str):
            tip = ""
        tip = tip.strip()
        coaching["one_sentence_tip"] = tip or "Працюйте за структурою розмови та чітко фіксуйте наступні дії."

    def _validate(obj: t.Any) -> tuple[bool, str]:
        if not isinstance(obj, dict):
            return False, "root not object"

        _normalize_coaching(obj)

        facts = obj.get("facts")
        if not isinstance(facts, dict):
            return False, "facts missing or not object"

        required_fact_keys = [
            "operator_greeted",
            "operator_introduced_self",
            "clarified_issue",
            "used_polite_supportive_phrases",
            "spoke_professionally",
            "gave_solution",
            "mentioned_deadline",
            "offered_extra_help",
            "closed_politely",
        ]
        for k in required_fact_keys:
            if k not in facts or not isinstance(facts[k], bool):
                return False, f"facts.{k} invalid"

        cl = obj.get("checklist")
        if not isinstance(cl, list) or len(cl) != 8:
            return False, "checklist must be list length 8"

        allowed_keys = set(QA_KEYS)
        seen_keys = set()

        for i, item in enumerate(cl):
            if not isinstance(item, dict):
                return False, f"checklist[{i}] not object"

            ck = item.get("criterion_key")
            if ck not in allowed_keys:
                return False, f"checklist[{i}].criterion_key invalid"

            if ck in seen_keys:
                return False, f"duplicate criterion_key: {ck}"
            seen_keys.add(ck)

            sc = item.get("score")
            note = item.get("note")
            ev = item.get("evidence", "")
            conf = item.get("confidence")

            if sc not in (0, 1):
                return False, f"checklist[{i}].score invalid"

            if not isinstance(note, str):
                return False, f"checklist[{i}].note not string"
            note_s = note.strip()
            if len(note_s) < 6 or len(note_s) > 220:
                return False, f"checklist[{i}].note bad length"

            if not isinstance(ev, str):
                return False, f"checklist[{i}].evidence not string"
            if len(ev) > 180:
                return False, f"checklist[{i}].evidence too long"

            if not isinstance(conf, (int, float)):
                return False, f"checklist[{i}].confidence not number"
            if conf < 0 or conf > 1:
                return False, f"checklist[{i}].confidence out of range"

            if sc == 1 and float(conf) < 0.75:
                return False, f"checklist[{i}] score=1 with conf<0.75"

            if note_s.lower() in ("так", "ні", "ок", "добре"):
                return False, f"checklist[{i}] trivial note"

        if seen_keys != allowed_keys:
            return False, "missing criterion_key(s)"

        tag = obj.get("tag")
        if tag not in ALLOWED_TAGS:
            return False, "tag not allowed"

        summary = obj.get("summary")
        if not isinstance(summary, str) or len(summary.strip()) < 10:
            return False, "summary too short"

        root_reason = obj.get("root_reason")
        if not isinstance(root_reason, str) or len(root_reason.strip()) < 1:
            return False, "root_reason invalid"

        roc = obj.get("resolved_on_first_contact")
        if roc not in (True, False, None):
            return False, "resolved_on_first_contact invalid"

        rcs = obj.get("repeat_contact_signal")
        if not isinstance(rcs, bool):
            return False, "repeat_contact_signal invalid"

        po = obj.get("price_objection")
        if not isinstance(po, bool):
            return False, "price_objection invalid"

        pon = obj.get("price_objection_note")
        if not isinstance(pon, str):
            return False, "price_objection_note invalid"

        churn_risk = obj.get("churn_risk")
        if churn_risk not in ("low", "medium", "high"):
            return False, "churn_risk invalid"

        emotion = obj.get("customer_emotion")
        if emotion not in ("calm", "annoyed", "angry", "frustrated", "neutral"):
            return False, "customer_emotion invalid"

        nsp = obj.get("next_step_promised")
        if not isinstance(nsp, str):
            return False, "next_step_promised invalid"

        dp = obj.get("deadline_promised")
        if not isinstance(dp, str):
            return False, "deadline_promised invalid"

        coaching = obj.get("coaching")
        if not isinstance(coaching, dict):
            return False, "coaching missing"

        top_issues = coaching.get("top_issues")
        if not isinstance(top_issues, list) or len(top_issues) != 2 or not all(isinstance(x, str) for x in top_issues):
            return False, "coaching.top_issues invalid"

        tip = coaching.get("one_sentence_tip")
        if not isinstance(tip, str):
            return False, "coaching.one_sentence_tip invalid"

        risk_flags = obj.get("risk_flags")
        if not isinstance(risk_flags, list) or not all(isinstance(x, str) for x in risk_flags):
            return False, "risk_flags invalid"

        return True, ""

    obj = _call_openai(_build_messages())
    ok, why = _validate(obj)

    if not ok:
        print(f"[analysis] first validation failed: {why}", flush=True)
        obj = _call_openai(
            _build_messages(
                fix_note=(
                    "Попередня відповідь порушила формат або якість. "
                    "Суворо: checklist рівно 8 елементів; score тільки 0 або 1; "
                    "score=1 лише при confidence >= 0.75; "
                    "обов'язково поверни criterion_key для кожного пункту; "
                    "усі 8 criterion_key мають бути присутні рівно один раз."
                )
            )
        )
        ok, why = _validate(obj)

        if not ok:
            print(f"[analysis] second validation failed: {why}", flush=True)
            fallback = {
                "error": "invalid_format",
                "checklist": [
                    {
                        "criterion_key": key,
                        "score": 0,
                        "note": "Недостатньо валідних даних для оцінки.",
                        "evidence": "",
                        "confidence": 0.0,
                    }
                    for key, _label in QA_CRITERIA
                ],
                "summary": "Не вдалося побудувати повне структуроване резюме.",
                "tag": "Інформаційні звернення",
                "root_reason": "Невідомо",
                "resolved_on_first_contact": None,
                "repeat_contact_signal": False,
                "price_objection": False,
                "price_objection_note": "",
                "churn_risk": "low",
                "customer_emotion": "neutral",
                "next_step_promised": "",
                "deadline_promised": "",
                "coaching": {
                    "top_issues": ["Недостатньо даних для оцінки", "—"],
                    "one_sentence_tip": "Перевір якість транскрипту або запис дзвінка.",
                },
                "risk_flags": [],
            }
            return (
                "⚠️ Аналіз частково недоступний через невалідну структуру відповіді моделі.",
                "Не вдалося побудувати повне структуроване резюме.",
                "Інформаційні звернення",
                0,
                fallback,
            )

    cl = obj["checklist"]
    summary = (obj.get("summary") or "").strip()
    tag = obj.get("tag") or "Інформаційні звернення"
    coaching = obj.get("coaching") or {}
    risks = obj.get("risk_flags") or []

    by_key: dict[str, dict] = {}
    for item in cl:
        if isinstance(item, dict):
            ck = item.get("criterion_key")
            if isinstance(ck, str):
                by_key[ck] = item

    lines: list[str] = []
    score = 0

    for key, label in QA_CRITERIA:
        item = by_key.get(key, {})
        sc = item.get("score", 0)
        note = (item.get("note") or "Немає пояснення.").strip()
        ev = (item.get("evidence") or "").strip()
        conf = float(item.get("confidence") or 0.0)

        emoji = "✅" if sc == 1 else "❌"
        if sc == 1:
            score += 1

        conf_str = ""
        if sc == 0 and 0 < conf < 0.95:
            conf_str = f" (conf {conf:.2f})"

        lines.append(f"{emoji} {label}: {note}{conf_str}")

        if SHOW_EVIDENCE_IN_TG and ev:
            lines.append(f"    <i>«{html_escape(ev)}»</i>")

    checklist_html = "\n".join(html_escape(x) if not x.strip().startswith("<i>") else x for x in lines)
    summary_html = html_escape(summary if summary else "Немає короткого резюме.")

    coach_block = ""
    try:
        ti = coaching.get("top_issues") or []
        tip = coaching.get("one_sentence_tip") or ""
        if ti and tip:
            coach_block = (
                "\n\n<b>Що покращити:</b>\n"
                f"• {html_escape(str(ti[0]))}\n"
                f"• {html_escape(str(ti[1]))}\n"
                f"<b>Порада:</b> {html_escape(str(tip))}"
            )
    except Exception:
        coach_block = ""

    risk_block = ""
    if risks:
        risk_block = "\n\n<b>Ризики:</b>\n" + "\n".join([f"• {html_escape(x)}" for x in risks[:6]])

    checklist_html = checklist_html + coach_block + risk_block
    return checklist_html, summary_html, str(tag), int(score), obj


# -------------------- Telegram --------------------
def tg_send_message(text: str) -> None:
    try:
        if TG_BOT_TOKEN.startswith("sk-"):
            print("[tg] ERROR: TG_BOT_TOKEN схожий на OpenAI ключ (sk-...). Замініть на токен BotFather.", flush=True)
            return

        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        chunk = 3500
        parts = [text[i:i + chunk] for i in range(0, len(text), chunk)] or [text]

        for part in parts:
            payload = {
                "chat_id": TG_CHAT_ID,
                "text": part,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            r = SESSION.post(url, json=payload, timeout=TIMEOUT)
            if r.status_code >= 400:
                print(f"[tg] sendMessage {r.status_code}: {r.text[:300]}", flush=True)
            r.raise_for_status()
    except Exception:
        traceback.print_exc()


def _tg_send_document(path: str, caption: str = "") -> None:
    try:
        if TG_BOT_TOKEN.startswith("sk-"):
            print("[tg] ERROR: TG_BOT_TOKEN виглядає як OpenAI ключ.", flush=True)
            return

        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument"
        with open(path, "rb") as f:
            files = {"document": (path, f)}
            data = {"chat_id": TG_CHAT_ID, "caption": caption}
            r = SESSION.post(url, data=data, files=files, timeout=TIMEOUT)
            if r.status_code >= 400:
                print(f"[tg] sendDocument {r.status_code}: {r.text[:300]}", flush=True)
            r.raise_for_status()
    except Exception:
        traceback.print_exc()


# -------------------- Weekly store/helpers --------------------
def _now_kyiv() -> datetime:
    return datetime.now(ZoneInfo(WEEKLY_TZ))


def _iso_week_key(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _weekday_name(dt: datetime) -> str:
    return dt.strftime("%a")


def _load_weekly_state() -> dict:
    p = pathlib.Path(WEEKLY_STATE_FILE)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}


def _save_weekly_state(st: dict) -> None:
    pathlib.Path(WEEKLY_STATE_FILE).write_text(
        json.dumps(st, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _append_call_record(rec: dict) -> None:
    with open(CALLS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _read_calls() -> list[dict]:
    res = []
    p = pathlib.Path(CALLS_FILE)
    if not p.exists():
        return res

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                res.append(json.loads(line))
            except Exception:
                continue
    return res


def _prune_old_calls() -> None:
    if WEEKLY_KEEP_DAYS <= 0:
        return

    cutoff = datetime.utcnow() - timedelta(days=WEEKLY_KEEP_DAYS)
    items = _read_calls()
    keep = []

    for it in items:
        try:
            ts = datetime.fromisoformat(it.get("ts").replace("Z", "+00:00"))
            if ts >= cutoff:
                keep.append(it)
        except Exception:
            keep.append(it)

    with open(CALLS_FILE, "w", encoding="utf-8") as f:
        for it in keep:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def _week_bounds_kyiv(now: datetime) -> tuple[datetime, datetime]:
    end_kyiv = now
    start_kyiv = now - timedelta(days=7)
    return start_kyiv.astimezone(ZoneInfo("UTC")), end_kyiv.astimezone(ZoneInfo("UTC"))


def _safe_parse_dt(val: str) -> t.Optional[datetime]:
    if not val:
        return None
    try:
        s = str(val).strip().replace("T", " ").replace("/", "-")
        fmts = [
            "%Y-%m-%d %H:%M:%S",
            "%d.%m.%Y %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d.%m.%Y %H:%M",
        ]
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.replace(tzinfo=ZoneInfo(WEEKLY_TZ)).astimezone(ZoneInfo("UTC"))
            except Exception:
                pass
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(ZoneInfo("UTC"))
    except Exception:
        return None


def _send_weekly_report() -> None:
    now = _now_kyiv()
    start_utc, end_utc = _week_bounds_kyiv(now)
    calls = _read_calls()

    window = []
    for it in calls:
        try:
            call_dt = _safe_parse_dt(it.get("call_start", ""))
            if call_dt is None:
                call_dt = datetime.fromisoformat(it.get("ts").replace("Z", "+00:00"))
            if start_utc <= call_dt <= end_utc:
                window.append(it)
        except Exception:
            continue

    total = len(window)
    if total == 0:
        tg_send_message("📊 Тижневий звіт: за період дзвінків не знайдено.")
        return

    avg_dur = round(sum(it.get("duration", 0) or 0 for it in window) / total, 1)
    avg_score = round(sum(it.get("score", 0) or 0 for it in window) / total, 2)

    tag_counts = Counter((it.get("tag") or "Інформаційні звернення") for it in window)
    top_tags = tag_counts.most_common(10)
    tags_block = "\n".join([f"• {html_escape(t)} — {n}" for t, n in top_tags]) or "• —"

    reason_counts = Counter((it.get("root_reason") or "—") for it in window)
    top_reasons = reason_counts.most_common(5)
    reasons_block = "\n".join([f"• {html_escape(r)} — {n}" for r, n in top_reasons]) or "• —"

    resolved_true = sum(1 for it in window if it.get("resolved_on_first_contact") is True)
    resolved_known = sum(1 for it in window if it.get("resolved_on_first_contact") in (True, False))
    fcr_rate = round((resolved_true / resolved_known) * 100, 1) if resolved_known else 0.0

    repeat_count = sum(1 for it in window if it.get("repeat_contact_signal") is True)
    repeat_rate = round((repeat_count / total) * 100, 1) if total else 0.0

    price_objection_count = sum(1 for it in window if it.get("price_objection") is True)
    price_objection_rate = round((price_objection_count / total) * 100, 1) if total else 0.0

    churn_count = sum(1 for it in window if it.get("churn_risk") == "high")
    churn_rate = round((churn_count / total) * 100, 1) if total else 0.0

    criteria_names = [label for _, label in QA_CRITERIA]
    criteria_totals = [0] * len(QA_CRITERIA)
    criteria_count = 0

    for it in window:
        scores = it.get("criteria_scores") or []
        if len(scores) == len(QA_CRITERIA):
            for i, sc in enumerate(scores):
                try:
                    criteria_totals[i] += int(sc)
                except Exception:
                    pass
            criteria_count += 1

    criteria_block = "• —"
    if criteria_count:
        crit_stats = []
        for i, total_sc in enumerate(criteria_totals):
            pct = round((total_sc / criteria_count) * 100, 1)
            crit_stats.append((criteria_names[i], pct))
        crit_stats = sorted(crit_stats, key=lambda x: x[1])[:5]
        criteria_block = "\n".join([f"• {html_escape(name)} — {pct}%" for name, pct in crit_stats])

    worst = sorted(
        window,
        key=lambda x: (
            x.get("score", 0),
            x.get("trust", {}).get("overall", 0),
            x.get("duration", 0) or 0,
        ),
    )[:5]
    worst_block = "\n".join(
        [
            f"• {html_escape(it.get('name', '—'))} | {html_escape(_mask_phone(it.get('phone', '—')))} | "
            f"тег: {html_escape(it.get('tag', '—'))} | бал: {int(it.get('score', 0))}"
            for it in worst
        ]
    ) or "• —"

    title = f"📊 Тижневий звіт ({now.strftime('%d.%m.%Y')})"
    body = (
        f"<b>{title}</b>\n"
        f"Дзвінків: <b>{total}</b>\n"
        f"Середня тривалість: <b>{avg_dur}s</b>\n"
        f"Середній бал якості (0–8): <b>{avg_score}</b>\n"
        f"FCR (закрито з 1-го контакту): <b>{fcr_rate}%</b>\n"
        f"Повторні звернення: <b>{repeat_rate}%</b>\n"
        f"Заперечення по ціні: <b>{price_objection_rate}%</b>\n"
        f"Ризик відтоку: <b>{churn_rate}%</b>\n\n"
        f"<b>Топ тем:</b>\n{tags_block}\n\n"
        f"<b>Топ причин звернень:</b>\n{reasons_block}\n\n"
        f"<b>Найслабші критерії команди:</b>\n{criteria_block}\n\n"
        f"<b>Топ проблемні (найнижчий бал):</b>\n{worst_block}"
    )
    tg_send_message(body)

    try:
        csv_path = pathlib.Path(CSV_FILENAME)
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "ts",
                    "call_start",
                    "call_id",
                    "name",
                    "phone",
                    "duration",
                    "tag",
                    "root_reason",
                    "score",
                    "resolved_on_first_contact",
                    "repeat_contact_signal",
                    "price_objection",
                    "price_objection_note",
                    "churn_risk",
                    "summary",
                    "trust",
                ],
            )
            w.writeheader()

            for it in window:
                summary_plain = (it.get("summary_plain") or it.get("summary") or "")
                trust = it.get("trust", {})
                overall = trust.get("overall", "")
                w.writerow(
                    {
                        "ts": it.get("ts", ""),
                        "call_start": it.get("call_start", ""),
                        "call_id": it.get("call_id", ""),
                        "name": it.get("name", ""),
                        "phone": it.get("phone", ""),
                        "duration": it.get("duration", ""),
                        "tag": it.get("tag", ""),
                        "root_reason": it.get("root_reason", ""),
                        "score": it.get("score", ""),
                        "resolved_on_first_contact": it.get("resolved_on_first_contact", ""),
                        "repeat_contact_signal": it.get("repeat_contact_signal", ""),
                        "price_objection": it.get("price_objection", ""),
                        "price_objection_note": it.get("price_objection_note", ""),
                        "churn_risk": it.get("churn_risk", ""),
                        "summary": summary_plain,
                        "trust": overall,
                    }
                )
        _tg_send_document(str(csv_path), caption=title)
    except Exception:
        traceback.print_exc()


def _maybe_send_weekly_report() -> None:
    st = _load_weekly_state()
    now = _now_kyiv()
    week_key = _iso_week_key(now)

    if _weekday_name(now) != WEEKLY_REPORT_DAY:
        return
    if now.hour < WEEKLY_REPORT_HOUR:
        return
    if st.get("last_sent_week") == week_key:
        return

    _send_weekly_report()
    st["last_sent_week"] = week_key
    _save_weekly_state(st)
    _prune_old_calls()


# -------------------- State --------------------
def load_state() -> dict:
    p = pathlib.Path(STATE_FILE)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}


def save_state(st: dict) -> None:
    pathlib.Path(STATE_FILE).write_text(
        json.dumps(st, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _build_low_transcript_message(
    *,
    name: str,
    phone: str,
    link: str,
    call_start: str,
    duration: t.Optional[int],
    transcript_trust: int,
) -> str:
    header = f"AI: 📞 {html_escape(name)} | {html_escape(phone)} | ⏱{duration}s"
    body = (
        f"<b>Новий дзвінок</b>\n"
        f"<b>ПІБ:</b> {html_escape(name)}\n"
        f"<b>Телефон:</b> {html_escape(phone)}\n"
        f"<b>CRM:</b> <a href='{html_escape(link)}'>відкрити</a>\n"
        f"<b>Початок:</b> {html_escape(call_start)}\n"
        f"<b>Тривалість:</b> {duration}s\n"
        f"<b>Trust:</b> ❌ <b>{transcript_trust}%</b> (низька якість транскрипту)\n\n"
        f"<b>Статус:</b> недостатньо якісний транскрипт для повного QA-аналізу.\n"
        f"<b>Підказка:</b> перевір запис дзвінка або спробуй іншу модель транскрипції."
    )
    return f"{header}\n\n{body}"


# -------------------- Main --------------------
def process() -> None:
    if not all(_require_env(n) for n in ["BITRIX_WEBHOOK_BASE", "OPENAI_API_KEY", "TG_BOT_TOKEN", "TG_CHAT_ID"]):
        return

    state = load_state()
    processed_list = state.get("processed_call_ids") or []
    processed_set = set(processed_list)

    calls = b24_vox_get_latest(LIMIT_LAST)
    if not calls:
        _maybe_send_weekly_report()
        return

    for c in calls:
        if c.call_id in processed_set:
            continue

        try:
            audio, mime, fname = fetch_audio(c.record_url, max_mb=MAX_AUDIO_MB)
            transcript = transcribe_audio(audio, filename=fname, mime=mime)

            name = b24_get_entity_name(c.crm_entity_type, c.crm_entity_id)
            phone = c.phone_number or "—"
            link = b24_entity_link(c.crm_entity_type, c.crm_entity_id, c.crm_activity_id)

            transcript_trust = compute_transcript_trust(transcript, c.duration)

            if transcript_trust < MIN_TRANSCRIPT_TRUST_FOR_FULL_QA:
                tg_send_message(
                    _build_low_transcript_message(
                        name=name,
                        phone=phone,
                        link=link,
                        call_start=c.call_start,
                        duration=c.duration,
                        transcript_trust=transcript_trust,
                    )
                )

                processed_list.append(c.call_id)
                processed_set.add(c.call_id)
                if len(processed_list) > PROCESSED_KEEP:
                    processed_list = processed_list[-PROCESSED_KEEP:]
                    processed_set = set(processed_list)

                state["processed_call_ids"] = processed_list
                save_state(state)

                _append_call_record(
                    {
                        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                        "call_start": c.call_start,
                        "call_id": c.call_id,
                        "name": name,
                        "phone": phone,
                        "duration": c.duration,
                        "tag": "Інформаційні звернення",
                        "score": 0,
                        "summary": "Недостатньо якісний транскрипт для повного QA-аналізу.",
                        "summary_plain": "Недостатньо якісний транскрипт для повного QA-аналізу.",
                        "analysis": {"error": "low_transcript_trust"},
                        "root_reason": "Невідомо",
                        "resolved_on_first_contact": None,
                        "repeat_contact_signal": False,
                        "price_objection": False,
                        "price_objection_note": "",
                        "churn_risk": "low",
                        "customer_emotion": "neutral",
                        "next_step_promised": "",
                        "deadline_promised": "",
                        "criteria_scores": [0] * len(QA_CRITERIA),
                        "trust": {
                            "overall": 0,
                            "transcript": transcript_trust,
                            "analysis": 0,
                        },
                    }
                )
                continue

            checklist_html, summary_html, tag, score, analysis_obj = analyze_and_summarize(
                transcript,
                call_duration_sec=c.duration,
            )

            analysis_trust = compute_analysis_trust(analysis_obj if isinstance(analysis_obj, dict) else {})
            overall_trust = compute_overall_trust(transcript_trust, analysis_trust)
            trust_emoji, trust_label = trust_badge(overall_trust)

            trust_line = (
                f"<b>Trust:</b> {trust_emoji} <b>{overall_trust}%</b> ({trust_label}) "
                f"| transcript {transcript_trust}% | analysis {analysis_trust}%"
            )

            root_reason = str(analysis_obj.get("root_reason") or "—")
            resolved = analysis_obj.get("resolved_on_first_contact")
            repeat_signal = bool(analysis_obj.get("repeat_contact_signal", False))
            price_objection = bool(analysis_obj.get("price_objection", False))
            price_objection_note = str(analysis_obj.get("price_objection_note") or "")
            churn_risk = str(analysis_obj.get("churn_risk") or "low")
            customer_emotion = str(analysis_obj.get("customer_emotion") or "neutral")
            next_step_promised = str(analysis_obj.get("next_step_promised") or "")
            deadline_promised = str(analysis_obj.get("deadline_promised") or "")

            resolved_text = _to_bool_text_ua(resolved)
            repeat_text = "так" if repeat_signal else "ні"
            price_objection_text = "так" if price_objection else "ні"

            header = f"AI: 📞 {html_escape(name)} | {html_escape(phone)} | ⏱{c.duration}s"
            body = (
                f"<b>Новий дзвінок</b>\n"
                f"<b>ПІБ:</b> {html_escape(name)}\n"
                f"<b>Телефон:</b> {html_escape(phone)}\n"
                f"<b>CRM:</b> <a href='{html_escape(link)}'>відкрити</a>\n"
                f"<b>Початок:</b> {html_escape(c.call_start)}\n"
                f"<b>Тривалість:</b> {c.duration}s\n"
                f"<b>Тема:</b> {html_escape(tag)} | <b>Бал:</b> {score}/8\n"
                f"<b>Причина звернення:</b> {html_escape(root_reason)}\n"
                f"<b>Питання закрито з 1-го контакту:</b> {html_escape(resolved_text)}\n"
                f"<b>Повторне звернення:</b> {html_escape(repeat_text)}\n"
                f"<b>Заперечення по ціні:</b> {html_escape(price_objection_text)}\n"
                f"<b>Ризик відтоку:</b> {html_escape(churn_risk)}\n"
                f"<b>Емоція клієнта:</b> {html_escape(customer_emotion)}\n"
                f"{trust_line}\n"
            )

            if price_objection_note:
                body += f"<b>Коментар по ціні:</b> {html_escape(price_objection_note)}\n"
            if next_step_promised:
                body += f"<b>Наступний крок:</b> {html_escape(next_step_promised)}\n"
            if deadline_promised:
                body += f"<b>Озвучений строк:</b> {html_escape(deadline_promised)}\n"

            body += (
                f"\n<b>Аналіз розмови:</b>\n{checklist_html}\n\n"
                f"<b>Коротке резюме:</b> {summary_html}"
            )

            tg_send_message(f"{header}\n\n{body}")

            processed_list.append(c.call_id)
            processed_set.add(c.call_id)
            if len(processed_list) > PROCESSED_KEEP:
                processed_list = processed_list[-PROCESSED_KEEP:]
                processed_set = set(processed_list)

            state["processed_call_ids"] = processed_list
            save_state(state)

            summary_plain = _strip_html(summary_html)

            checklist = analysis_obj.get("checklist") if isinstance(analysis_obj, dict) else []
            criteria_scores: list[int] = []
            if isinstance(checklist, list):
                score_map: dict[str, int] = {}
                for it in checklist:
                    if isinstance(it, dict):
                        ck = it.get("criterion_key")
                        if isinstance(ck, str):
                            score_map[ck] = int(it.get("score", 0))
                for key, _label in QA_CRITERIA:
                    criteria_scores.append(int(score_map.get(key, 0)))

            _append_call_record(
                {
                    "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "call_start": c.call_start,
                    "call_id": c.call_id,
                    "name": name,
                    "phone": phone,
                    "duration": c.duration,
                    "tag": tag,
                    "score": score,
                    "summary": summary_html,
                    "summary_plain": summary_plain,
                    "analysis": analysis_obj,
                    "root_reason": root_reason,
                    "resolved_on_first_contact": analysis_obj.get("resolved_on_first_contact"),
                    "repeat_contact_signal": bool(analysis_obj.get("repeat_contact_signal", False)),
                    "price_objection": bool(analysis_obj.get("price_objection", False)),
                    "price_objection_note": str(analysis_obj.get("price_objection_note") or ""),
                    "churn_risk": str(analysis_obj.get("churn_risk") or "low"),
                    "customer_emotion": str(analysis_obj.get("customer_emotion") or "neutral"),
                    "next_step_promised": str(analysis_obj.get("next_step_promised") or ""),
                    "deadline_promised": str(analysis_obj.get("deadline_promised") or ""),
                    "criteria_scores": criteria_scores,
                    "trust": {
                        "overall": overall_trust,
                        "transcript": transcript_trust,
                        "analysis": analysis_trust,
                    },
                }
            )

        except Exception as e:
            traceback.print_exc()
            tg_send_message(
                "🚨 Помилка обробки CALL_ID "
                f"<code>{html_escape(c.call_id)}</code>:\n"
                f"<code>{html_escape(str(e))[:1800]}</code>\n"
                "Підказка: якщо це 400 від chat/completions — перевір OPENAI_ANALYSIS_MODEL і body помилки; "
                "якщо 400 від transcription — перевір аудіо, розмір або посилання."
            )

    _maybe_send_weekly_report()


if __name__ == "__main__":
    process()
