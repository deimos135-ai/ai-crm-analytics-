#!/usr/bin/env python3
"""
Bitrix24 → Whisper → Telegram monitor (real-time, weekly analytics, safe import)

- Тягне останні дзвінки з Bitrix24 (voximplant.statistic.get через total→start)
- Фільтрує тільки вхідні (або за env) + мінімальна тривалість
- Скачує запис, перевіряє MIME/розмір, транскрибує Whisper'ом (uk + підказка)
- ОДИН запит до OpenAI (chat): чек-лист 8 критеріїв + коротке резюме + tag + score
- Шле у Telegram: короткий підсумок у першому рядку + структурований аналіз
- Пише кожен дзвінок у JSONL (calls_week.jsonl)
- Раз на тиждень (Fri 18:00 Europe/Kyiv) шле тижневий звіт + CSV
- Без fail-fast на імпорті: секрети перевіряються всередині process()
"""

import os
import re
import json
import pathlib
import traceback
import typing as t
from dataclasses import dataclass

import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import csv
from collections import Counter


# -------------------- Config --------------------
STATE_FILE = os.getenv("STATE_FILE", "b24_monitor_state.json")
BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE", "")  # must end with '/'
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
LIMIT_LAST = int(os.getenv("LIMIT_LAST", "1"))

LANGUAGE_HINT = (os.getenv("LANGUAGE_HINT") or "uk").strip().lower()
OPENAI_CHAT_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))

MAX_AUDIO_MB = int(os.getenv("MAX_AUDIO_MB", "25"))
MAX_TRANSCRIPT_CHARS = int(os.getenv("MAX_TRANSCRIPT_CHARS", "7000"))

ONLY_INCOMING = (os.getenv("ONLY_INCOMING", "true").lower() == "true")
INCOMING_CODE = os.getenv("INCOMING_CODE", "1")  # 1=incoming, 2=outgoing
MIN_DURATION_SEC = int(os.getenv("MIN_DURATION_SEC", "10"))

WEEKLY_TZ = os.getenv("WEEKLY_TZ", "Europe/Kyiv")
WEEKLY_REPORT_DAY = os.getenv("WEEKLY_REPORT_DAY", "Fri")  # Mon..Sun (strftime %a)
WEEKLY_REPORT_HOUR = int(os.getenv("WEEKLY_REPORT_HOUR", "18"))
WEEKLY_KEEP_DAYS = int(os.getenv("WEEKLY_KEEP_DAYS", "35"))

CALLS_FILE = os.getenv("CALLS_FILE", "calls_week.jsonl")
WEEKLY_STATE_FILE = os.getenv("WEEKLY_STATE_FILE", "weekly_state.json")
CSV_FILENAME = os.getenv("WEEKLY_CSV_NAME", "weekly_calls.csv")

if BITRIX_WEBHOOK_BASE and not BITRIX_WEBHOOK_BASE.endswith("/"):
    BITRIX_WEBHOOK_BASE += "/"


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
    call_type: t.Optional[str]  # 1=incoming, 2=outgoing (рядок)


# -------------------- Utils --------------------
def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def http_post_json(url: str, payload: dict) -> dict:
    """
    Важливо: при помилках Bitrix часто повертає корисний body.
    Логуємо його, щоб не гадати “чому 401/403/500”.
    """
    resp = requests.post(url, json=payload, timeout=TIMEOUT)
    if resp.status_code >= 400:
        print(f"[http] {resp.status_code} POST {url} -> {resp.text[:2000]}", flush=True)
        resp.raise_for_status()
    return resp.json()


def _require_env(name: str) -> bool:
    val = os.getenv(name, "")
    if not val:
        print(f"[monitor] WARN missing env {name}", flush=True)
        return False
    return True


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


# -------------------- Audio fetch (safe + memory friendly) --------------------
def fetch_audio(url: str, max_mb: int = 25) -> tuple[bytes, str, str]:
    """
    Завантажує запис з перевірками.
    Повертає: (bytes, mime, suggest_filename)
    Кидає виняток, якщо це не аудіо або великий файл.
    """
    headers = {"User-Agent": "ai-crm-analytics/1.0", "Accept": "*/*"}
    max_bytes = max_mb * 1024 * 1024

    with requests.get(url, headers=headers, stream=True, allow_redirects=True, timeout=TIMEOUT) as r:
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


# -------------------- OpenAI: Whisper --------------------
def transcribe_whisper(audio_bytes: bytes, filename: str = "audio.mp3", mime: str = "audio/mpeg") -> str:
    url = "https://api.openai.com/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}

    initial_prompt = (
        "Транскрибуй українською мовою (uk). Дотримуйся української орфографії, "
        "без російських літер і кальок. Приклади: «будь ласка», «зв'язок», "
        "«підключення», «номер». Не змішуй українську та російську."
    )

    files = {"file": (filename, audio_bytes, mime)}
    data = {
        "model": "whisper-1",
        "language": LANGUAGE_HINT,
        "temperature": 0,
        "prompt": initial_prompt,
    }

    r = requests.post(url, headers=headers, files=files, data=data, timeout=TIMEOUT)
    if r.status_code >= 400:
        try:
            err = r.json()
        except Exception:
            err = r.text
        raise requests.HTTPError(f"OpenAI audio/transcriptions {r.status_code}: {err}")
    return (r.json().get("text", "") or "").strip()


# -------------------- OpenAI: One-shot analysis + summary (stable schema) --------------------
def analyze_and_summarize(transcript: str) -> tuple[str, str, str, int]:
    """
    Повертає (html_checklist, html_summary, tag, score_0_8).

    Важливо: тепер checklist — НЕ “рядки”, а об’єкти зі status/note.
    Це усуває крихкість типу “Не перебивав/Не перебивала”.
    """
    if not transcript:
        return ("Немає транскрипту для аналізу.", "Немає даних для резюме.", "Інше", 0)

    if MAX_TRANSCRIPT_CHARS > 0 and len(transcript) > MAX_TRANSCRIPT_CHARS:
        transcript = transcript[:MAX_TRANSCRIPT_CHARS] + " …(урізано для економії токенів)"

    allowed_tags = [
        "Технічна проблема",
        "Платежі/рахунок",
        "Підключення/тарифи",
        "Доставка/візит",
        "Підтримка акаунта",
        "Скарга",
        "Загальне питання",
        "Інше",
    ]

    labels = [
        "Привітання по скрипту",
        "З’ясування суті звернення",
        "Ввічливість і емпатія",
        "Не перебивав",
        "Рішення",
        "Наступні дії або строки",
        "Допомога/upsell",
        "Завершення",
    ]

    status_to_emoji = {"ok": "✅", "partial": "⚠️", "fail": "❌"}

    def _build_messages(fix_note: str = ""):
        system = (
            "Ти — аналітик якості дзвінків кол-центру. Відповідай УКРАЇНСЬКОЮ. "
            "ПОВЕРТАЙ СУВОРО JSON (без жодного тексту поза JSON). "
            "Структура: {"
            "\"checklist\":[{\"status\":\"ok|partial|fail\",\"note\":\"...\"} x8], "
            "\"summary\":\"...\", \"tag\":\"...\"}. "
            f"Поле tag — ОДИН зі списку: {', '.join(allowed_tags)}. "
            "checklist МАЄ містити рівно 8 елементів У ПОРЯДКУ критеріїв. "
            "status ТІЛЬКИ: ok / partial / fail. "
            "note: 6–200 символів, конкретно, без односкладових «Так/Ні/Ок». "
            "НЕ повертай markdown, НЕ додавай зайві поля."
            + (f" ДОДАТКОВО: {fix_note}" if fix_note else "")
        )

        user = f"""
Оціни розмову за 8 критеріями (строго в такій послідовності) і дай стислий підсумок (1–2 речення).
Поверни СТРОГО JSON.

Критерії (порядок незмінний):
1) {labels[0]}
2) {labels[1]}
3) {labels[2]}
4) {labels[3]}
5) {labels[4]}
6) {labels[5]}
7) {labels[6]}
8) {labels[7]}

Вимоги:
- checklist: рівно 8 елементів
- кожен елемент: {{"status":"ok|partial|fail","note":"6–200 символів"}}
- summary: 1–2 речення без оцінок
- tag: один із дозволених

Транскрипт:
---
{transcript}
---
"""
        return [{"role": "system", "content": system}, {"role": "user", "content": user}]

    def _call_openai(messages):
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": OPENAI_CHAT_MODEL,
            "messages": messages,
            "temperature": 0.2,
            "max_tokens": 750,
            "response_format": {"type": "json_object"},
        }
        r = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
        if r.status_code >= 400:
            # корисно мати body при дебазі
            print(f"[openai] chat {r.status_code} -> {r.text[:2000]}", flush=True)
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        return json.loads(content)

    def _validate(obj) -> tuple[bool, str]:
        if not isinstance(obj, dict):
            return False, "root not object"

        cl = obj.get("checklist")
        if not isinstance(cl, list) or len(cl) != 8:
            return False, "checklist must be list of length 8"

        for i, item in enumerate(cl):
            if not isinstance(item, dict):
                return False, f"checklist[{i}] not object"
            st = item.get("status")
            note = item.get("note")
            if st not in ("ok", "partial", "fail"):
                return False, f"checklist[{i}].status invalid"
            if not isinstance(note, str):
                return False, f"checklist[{i}].note not string"
            note_s = note.strip()
            if len(note_s) < 6:
                return False, f"checklist[{i}].note too short"
            if len(note_s) > 200:
                return False, f"checklist[{i}].note too long"
            if note_s.lower() in ("так", "ні", "ок", "добре"):
                return False, f"checklist[{i}] trivial note"

        tag = obj.get("tag")
        if tag not in allowed_tags:
            return False, "tag not allowed"

        summary = obj.get("summary")
        if not isinstance(summary, str) or len(summary.strip()) < 10:
            return False, "summary too short"

        return True, ""

    # 1-й виклик
    obj = _call_openai(_build_messages())
    ok, _ = _validate(obj)

    if not ok:
        # 2-й (останній) повтор із жорсткішим наголосом
        obj = _call_openai(
            _build_messages(
                fix_note=(
                    "Попередня відповідь НЕ відповідала формату. "
                    "Суворо: checklist рівно 8 об’єктів, status лише ok|partial|fail, "
                    "note 6–200 символів; без markdown/зайвих полів."
                )
            )
        )
        ok, _ = _validate(obj)

        if not ok:
            # без JSON-каші — людський fallback
            return (
                "❌ Не вдалося отримати валідний чек-лист (формат порушено).",
                "Не вдалося отримати структуроване резюме (fallback).",
                "Інше",
                0,
            )

    cl = obj["checklist"]
    summary = (obj.get("summary") or "").strip()
    tag = obj.get("tag") or "Інше"

    lines = []
    score = 0
    for i, item in enumerate(cl):
        st = item.get("status")
        note = (item.get("note") or "").strip()
        emoji = status_to_emoji.get(st, "⚠️")
        if st == "ok":
            score += 1
        # Канонічний лейбл з твого коду (без залежності від LLM)
        lines.append(f"{emoji} {labels[i]}: {note}")

    checklist_html = "\n".join(html_escape(x) for x in lines)
    summary_html = html_escape(summary if summary else "Немає короткого резюме.")

    return checklist_html, summary_html, str(tag), int(score)


# -------------------- Telegram --------------------
def tg_send_message(text: str) -> None:
    """
    Відправляє повідомлення. Якщо довше за ~3500 символів — ріже на частини.
    """
    try:
        if TG_BOT_TOKEN.startswith("sk-"):
            print("[tg] ERROR: TG_BOT_TOKEN схожий на OpenAI ключ (sk-...). Замініть на токен BotFather.", flush=True)
            return
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

        CHUNK = 3500
        parts = [text[i : i + CHUNK] for i in range(0, len(text), CHUNK)] or [text]
        for part in parts:
            payload = {
                "chat_id": TG_CHAT_ID,
                "text": part,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            r = requests.post(url, json=payload, timeout=TIMEOUT)
            if r.status_code >= 400:
                print(f"[tg] sendMessage {r.status_code}: {r.text[:300]}", flush=True)
            r.raise_for_status()
    except Exception:
        traceback.print_exc()


def _tg_send_document(path: str, caption: str = ""):
    try:
        if TG_BOT_TOKEN.startswith("sk-"):
            print("[tg] ERROR: TG_BOT_TOKEN виглядає як OpenAI ключ.", flush=True)
            return
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument"
        with open(path, "rb") as f:
            files = {"document": (path, f)}
            data = {"chat_id": TG_CHAT_ID, "caption": caption}
            r = requests.post(url, data=data, files=files, timeout=TIMEOUT)
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


def _save_weekly_state(st: dict):
    pathlib.Path(WEEKLY_STATE_FILE).write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_call_record(rec: dict):
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


def _prune_old_calls():
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


def _send_weekly_report():
    now = _now_kyiv()
    start_utc, end_utc = _week_bounds_kyiv(now)
    calls = _read_calls()

    window = []
    for it in calls:
        try:
            ts = datetime.fromisoformat(it.get("ts").replace("Z", "+00:00"))
            if start_utc <= ts <= end_utc:
                window.append(it)
        except Exception:
            continue

    total = len(window)
    if total == 0:
        tg_send_message("📊 Тижневий звіт: за період дзвінків не знайдено.")
        return

    avg_dur = round(sum(it.get("duration", 0) or 0 for it in window) / total, 1)
    avg_score = round(sum(it.get("score", 0) or 0 for it in window) / total, 2)

    tag_counts = Counter((it.get("tag") or "Інше") for it in window)
    top_tags = tag_counts.most_common(5)
    tags_block = "\n".join([f"• {html_escape(t)} — {n}" for t, n in top_tags]) or "• —"

    worst = sorted(window, key=lambda x: (x.get("score", 0), x.get("duration", 0)))[:3]
    worst_block = "\n".join(
        [
            f"• {html_escape(it.get('name','—'))} | {html_escape(it.get('phone','—'))} | "
            f"тег: {html_escape(it.get('tag','—'))} | бал: {int(it.get('score',0))}"
            for it in worst
        ]
    ) or "• —"

    title = f"📊 Тижневий звіт ({now.strftime('%d.%m.%Y')})"
    body = (
        f"<b>{title}</b>\n"
        f"Дзвінків: <b>{total}</b>\n"
        f"Середня тривалість: <b>{avg_dur}s</b>\n"
        f"Середній бал якості (0–8): <b>{avg_score}</b>\n\n"
        f"<b>Топ тем:</b>\n{tags_block}\n\n"
        f"<b>Топ проблемні (найнижчий бал):</b>\n{worst_block}"
    )
    tg_send_message(body)

    try:
        csv_path = pathlib.Path(CSV_FILENAME)
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ts", "call_id", "name", "phone", "duration", "tag", "score", "summary"])
            w.writeheader()
            for it in window:
                # summary в CSV — без HTML
                summary_plain = (it.get("summary_plain") or it.get("summary") or "")
                w.writerow(
                    {
                        "ts": it.get("ts", ""),
                        "call_id": it.get("call_id", ""),
                        "name": it.get("name", ""),
                        "phone": it.get("phone", ""),
                        "duration": it.get("duration", ""),
                        "tag": it.get("tag", ""),
                        "score": it.get("score", ""),
                        "summary": summary_plain,
                    }
                )
        _tg_send_document(str(csv_path), caption=title)
    except Exception:
        traceback.print_exc()


def _maybe_send_weekly_report():
    """
    Було крихко: “рівно о 18:00”.
    Стало: якщо сьогодні потрібний день і зараз >= 18:00 та ще не слали цього тижня — шлемо.
    """
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


def save_state(st: dict):
    pathlib.Path(STATE_FILE).write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")


# -------------------- Main --------------------
def process():
    if not all(_require_env(n) for n in ["BITRIX_WEBHOOK_BASE", "OPENAI_API_KEY", "TG_BOT_TOKEN", "TG_CHAT_ID"]):
        return

    state = load_state()
    last_seen = state.get("last_seen_call_id")

    calls = b24_vox_get_latest(LIMIT_LAST)
    if not calls:
        _maybe_send_weekly_report()
        return

    for c in calls:
        if c.call_id == last_seen:
            continue
        try:
            audio, mime, fname = fetch_audio(c.record_url, max_mb=MAX_AUDIO_MB)
            transcript = transcribe_whisper(audio, filename=fname, mime=mime)

            name = b24_get_entity_name(c.crm_entity_type, c.crm_entity_id)
            phone = c.phone_number or "—"
            link = b24_entity_link(c.crm_entity_type, c.crm_entity_id, c.crm_activity_id)

            checklist_html, summary_html, tag, score = analyze_and_summarize(transcript)

            header = f"AI: 📞 {html_escape(name)} | {html_escape(phone)} | ⏱{c.duration}s"
            body = (
                f"<b>Новий дзвінок</b>\n"
                f"<b>ПІБ:</b> {html_escape(name)}\n"
                f"<b>Телефон:</b> {html_escape(phone)}\n"
                f"<b>CRM:</b> <a href='{html_escape(link)}'>відкрити</a>\n"
                f"<b>Початок:</b> {html_escape(c.call_start)}\n"
                f"<b>Тривалість:</b> {c.duration}s\n"
                f"<b>Тема:</b> {html_escape(tag)} | <b>Бал:</b> {score}/8\n\n"
                f"<b>Аналіз розмови:</b>\n{checklist_html}\n\n"
                f"<b>Коротке резюме:</b> {summary_html}"
            )
            tg_send_message(f"{header}\n\n{body}")

            state["last_seen_call_id"] = c.call_id
            save_state(state)

            # summary_plain для CSV/аналітики без HTML
            summary_plain = (summary_html or "").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

            _append_call_record(
                {
                    "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "call_id": c.call_id,
                    "name": name,
                    "phone": phone,
                    "duration": c.duration,
                    "tag": tag,
                    "score": score,
                    "summary": summary_html,
                    "summary_plain": summary_plain,
                }
            )

        except Exception as e:
            traceback.print_exc()
            tg_send_message(
                "🚨 Помилка обробки CALL_ID "
                f"<code>{html_escape(c.call_id)}</code>:\n"
                f"<code>{html_escape(str(e))[:1200]}</code>\n"
                "Підказка: 400 від OpenAI часто означає не-аудіо/HTML або занадто великий файл, "
                "або протухлий запис із Bitrix/Vox."
            )

    _maybe_send_weekly_report()


if __name__ == "__main__":
    process()
