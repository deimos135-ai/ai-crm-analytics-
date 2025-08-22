#!/usr/bin/env python3
"""
Bitrix24 → Whisper → Telegram monitor (real-time, safe import)

- Тягне останні дзвінки з Bitrix24 (voximplant.statistic.get через total→start)
- Скачує запис, транскрибує Whisper'ом (OpenAI) з фіксом мови uk
- Аналізує дзвінок за 8 критеріями (gpt-4o-mini)
- Шле у Telegram: короткий підсумок у першому рядку + чек‑лист (без сирого фрагмента)
- Без fail‑fast на імпорті: секрети перевіряються всередині process()
"""

import os
import json
import pathlib
import traceback
import typing as t
from dataclasses import dataclass

import requests

# -------------------- Config --------------------
STATE_FILE = os.getenv("STATE_FILE", "b24_monitor_state.json")
BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE", "")  # must end with '/'
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
LIMIT_LAST = int(os.getenv("LIMIT_LAST", "1"))
LANGUAGE_HINT = (os.getenv("LANGUAGE_HINT") or "uk").strip().lower()
OPENAI_CHAT_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-4o-mini")
TIMEOUT = 60

# не валимо імпорт; лише нормалізуємо base
if BITRIX_WEBHOOK_BASE and not BITRIX_WEBHOOK_BASE.endswith('/'):
    BITRIX_WEBHOOK_BASE += '/'

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

# -------------------- Utils --------------------
def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def http_post_json(url: str, payload: dict) -> dict:
    resp = requests.post(url, json=payload, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()

def http_get_binary(url: str) -> bytes:
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.content

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
                    duration=int(it.get("CALL_DURATION")) if it.get("CALL_DURATION") not in (None, "", "empty") else None,
                    record_url=(it.get("CALL_RECORD_URL") if it.get("CALL_RECORD_URL") not in (None, "", "empty") else None),
                    crm_entity_type=(it.get("CRM_ENTITY_TYPE") or None),
                    crm_entity_id=(it.get("CRM_ENTITY_ID") or None),
                    crm_activity_id=(it.get("CRM_ACTIVITY_ID") or None),
                    phone_number=(it.get("PHONE_NUMBER") or None),
                )
            )
        except Exception:
            continue

    # Тільки дзвінки з записом і тривалістю > 0
    result = [r for r in result if r.duration and r.duration > 0 and r.record_url]
    # Найновіші першими
    result = sorted(result, key=lambda x: x.call_start, reverse=True)[:limit]
    return result

# -------------------- CRM helpers --------------------
def _portal_base_from_webhook() -> str:
    try:
        return BITRIX_WEBHOOK_BASE.split('/rest/')[0].rstrip('/') + '/'
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
    et = (entity_type or '').upper()
    path_map = {
        'CONTACT': 'crm/contact/details/',
        'LEAD': 'crm/lead/details/',
        'DEAL': 'crm/deal/details/',
        'COMPANY': 'crm/company/details/',
    }
    if activity_id:
        return f"{base}crm/activity/?open_view={activity_id}"
    path = path_map.get(et)
    return f"{base}{path}{entity_id}/" if path and entity_id else base

# -------------------- OpenAI: Whisper --------------------
def transcribe_whisper(audio_bytes: bytes, filename: str = "audio.mp3") -> str:
    """
    Фіксуємо українську мову та даємо україномовний підказуючий prompt.
    """
    url = "https://api.openai.com/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}

    initial_prompt = (
        "Транскрибуй українською мовою (uk). Дотримуйся української орфографії, "
        "без російських літер і кальок. Приклади: «будь ласка», «зв'язок», "
        "«підключення», «номер». Не змішуй українську та російську."
    )

    files = {"file": (filename, audio_bytes, "audio/mpeg")}
    data = {
        "model": "whisper-1",
        "language": LANGUAGE_HINT,
        "temperature": 0,
        "prompt": initial_prompt,
    }

    r = requests.post(url, headers=headers, files=files, data=data, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json().get("text", "").strip()

# -------------------- OpenAI: Checklist analysis --------------------
def analyze_transcript(transcript: str) -> str:
    """
    Повертає структурований чек‑лист за 8 критеріями українською.
    Формат відповіді — короткі рядки з емодзі (✅/⚠️/❌) та стислим поясненням.
    """
    if not transcript:
        return "Немає транскрипту для аналізу."

    system = (
        "Ти — аналітик якості кол-центру. Оцінюй стисло, по суті, українською."
        " Виводь тільки список з 8 пунктів, кожен з емодзі-статусом і коротким поясненням."
    )
    user = f"""
Проаналізуй розмову за 8 критеріями. Для кожного — один рядок:
<емодзі статусу> <Назва критерію>: <дуже коротке пояснення або приклад фрази>

Використовуй ці критерії (в цій самій послідовності):
1. Привітання по скрипту (представився).
2. З’ясування суті звернення.
3. Ввічливість і емпатія.
4. Не перебивав, не агресивний тон.
5. Правильність відповіді / рішення.
6. Наступні дії або строки.
7. Допомога наприкінці / upsell.
8. Коректне завершення (подякував).

Транскрипт:
---
{transcript}
---"""

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_CHAT_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_tokens": 700,
        "temperature": 0.2,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"].strip()

    # Telegram HTML: екрануємо небезпечні символи
    return html_escape(content)

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

        # Розбивка на шматки (ліміт 4096; запас — 3500)
        CHUNK = 3500
        parts = [text[i:i+CHUNK] for i in range(0, len(text), CHUNK)] or [text]
        for idx, part in enumerate(parts, 1):
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

# -------------------- State --------------------
def load_state() -> dict:
    p = pathlib.Path(STATE_FILE)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}

def save_state(st: dict):
    pathlib.Path(STATE_FILE).write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

# -------------------- Env check --------------------
def _require_env(name: str) -> bool:
    val = os.getenv(name, "")
    if not val:
        print(f"[monitor] WARN missing env {name}", flush=True)
        return False
    return True

# -------------------- Main --------------------
def process():
    # М’яка валідація секретів (щоб /health жив навіть без них)
    if not all(_require_env(n) for n in ["BITRIX_WEBHOOK_BASE","OPENAI_API_KEY","TG_BOT_TOKEN","TG_CHAT_ID"]):
        return

    state = load_state()
    last_seen = state.get("last_seen_call_id")

    calls = b24_vox_get_latest(LIMIT_LAST)
    if not calls:
        return

    for c in calls:
        if c.call_id == last_seen:
            continue
        try:
            # 1) Аудіо → транскрипція
            audio = http_get_binary(c.record_url)
            transcript = transcribe_whisper(audio, filename=f"{c.call_id}.mp3")

            # 2) Ім'я/посилання/номер
            name = b24_get_entity_name(c.crm_entity_type, c.crm_entity_id)
            phone = c.phone_number or "—"
            link = b24_entity_link(c.crm_entity_type, c.crm_entity_id, c.crm_activity_id)

            # 3) Аналітика чек‑листом
            checklist = analyze_transcript(transcript)

            # 4) Хедер (для прев’ю) + тіло (без сирого фрагмента)
            header = (
                f"BOTR: 📞 {html_escape(name)} | {html_escape(phone)} | ⏱{c.duration}s"
            )
            body = (
                f"<b>Новий дзвінок</b>\n"
                f"<b>ПІБ:</b> {html_escape(name)}\n"
                f"<b>Телефон:</b> {html_escape(phone)}\n"
                f"<b>CRM:</b> <a href='{html_escape(link)}'>відкрити</a>\n"
                f"<b>CALL_ID:</b> <code>{html_escape(c.call_id)}</code>\n"
                f"<b>Початок:</b> {html_escape(c.call_start)}\n"
                f"<b>Тривалість:</b> {c.duration}s\n\n"
                f"<b>Аналіз розмови:</b>\n{checklist}"
            )
            tg_send_message(f"{header}\n\n{body}")

            # 5) Маркуємо як оброблений
            state["last_seen_call_id"] = c.call_id
            save_state(state)

        except Exception:
            traceback.print_exc()
            tg_send_message(
                f"🚨 Помилка обробки CALL_ID <code>{html_escape(c.call_id)}</code>:\n"
                f"<code>{html_escape(traceback.format_exc()[:3500])}</code>"
            )

if __name__ == "__main__":
    process()
