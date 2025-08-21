#!/usr/bin/env python3
"""
Bitrix24 → Whisper → Telegram monitor (real-time)

Now enriched with:
- Contact info (Full Name + Phone) for each call
- Transcript analysis results
- Telegram alert message contains subscriber’s name + phone + analysis summary

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
SCRIPT_RULES_FILE = os.getenv("SCRIPT_RULES_FILE", "script_rules.json")
BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE", "")  # must end with '/'
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
LIMIT_LAST = int(os.getenv("LIMIT_LAST", "1"))
LANGUAGE_HINT = os.getenv("LANGUAGE_HINT", "uk")
TIMEOUT = 60

if not BITRIX_WEBHOOK_BASE.endswith('/'):
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

# -------------------- Helpers --------------------

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
    total = res.get("total") if isinstance(res, dict) else js.get("total")
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
    result = [r for r in result if r.duration and r.duration > 0 and r.record_url]
    result = sorted(result, key=lambda x: x.call_start, reverse=True)[:limit]
    return result

# CRM entity details (for Full Name)

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
    js = http_post_json(f"{BITRIX_WEBHOOK_BASE}{method}", {"ID": str(entity_id)})
    data = js.get("result", {})
    if not data:
        return "—"
    parts = []
    for k in ("NAME", "SECOND_NAME", "LAST_NAME"):
        v = data.get(k)
        if v:
            parts.append(str(v).strip())
    name = " ".join(parts).strip()
    if not name:
        name = str(data.get("TITLE", "")).strip() or "—"
    return name

def b24_entity_link(entity_type: str, entity_id: str, activity_id: str | None = None) -> str:
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

# -------------------- Whisper --------------------
def transcribe_whisper(audio_bytes: bytes, filename: str = "audio.mp3") -> str:
    url = "https://api.openai.com/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    files = {"file": (filename, audio_bytes, "audio/mpeg"), "model": (None, "whisper-1")}
    r = requests.post(url, headers=headers, files=files, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json().get("text", "").strip()

# -------------------- Telegram --------------------
def tg_send_message(text: str) -> None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    requests.post(url, json=payload, timeout=TIMEOUT)

# -------------------- Script Checking --------------------
def evaluate_transcript(transcript: str, rules: dict) -> str:
    # Simplified: just return first 500 chars for now
    return transcript[:500]

# -------------------- State --------------------
def load_state() -> dict:
    if pathlib.Path(STATE_FILE).exists():
        return json.loads(pathlib.Path(STATE_FILE).read_text(encoding="utf-8"))
    return {}

def save_state(st: dict):
    pathlib.Path(STATE_FILE).write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

# -------------------- Main --------------------
def process():
    state = load_state()
    last_seen = state.get("last_seen_call_id")
    calls = b24_vox_get_latest(LIMIT_LAST)
    for c in calls:
        if c.call_id == last_seen:
            continue
        audio = http_get_binary(c.record_url)
        transcript = transcribe_whisper(audio, filename=f"{c.call_id}.mp3")
        name = b24_get_entity_name(c.crm_entity_type, c.crm_entity_id)
        phone = c.phone_number or "—"
        preview = evaluate_transcript(transcript, {})

        msg = (
            f"☎️ <b>Новий дзвінок</b>
"
            f"<b>ПІБ:</b> {name}
"
            f"<b>Телефон:</b> {phone}
"
            f"<b>CRM:</b> <a href='{b24_entity_link(c.crm_entity_type, c.crm_entity_id, c.crm_activity_id)}'>відкрити</a>
"
            f"<b>CALL_ID:</b> <code>{c.call_id}</code>
"
            f"<b>Початок:</b> {c.call_start}
"
            f"<b>Тривалість:</b> {c.duration}s

"
            f"<b>Аналіз (фрагмент 500):</b>
<code>{preview}</code>"
        )
        tg_send_message(msg)
        state["last_seen_call_id"] = c.call_id
        save_state(state)

if __name__ == "__main__":
    process()
