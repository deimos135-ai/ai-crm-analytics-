def analyze_and_summarize(transcript: str) -> tuple[str, str, str, int]:
    """
    Один запит до OpenAI з жорстким форматом + валідація.
    Повертає (html_checklist, html_summary, tag, score_0_8).
    Якщо відповідь не за форматом, робить ОДИН повтор із посиленим інструктажем.
    """
    if not transcript:
        return ("Немає транскрипту для аналізу.", "Немає даних для резюме.", "Інше", 0)

    if MAX_TRANSCRIPT_CHARS > 0 and len(transcript) > MAX_TRANSCRIPT_CHARS:
        transcript = transcript[:MAX_TRANSCRIPT_CHARS] + " …(урізано для економії токенів)"

    allowed_tags = [
        "Технічна проблема", "Платежі/рахунок", "Підключення/тарифи", "Доставка/візит",
        "Підтримка акаунта", "Скарга", "Загальне питання", "Інше"
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
    label_pattern = r"(Привітання по скрипту|З’ясування суті звернення|Ввічливість і емпатія|Не перебивав|Рішення|Наступні дії або строки|Допомога/upsell|Завершення)"
    row_regex = re.compile(rf"^(✅|⚠️|❌) {label_pattern}: .{{6,200}}$")

    def _build_messages(fix_note: str = ""):
        system = (
            "Ти — аналітик якості кол-центру. Відповідай УКРАЇНСЬКОЮ. "
            "ПОВЕРТАЙ СУВОРО JSON (без жодного тексту поза JSON). "
            "Структура: {\"checklist\":[...8], \"summary\":\"...\", \"tag\":\"...\"}. "
            f"Поле tag — ОДИН зі списку: {', '.join(allowed_tags)}. "
            "checklist МАЄ містити рівно 8 РЯДКІВ у такому порядку й форматі: "
            "«<емодзі> <Назва критерію>: <Коротке обґрунтування>». "
            "ДОЗВОЛЕНІ емодзі: ✅ (виконано), ⚠️ (частково/посередньо), ❌ (не виконано). "
            "Кожне обґрунтування 6–200 символів, без односкладових «Так/Ні/Ок». "
            "ЖОДНИХ узагальнень типу «Так» — тільки конкретний мікровисновок. "
            + (f"ДОДАТКОВО: {fix_note} " if fix_note else "")
        )
        user = f"""
Оціни розмову за 8 критеріями (строго в такій послідовності) і дай стислий підсумок (1–2 речення). 
Поверни СТРОГО JSON (без пояснень), наприклад:
{{
  "checklist": [
    "✅ Привітання по скрипту: оператор привітався і назвав ім'я.",
    "⚠️ З’ясування суті звернення: частково розпитав, лишилися неясності.",
    "✅ Ввічливість і емпатія: підтримував ввічливий тон, дякував.",
    "✅ Не перебивав: давав висловитися, не перебивав.",
    "❌ Рішення: конкретного рішення не запропоновано.",
    "⚠️ Наступні дії або строки: терміни звучали розмито.",
    "❌ Допомога/upsell: додаткову допомогу не запропонував.",
    "✅ Завершення: подякував і коректно попрощався."
  ],
  "summary": "1–2 речення про суть дзвінка без оцінок.",
  "tag": "Один із: {', '.join(allowed_tags)}"
}}

Критерії (порядок незмінний):
1) Привітання по скрипту
2) З’ясування суті звернення
3) Ввічливість і емпатія
4) Не перебивав
5) Рішення
6) Наступні дії або строки
7) Допомога/upsell
8) Завершення

Транскрипт:
---
{transcript}
---
"""
        return [{"role":"system","content":system},{"role":"user","content":user}]

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
        r.raise_for_status()
        data = r.json()["choices"][0]["message"]["content"]
        return json.loads(data)

    def _validate(obj) -> tuple[bool, str]:
        if not isinstance(obj, dict):
            return False, "root is not object"
        cl = obj.get("checklist")
        if not isinstance(cl, list) or len(cl) != 8:
            return False, "checklist must be list of length 8"
        # порядок і формат
        for idx, (want, got) in enumerate(zip(labels, cl)):
            if not isinstance(got, str):
                return False, f"row {idx} not string"
            # мусить починатися з емодзі + правильна назва критерію
            if not row_regex.match(got):
                return False, f"row {idx} bad format -> {got}"
            # ще перевіримо, що саме цей ярлик
            if f" {want}:" not in got:
                return False, f"row {idx} wrong label"
            # «Так/Ні/Ок» самі по собі заборонені
            tail = got.split(":", 1)[-1].strip().lower()
            if tail in ("так", "ні", "ок", "добре"):
                return False, f"row {idx} trivial answer"
        tag = obj.get("tag", "")
        if tag not in allowed_tags:
            return False, "tag not allowed"
        summary = obj.get("summary", "")
        if not isinstance(summary, str) or len(summary) < 10:
            return False, "summary too short"
        return True, ""

    # 1-й виклик
    obj = _call_openai(_build_messages())
    ok, why = _validate(obj)
    if not ok:
        # 2-й (останній) повтор із жорсткішим наголосом
        obj = _call_openai(_build_messages(
            fix_note="Попередня відповідь НЕ відповідала формату. "
                     "Суворо дотримуйся: 8 пунктів, порядок імен, префікси емодзі, "
                     "кожен рядок має обґрунтування 6–200 символів; жодних «Так/Ні/Ок»."
        ))
        ok, why = _validate(obj)
        if not ok:
            # віддаємо як є, але в UI буде видно, що пішов fallback
            safe = html_escape(json.dumps(obj, ensure_ascii=False))
            return (safe, "Не вдалося отримати структуроване резюме (fallback).", "Інше", 0)

    checklist_items = obj.get("checklist", [])
    summary = obj.get("summary", "")
    tag = obj.get("tag", "Інше")

    # у HTML
    checklist_html = "\n".join(html_escape(line) for line in checklist_items)
    score = sum(1 for line in checklist_items if isinstance(line, str) and line.strip().startswith("✅"))

    return (
        checklist_html or "Немає даних по чек‑листу.",
        html_escape(summary or "Немає короткого резюме."),
        str(tag),
        int(score),
    )
