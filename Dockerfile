FROM python:3.11-slim
WORKDIR /app

# Системні оптимізації
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

# Залежності
COPY requirements.txt .
RUN pip install -r requirements.txt

# Код
COPY . /app

# Порт для healthcheck
EXPOSE 8080

# Інтервал опитування (сек)
ENV POLL_INTERVAL_SECONDS=180

# Запуск циклічного раннера
CMD ["python", "runner.py"]
