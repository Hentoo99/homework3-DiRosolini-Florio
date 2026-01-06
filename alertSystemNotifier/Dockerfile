FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get clean

COPY alertSystemNotifier/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alertSystemNotifier/ .

CMD ["python", "main.py"]