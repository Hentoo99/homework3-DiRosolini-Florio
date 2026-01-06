FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get clean

COPY alertSystem/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alertSystem/ .

CMD ["python", "alertSystem.py"]