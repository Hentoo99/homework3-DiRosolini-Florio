FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install flask requests pyyaml confluent-kafka

COPY main.py .
COPY sla_config.yaml .

CMD ["python", "main.py"]