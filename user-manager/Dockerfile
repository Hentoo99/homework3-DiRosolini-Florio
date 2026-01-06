FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y gcc pkg-config default-libmysqlclient-dev \
    && apt-get clean

COPY user-manager/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY user-manager/ .
COPY proto/ ./proto/

RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/user_manager.proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/data_collector.proto


CMD ["python", "main.py"]