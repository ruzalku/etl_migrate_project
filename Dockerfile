FROM python:3.13-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /etl-process
COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --no-cache-dir

COPY . .
CMD ["python", "main.py"]
