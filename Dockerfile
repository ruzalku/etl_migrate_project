FROM python:3.13-alpine

WORKDIR /etl-process

COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --no-cache-dir

COPY . .

ENTRYPOINT [ "python3", "main.py" ]
