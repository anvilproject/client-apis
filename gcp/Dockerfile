# syntax=docker/dockerfile:1
FROM python:3.9-slim

ENV PYTHONUNBUFFERED True

COPY requirements.txt .
RUN pip install -r requirements.txt

ENV APP_HOME /app
WORKDIR ${APP_HOME}
COPY . .

EXPOSE 8080/udp
EXPOSE 8080/tcp

CMD exec gunicorn --bind :$PORT --workers 1 --threads 1 --timeout 0 main:app