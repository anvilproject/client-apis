# syntax=docker/dockerfile:1
FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim

ENV PYTHONUNBUFFERED True
ENV GOOGLE_APPLICATION_CREDENTIALS ./creds.json

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENV APP_HOME /app
WORKDIR ${APP_HOME}
COPY . .

EXPOSE 8080/udp
EXPOSE 8080/tcp

CMD exec gunicorn --bind :$PORT --workers 1 --threads 1 --timeout 0 main:app