FROM python:3.8-slim-buster

ENV CELERY_BROKER_URL redis://redis:6379/0
ENV CELERY_RESULT_BACKEND redis://redis:6379/0
ENV C_FORCE_ROOT true

COPY ./requirements.txt .
RUN pip install -r requirements.txt
RUN python -m nltk.downloader stopwords

COPY . /asyncworker
WORKDIR /asyncworker

ENTRYPOINT celery -A tasks worker --loglevel=info