FROM python:3.9.0

ENV REDIS_URL redis://redis:6379/0
ENV C_FORCE_ROOT true

COPY ./requirements.txt .
RUN pip install -r requirements.txt
RUN python -m nltk.downloader stopwords

COPY . /asyncworker
WORKDIR /asyncworker

ENTRYPOINT celery -A asyncworker worker