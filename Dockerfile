FROM python:3.8-slim-buster as kotik_base

ENV HOME /app/kotik
WORKDIR $HOME

ENV CELERY_BROKER_URL redis://redis:6379/0
ENV CELERY_RESULT_BACKEND redis://redis:6379/0
ENV C_FORCE_ROOT true
ENV HOST 0.0.0.0
ENV PORT 5001
ENV DEBUG true
EXPOSE 5001

COPY ./requirements.txt $HOME
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . .

### API
FROM kotik_base as api
WORKDIR $HOME/api
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--workers", "3", "app:app"]

### ASYNCWORKER
FROM kotik_base as worker
RUN python -m nltk.downloader stopwords
WORKDIR $HOME/asyncworker
CMD ["celery", "-A", "tasks", "worker"]


### NGINX
FROM nginx:1.17-alpine as nginx
RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d
