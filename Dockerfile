FROM python:3.10.0 as kotik_base

ENV HOME /app
WORKDIR $HOME

ENV CELERY_BROKER_URL redis://redis:6379/0
ENV CELERY_RESULT_BACKEND redis://redis:6379/0
ENV C_FORCE_ROOT true

COPY ./requirements.txt $HOME
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . .

### API
FROM kotik_base as api
WORKDIR $HOME/api
ENV HOST 0.0.0.0
ENV PORT 5001
ENV DEBUG true
EXPOSE 5001
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--workers", "3", "app:app"]

### ASYNCWORKER
FROM kotik_base as worker
RUN python -m nltk.downloader stopwords
WORKDIR $HOME/asyncworker
CMD ["celery", "-A", "asyncworker", "worker", "--loglevel=WARNING", "--concurrency=50"]

### NGINX
FROM nginx:1.17-alpine as nginx
RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d
