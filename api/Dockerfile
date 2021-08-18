FROM python:3.9.0

WORKDIR /app

# Celery
ENV REDIS_URL redis://redis:6379/0
ENV C_FORCE_ROOT true

# Flask
ENV HOST 0.0.0.0
ENV PORT 5001
ENV DEBUG true
EXPOSE 5001

COPY ./requirements.txt /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . /app

# run the app server
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--workers", "3", "app:app"]
