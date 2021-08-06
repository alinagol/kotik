import os

from celery import Celery

broker_url = result_backend = os.getenv("REDIS_URL", "redis://redis:6379")

celery_parameters = {
    "main": "asyncworker",
    "broker": broker_url,
    "backend": broker_url,
}

celery_app = Celery(**celery_parameters)
