import celeryconfig
from celery import Celery

celery_app = Celery("asyncworker", include=["tasks"])
celery_app.config_from_object(celeryconfig)
