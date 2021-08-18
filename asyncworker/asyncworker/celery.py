from celery import Celery

from asyncworker import celeryconfig  # type: ignore[attr-defined]

celery_app = Celery("asyncworker")
celery_app.config_from_object(celeryconfig)
celery_app.autodiscover_tasks(["asyncworker.tasks"])
