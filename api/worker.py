from celery import Celery
import os

# The hardcoded Redis host `redisservice` is a hostname created for the Redis
# container in the Docker network, defined in the docker-compose.yml.
# This is unfortunately specific to the currend Docker-compose setup and will
# not work if we want the Redis backend to be located elsewhere.

CELERY_BROKER_URL = (os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379"),)
CELERY_RESULT_BACKEND = os.environ.get(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379"
)

celery = Celery("tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)
