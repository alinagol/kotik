import os
import ssl

from kombu import Queue

# BROKER AND BACKEND
broker_url = result_backend = os.getenv("REDIS_URL", "redis://redis:6379")

if os.getenv("USE_SSL"):
    broker_use_ssl = {"ssl_cert_reqs": ssl.CERT_NONE}
    redis_backend_use_ssl = {"ssl_cert_reqs": ssl.CERT_NONE}

# QUEUES
task_queues = (
    Queue("default", routing_key="default"),
    Queue("priority", routing_key="priority"),
)

# All tasks are routed to "default" queue, unless specified otherwise
task_default_queue = "default"
task_default_exchange = "tasks"
task_default_routing_key = "default"
worker_direct = True

# SETTINGS
task_time_limit = 7200
ignore_result = True
