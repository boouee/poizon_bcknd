from celery.schedules import crontab
from config import settings

# Настройки брокера и бэкенда
broker_url = f'redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}'
result_backend = f'redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB + 1}'

# Настройки сериализации
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Europe/Moscow'
enable_utc = True

# Настройки для обработки задач
task_track_started = True
task_time_limit = 3600  # 1 час
task_soft_time_limit = 3300  # 55 минут
task_max_retries = 3
task_retry_delay = 60  # 1 минута
task_retry_backoff = True
task_retry_backoff_max = 600  # 10 минут
task_retry_jitter = True

# Настройки для обработки ошибок
task_acks_late = True
task_reject_on_worker_lost = True
task_ignore_result = False
task_store_errors_even_if_ignored = True

# Настройки для очередей
task_default_queue = 'default'
task_queues = {
    'default': {
        'exchange': 'default',
        'routing_key': 'default',
        'queue_arguments': {'x-max-priority': 10},
    },
    'high_priority': {
        'exchange': 'high_priority',
        'routing_key': 'high_priority',
        'queue_arguments': {'x-max-priority': 20},
    },
    'low_priority': {
        'exchange': 'low_priority',
        'routing_key': 'low_priority',
        'queue_arguments': {'x-max-priority': 5},
    }
}

# Настройки для воркеров
worker_prefetch_multiplier = 1
worker_max_tasks_per_child = 100
worker_max_memory_per_child = 200000  # 200MB
worker_send_task_events = True
task_send_sent_event = True

# Настройки для мониторинга
flower_basic_auth = ['admin:admin']  # Замените на реальные учетные данные
flower_port = 5555
flower_url_prefix = 'flower'

# Периодические задачи
beat_schedule = {
    'cleanup-old-tasks': {
        'task': 'tasks.cleanup_old_tasks',
        'schedule': crontab(hour=0, minute=0),  # Каждый день в полночь
    },
    'check-processing-status': {
        'task': 'tasks.check_processing_status',
        'schedule': crontab(minute='*/5'),  # Каждые 5 минут
    }
} 