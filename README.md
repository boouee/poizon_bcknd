# Poizon Parser Backend

Бэкенд для обработки товаров из различных магазинов с использованием FastAPI и Celery.

## Требования

- Python 3.8+
- Redis
- WooCommerce API

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Убедитесь, что Redis запущен:
```bash
redis-server
```

3. Запустите Celery worker:
```bash
celery -A tasks worker --loglevel=info
```

4. Запустите FastAPI сервер:
```bash
uvicorn main:app --reload
```

## API Endpoints

### Запуск обработки товаров
```
POST /start_processing
{
    "url": "https://your-store.com",
    "consumer_key": "your_key",
    "consumer_secret": "your_secret",
    "category_id": 123
}
```

### Остановка обработки
```
POST /stop_processing/{store_url}
```

### Возобновление обработки
```
POST /resume_processing/{store_url}
```

### Получение статуса
```
GET /status/{store_url}
```

### Получение статуса задачи
```
GET /task/{task_id}
```

## Структура проекта

- `main.py` - FastAPI приложение
- `tasks.py` - Celery задачи
- `celery_config.py` - Конфигурация Celery
- `requirements.txt` - Зависимости проекта

## Особенности

- Асинхронная обработка товаров через Celery
- Сохранение состояния обработки в Redis
- Возможность остановки и возобновления обработки
- Поддержка множества магазинов
- Логирование через loguru 