from fastapi import FastAPI, HTTPException, Request, Depends, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, HttpUrl
from typing import Optional, List
from celery.result import AsyncResult
from tasks import process_products, stop_processing, resume_processing, process_single_product_task
from loguru import logger
from datetime import datetime
import asyncpg
import redis
import time
from config import settings

# Настройка Redis
redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB
)

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_PREFIX}/openapi.json"
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Модели данных
class StoreConfig(BaseModel):
    url: str
    consumer_key: str
    consumer_secret: str
    category_id: int

class ProcessingStatus(BaseModel):
    store_url: str
    status: str
    processed_count: int
    total_count: int
    last_processed_id: Optional[int] = None

# Rate limiting middleware
def rate_limit_middleware(request: Request):
    client_ip = request.client.host
    current_time = time.time()
    key = f"rate_limit:{client_ip}"
    
    current_requests = redis_client.get(key)
    if current_requests is None:
        redis_client.setex(key, 60, 1)
        return True
    
    current_requests = int(current_requests)
    if current_requests >= settings.RATE_LIMIT_PER_MINUTE:
        return False
    
    redis_client.incr(key)
    return True

@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if not rate_limit_middleware(request):
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests"}
        )
    response = await call_next(request)
    return response

# Зависимость для получения подключения к базе данных
async def get_db():
    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()

# Эндпоинты
@app.post("/start_processing")
async def start_processing(
    store: StoreConfig,
    conn: asyncpg.Connection = Depends(get_db)
):
    try:
        # Проверяем существование магазина в базе данных
        existing_store = await conn.fetchrow(
            "SELECT * FROM stores WHERE url = $1",
            str(store.url)
        )
        
        if existing_store:
            # Если магазин существует, обновляем его данные
            await conn.execute(
                """
                UPDATE stores 
                SET consumer_key = $1, consumer_secret = $2, category_id = $3
                WHERE url = $4
                """,
                store.consumer_key,
                store.consumer_secret,
                store.category_id,
                str(store.url)
            )
        else:
            # Если магазин не существует, создаем новый
            await conn.execute(
                """
                INSERT INTO stores (url, consumer_key, consumer_secret, category_id)
                VALUES ($1, $2, $3, $4)
                """,
                str(store.url),
                store.consumer_key,
                store.consumer_secret,
                store.category_id
            )

        task = process_products.delay(
            str(store.url),
            store.consumer_key,
            store.consumer_secret,
            store.category_id
        )
        return {"task_id": task.id, "status": "started"}
    except Exception as e:
        logger.error(f"Error starting processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/stop_processing/{store_url}")
async def stop_store_processing(store_url: str):
    try:
        task = stop_processing.delay(store_url)
        return {"task_id": task.id, "status": "stopping"}
    except Exception as e:
        logger.error(f"Error stopping processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/resume_processing/{store_url}")
async def resume_store_processing(store_url: str):
    try:
        task = resume_processing.delay(store_url)
        return {"task_id": task.id, "status": "resuming"}
    except Exception as e:
        logger.error(f"Error resuming processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/task/{task_id}")
async def get_task_status(task_id: str):
    task_result = AsyncResult(task_id)
    return {
        "task_id": task_id,
        "status": task_result.status,
        "result": task_result.result if task_result.ready() else None
    }

@app.get("/health")
async def health_check(conn: asyncpg.Connection = Depends(get_db)):
    try:
        await conn.execute("SELECT 1")
        return {"status": "healthy"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")

@app.post("/process-single-product")
async def process_single_product(
    store_url: str = Form(...),
    consumer_key: str = Form(...),
    consumer_secret: str = Form(...),
    category_id: int = Form(...),
    product_id: int = Form(...)
):
    """
    Обрабатывает один товар для указанного магазина.
    
    Args:
        store_url: URL магазина WooCommerce
        consumer_key: Consumer Key WooCommerce
        consumer_secret: Consumer Secret WooCommerce
        category_id: ID категории в WooCommerce
        product_id: ID товара для обработки
    """
    try:
        # Запускаем задачу
        task = process_single_product_task.delay(
            store_url=store_url,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            category_id=category_id,
            product_id=product_id
        )
        
        return {
            "status": "success",
            "message": "Product processing started",
            "task_id": task.id,
            "product_id": product_id
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error starting product processing: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 