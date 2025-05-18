from celery import Celery
from woocommerce import API
import json
import os
from loguru import logger
from typing import Dict, Set, List, Optional, Any
import redis
from datetime import datetime
import asyncpg
from config import settings
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pydantic import BaseModel
import re
import asyncio
from contextlib import asynccontextmanager

# Настройка Celery
celery_app = Celery('poizon_parser',
                    broker=f'redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}',
                    backend=f'redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB + 1}')

# Настройка Redis для хранения состояния
redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB + 2
)

# Глобальные переменные для хранения состояния обработки
processing_states: Dict[str, Dict] = {}
processed_ids: Dict[str, Set[int]] = {}

# Стандартные размеры одежды
STANDARD_SIZES = {
    'XXS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', 'XXXL',
    '2XS', '3XS', '4XS', '5XS',
    '2XL', '3XL', '4XL', '5XL',
    
    # Международные размеры обуви
    'UK3', 'UK4', 'UK5', 'UK6', 'UK7', 'UK8', 'UK9', 'UK10', 'UK11', 'UK12',
    'US5', 'US6', 'US7', 'US8', 'US9', 'US10', 'US11', 'US12',
    'EU35', 'EU36', 'EU37', 'EU38', 'EU39', 'EU40', 'EU41', 'EU42', 'EU43', 'EU44', 'EU45', 'EU46'
}

# Словарь для конвертации размеров
SIZE_CONVERSIONS = {
    # EU в RU для обуви
    'EU35': '35', 'EU36': '36', 'EU37': '37', 'EU38': '38',
    'EU39': '39', 'EU40': '40', 'EU41': '41', 'EU42': '42',
    'EU43': '43', 'EU44': '44', 'EU45': '45', 'EU46': '46',
    
    # UK в RU примерно
    'UK3': '35.5', 'UK4': '37', 'UK5': '38',
    'UK6': '39.5', 'UK7': '41', 'UK8': '42',
    'UK9': '43', 'UK10': '44.5', 'UK11': '45.5',
    
    # US в RU примерно
    'US5': '35', 'US6': '36.5', 'US7': '38',
    'US8': '39.5', 'US9': '41', 'US10': '42.5',
    'US11': '44', 'US12': '45.5'
}

def normalize_size(size: str) -> str:
    """Нормализует размер в единый формат"""
    size = size.upper().strip()
    
    # Заменяем различные варианты записи дробных размеров
    size = size.replace('½', '.5').replace('1/2', '.5')
    size = re.sub(r'(\d+)\s*/\s*2', r'\1.5', size)  # Заменяем X/2 на X.5
    
    # Удаляем пробелы между цифрами и буквами
    size = re.sub(r'\s+', '', size)
    
    # Обработка специальных случаев для обуви
    if re.match(r'^(10|12|[3-9])$', size):  # Просто цифры 3-12
        return size
    
    # Добавляем префиксы для международных размеров
    if re.match(r'^(3[5-9]|4[0-6])$', size):
        size = f'EU{size}'
    elif re.match(r'^(UK|ЮК)?\s*(10|12|[3-9])([\.,]5)?$', size):
        base_size = re.search(r'(10|12|[3-9])([\.,]5)?', size).group()
        return base_size.replace(',', '.')
    elif re.match(r'^(US|УС)?\s*(10|12|[5-9])([\.,]5)?$', size):
        base_size = re.search(r'(10|12|[5-9])([\.,]5)?', size).group()
        return base_size.replace(',', '.')
    
    # Конвертируем в RU размер если возможно
    return SIZE_CONVERSIONS.get(size, size)

def is_valid_size(size: str) -> bool:
    size = size.upper()
    
    # Проверяем стандартные размеры
    if size in STANDARD_SIZES:
        return True
    
    # Проверяем простые числовые размеры обуви (3-12)
    if re.match(r'^(10|12|[3-9])$', size):
        return True
    
    # Проверяем дробные размеры обуви (например, 8.5, 9.5)
    if re.match(r'^(10|12|[3-9])[\.,]5$', size):
        return True
        
    # Проверяем остальные числовые размеры
    if re.match(r'^\d+([.,]\d+)?$', size):
        num = float(size.replace(',', '.'))
        return 3 <= num <= 50  # Расширенный диапазон размеров
        
    # Проверяем международные размеры
    return bool(re.match(r'^(EU|UK|US)\d+([.,]5)?$', size))

def clean_size(size: str) -> str:
    """Очищает и нормализует размер"""
    # Заменяем различные варианты дробных значений
    size = size.replace('½', '.5').replace('1/2', '.5')
    size = re.sub(r'(\d+)\s*/\s*2', r'\1.5', size)
    
    # Очищаем от лишних символов, сохраняя точки и запятые
    cleaned = re.sub(r'[^0-9A-Za-z,.]', '', size)
    
    # Нормализуем размер
    return normalize_size(cleaned)

def clean_text(text: str, allow_english: bool = True) -> str:
    if allow_english:
        cleaned = re.sub(r'[^а-яА-ЯёЁa-zA-Z0-9\s\-.,!?()]', '', text)
    else:
        cleaned = re.sub(r'[^а-яА-ЯёЁ0-9\s\-.,!?()]', '', text)
    return cleaned.strip()

def get_store_key(store_url: str) -> str:
    """Создает уникальный ключ для магазина"""
    return f"store:{store_url}"

def save_processing_state(store_url: str, state: Dict):
    """Сохраняет состояние обработки в Redis"""
    try:
        key = get_store_key(store_url)
        redis_client.set(key, json.dumps(state))
    except redis.RedisError as e:
        logger.error(f"Redis error while saving state: {e}")

def load_processing_state(store_url: str) -> Dict:
    """Загружает состояние обработки из Redis"""
    try:
        key = get_store_key(store_url)
        state = redis_client.get(key)
        return json.loads(state) if state else {}
    except redis.RedisError as e:
        logger.error(f"Redis error while loading state: {e}")
        return {}

async def get_product_data(conn: asyncpg.Connection, product_id: int) -> Optional[Dict]:
    """Получает все данные о товаре из БД"""
    try:
        # Получаем основную информацию о товаре
        product = await conn.fetchrow(
            """
            SELECT * FROM products WHERE id = $1
            """,
            product_id
        )
        
        if not product:
            return None
            
        # Получаем все характеристики товара одним запросом
        attributes = await conn.fetch(
            """
            SELECT name, value, group_name 
            FROM product_attributes 
            WHERE product_id = $1
            ORDER BY group_name, name
            """,
            product_id
        )
        
        # Оптимизированный запрос для получения вариаций с их характеристиками
        variations = await conn.fetch(
            """
            WITH RECURSIVE var_data AS (
                SELECT 
                    v.id,
                    v.product_id,
                    v.price,
                    v.original_price,
                    v.quantity,
                    v.sku,
                    json_agg(
                        json_build_object(
                            'name', va.name,
                            'value', va.value,
                            'group_name', va.group_name
                        )
                    ) FILTER (WHERE va.name IS NOT NULL) as attributes,
                    (
                        SELECT json_agg(
                            json_build_object(
                                'id', vi.id,
                                'url', vi.image_url
                            ) ORDER BY vi.id
                        )
                        FROM variation_images vi
                        WHERE vi.variation_id = v.id
                    ) as images
                FROM product_variations v
                LEFT JOIN variation_attributes va ON v.id = va.variation_id
                WHERE v.product_id = $1
                GROUP BY v.id, v.product_id, v.price, v.original_price, v.quantity, v.sku
                ORDER BY v.id
            )
            SELECT * FROM var_data
            """,
            product_id
        )
        
        # Формируем структуру данных
        product_data = dict(product)
        product_data['attributes'] = [dict(attr) for attr in attributes]
        
        # Преобразуем JSON в Python объекты
        variations_data = []
        for var in variations:
            var_data = dict(var)
            var_data['attributes'] = json.loads(var_data['attributes']) if var_data['attributes'] else []
            # Сортируем изображения по ID и извлекаем только URL
            images_data = json.loads(var_data['images']) if var_data['images'] else []
            var_data['images'] = [img['url'] for img in sorted(images_data, key=lambda x: x['id'])]
            variations_data.append(var_data)
            
        product_data['variations'] = variations_data
        
        return product_data
    except Exception as e:
        logger.error(f"Ошибка при получении данных товара {product_id}: {e}")
        return None

async def get_variation_size(conn: asyncpg.Connection, param_id: int) -> Optional[str]:
    """Получает размер вариации"""
    param = await conn.fetchrow(
        """
        SELECT name FROM params WHERE id = $1
        """,
        param_id
    )
    return param['name'] if param else None

async def get_product_attributes(conn: asyncpg.Connection, product_id: int) -> List[Dict]:
    """Получает все характеристики товара"""
    try:
        # Получаем все характеристики товара с группировкой
        attributes = await conn.fetch(
            """
            SELECT name, value, group_name 
            FROM product_attributes 
            WHERE product_id = $1
            ORDER BY group_name, name
            """,
            product_id
        )
        
        # Группируем характеристики по group_name
        grouped_attributes = {}
        for attr in attributes:
            group = attr['group_name'] or 'Общие характеристики'
            if group not in grouped_attributes:
                grouped_attributes[group] = set()
            grouped_attributes[group].add(attr['value'])
        
        # Формируем список атрибутов для WooCommerce
        woo_attributes = []
        for group_name, values in grouped_attributes.items():
            woo_attributes.append({
                "name": group_name,
                "options": sorted(list(values)),
                "visible": True,
                "variation": False
            })
        
        return woo_attributes
    except Exception as e:
        logger.error(f"Ошибка при получении характеристик товара {product_id}: {e}")
        return []

async def create_woocommerce_product(product_data: Dict, wcapi: API, conn: asyncpg.Connection, category_id: int):
    """Создает товар в WooCommerce"""
    try:
        # Получаем характеристики товара
        attributes = await get_product_attributes(conn, product_data['id'])

        # Основные данные товара
        product_name = clean_text(product_data['name'] if '/' not in product_data['name'] else product_data['name'].split('/')[-1], allow_english=True)
        
        # Проверяем, существует ли уже товар с таким именем
        existing_products = wcapi.get("products", params={"search": product_name}).json()
        for existing_product in existing_products:
            if existing_product['name'] == product_name:
                logger.warning(f"Товар с именем '{product_name}' уже существует в WooCommerce (ID: {existing_product['id']})")
                return

        woo_product_data = {
            "name": product_name,
            "type": "variable" if len(product_data['variations']) > 1 else "simple",
            "description": clean_text(product_data['description_clear'], allow_english=True),
            "categories": [{"id": category_id}],
            "brands": [{"name": clean_text(product_data['brand'])}],
            "images": [],
            "attributes": [
                {
                    "name": "Бренд",
                    "options": [clean_text(product_data['brand'])],
                    "visible": True,
                    "variation": False
                }
            ]
        }

        # Добавляем все характеристики товара
        woo_product_data["attributes"].extend(attributes)

        # Собираем все размеры и их цены
        size_prices = {}
        for variation in product_data['variations']:
            size = None
            for attr in variation['attributes']:
                if attr['group_name'] == 'Дизайн':
                    size = clean_size(attr['value'])
                    break
            
            if size and is_valid_size(size) and variation['price'] and variation['price'] > 0:
                size_prices[size] = variation['price']

        # Добавляем размеры как атрибут вариации
        if size_prices:
            woo_product_data["attributes"].append({
                "name": "Размер",
                "options": sorted(list(size_prices.keys())),
                "visible": True,
                "variation": True
            })

        # Добавление изображений из первой вариации
        if product_data['variations'] and product_data['variations'][0]['images']:
            # Используем только уникальные изображения из первой вариации и сортируем их
            woo_product_data["images"] = [{"src": url} for url in product_data['variations'][0]['images']]

        # Создание основного товара
        response = wcapi.post("products", woo_product_data)
        if response.status_code not in [200, 201]:
            logger.error(f"Ошибка создания товара: {response.text}")
            return

        parent_id = response.json()['id']
        logger.info(f"Создан товар {parent_id}: {product_name}")

        # Создание вариаций
        for index, variation in enumerate(product_data['variations']):
            # Получаем размер из характеристик вариации
            size = None
            for attr in variation['attributes']:
                if attr['group_name'] == 'Дизайн':
                    size = attr['value']
                    break

            if not size:
                logger.warning(f"Пропущена вариация {index + 1} для товара {parent_id}: невалидный размер {size}")
                continue

            # Используем цену конкретного размера
            variation_price = variation['price'] or 0.00

            logger.info(f"Размер: {size}, Цена: {variation_price}")

            if variation_price == 0:
                logger.warning(f"Пропущена вариация {index + 1} для товара {parent_id}: нулевая цена")
                continue

            # Проверяем, есть ли размер в списке size_prices (чтобы не создавать вариации для размеров с нулевой ценой)
            if size not in size_prices:
                logger.warning(f"Пропущена вариация {index + 1} для товара {parent_id}: размер {size} не в списке валидных размеров")
                continue

            variation_data = {
                "regular_price": str(int(round(variation_price, 2))),
                "price": str(int(round(variation_price, 2))),
                "stock_quantity": int(variation['quantity']) if isinstance(variation['quantity'], (int, float)) else 0,
                "sku": f"{variation['sku']}-{index + 1}",
                "attributes": [{
                    "name": "Размер",
                    "option": size
                }],
                # Используем только уникальные изображения для вариации и сортируем их
                "image": [{"src": url} for url in sorted(set(variation['images']))]
            }

            try:
                resp = wcapi.post(f"products/{parent_id}/variations", variation_data)
                if resp.status_code not in [200, 201]:
                    logger.error(f"Ошибка создания вариации {index + 1} для товара {parent_id}: {resp.text}")
                else:
                    logger.info(f"Создана вариация {index + 1} (размер {size}) для товара {parent_id} с ценой {variation_price}")
            except Exception as e:
                logger.error(f"Ошибка создания вариации {index + 1} для товара {parent_id}: {e}")

    except Exception as e:
        logger.error(f"Ошибка при создании товара в WooCommerce: {e}")

@asynccontextmanager
async def get_db_connection():
    """Контекстный менеджер для подключения к базе данных"""
    conn = await asyncpg.connect(settings.DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()

def get_processed_products_key(store_url: str) -> str:
    """Создает ключ для хранения обработанных товаров в Redis"""
    return f"processed_products:{store_url}"

def save_processed_products(store_url: str, product_ids: List[int]):
    """Сохраняет список обработанных товаров в Redis"""
    try:
        key = get_processed_products_key(store_url)
        # Используем Redis Set для хранения ID товаров
        redis_client.sadd(key, *product_ids)
        # Устанавливаем время жизни ключа (30 дней)
        redis_client.expire(key, 30 * 24 * 60 * 60)
    except redis.RedisError as e:
        logger.error(f"Redis error while saving processed products: {e}")

def get_processed_products(store_url: str) -> Set[int]:
    """Получает список обработанных товаров из Redis"""
    try:
        key = get_processed_products_key(store_url)
        # Получаем все ID из Redis Set
        return {int(id) for id in redis_client.smembers(key)}
    except redis.RedisError as e:
        logger.error(f"Redis error while getting processed products: {e}")
        return set()

async def get_unprocessed_products(conn: asyncpg.Connection, last_processed_id: Optional[int] = None, store_url: Optional[str] = None) -> List[int]:
    """Получает список необработанных товаров в обратном порядке"""
    try:
        # Проверяем общее количество товаров
        total_count = await conn.fetchval("SELECT COUNT(*) FROM products")
        logger.info(f"Total products in database: {total_count}")
        
        # Получаем максимальный ID товара
        max_id = await conn.fetchval("SELECT MAX(id) FROM products")
        logger.info(f"Maximum product ID: {max_id}")
        
        # Получаем уже обработанные товары из Redis
        processed_ids = set()
        if store_url:
            processed_ids = get_processed_products(store_url)
            logger.info(f"Found {len(processed_ids)} already processed products in Redis")
        
        # Формируем базовый запрос
        query = """
        SELECT id FROM products
        WHERE id > 0
        """
        params = []
        
        if last_processed_id:
            query += " AND id < $1"
            params.append(last_processed_id)
            logger.info(f"Using last_processed_id: {last_processed_id}")
        else:
            query += " AND id <= $1"
            params.append(max_id)
            logger.info(f"Starting from maximum ID: {max_id}")
            
        if processed_ids:
            # Добавляем условие для исключения уже обработанных товаров
            query += " AND id NOT IN (SELECT unnest($2::int[]))"
            params.append(list(processed_ids))
            
        query += " ORDER BY id DESC"
        
        products = await conn.fetch(query, *params)
        
        logger.info(f"Found {len(products)} products to process")
        if products:
            logger.info(f"First product ID: {products[0]['id']}, Last product ID: {products[-1]['id']}")
            logger.info(f"Sample of product IDs: {[p['id'] for p in products[:5]]}")
        return [p['id'] for p in products]
    except Exception as e:
        logger.error(f"Error fetching unprocessed products: {e}")
        return []

async def process_product_batch(product_ids: List[int], wcapi: API, conn: asyncpg.Connection, category_id: int):
    """Обработка пакета товаров"""
    try:
        for product_id in product_ids:
            try:
                product_data = await get_product_data(conn, product_id)
                if product_data:
                    await create_woocommerce_product(product_data, wcapi, conn, category_id)
                await asyncio.sleep(0.5)  # Небольшая задержка между товарами
            except Exception as e:
                logger.error(f"Error processing product {product_id}: {e}")
                continue
        return True
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        return False

async def process_all_batches(product_ids: List[int], wcapi: API, category_id: int):
    """Обработка всех товаров последовательно"""
    async with get_db_connection() as conn:
        results = []
        for product_id in product_ids:
            try:
                product_data = await get_product_data(conn, product_id)
                if product_data:
                    await create_woocommerce_product(product_data, wcapi, conn, category_id)
                    results.append(True)
                await asyncio.sleep(0.5)  # Небольшая задержка между товарами
            except Exception as e:
                logger.error(f"Error processing product {product_id}: {e}")
                results.append(False)
        return results

async def process_products_parallel(product_ids: List[int], wcapi: API, category_id: int, thread_id: int, store_url: str):
    """Обработка товаров в отдельном потоке"""
    try:
        async with get_db_connection() as conn:
            for product_id in product_ids:
                try:
                    # Проверяем, не была ли запрошена остановка
                    current_state = load_processing_state(store_url)
                    if current_state.get('status') == 'stopped':
                        logger.info(f"Processing stopped for store {store_url} in thread {thread_id}")
                        return

                    product_data = await get_product_data(conn, product_id)
                    if product_data:
                        await create_woocommerce_product(product_data, wcapi, conn, category_id)
                        logger.info(f"Thread {thread_id}: Processed product {product_id}")
                    await asyncio.sleep(0.5)  # Небольшая задержка между товарами
                except Exception as e:
                    logger.error(f"Thread {thread_id}: Error processing product {product_id}: {e}")
                    continue
    except Exception as e:
        logger.error(f"Thread {thread_id}: Error in parallel processing: {e}")

@celery_app.task(bind=True, max_retries=3)
def process_products(self, store_url: str, consumer_key: str, consumer_secret: str, category_id: int, last_processed_id: Optional[int] = None):
    """Обрабатывает товары для указанного магазина"""
    try:
        # Инициализируем WooCommerce API
        wcapi = API(
            url=str(store_url),
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            version="wc/v3",
            timeout=60
        )
        
        # Сохраняем начальное состояние
        state = {
            'status': 'running',
            'store_url': store_url,
            'consumer_key': consumer_key,
            'consumer_secret': consumer_secret,
            'category_id': category_id,
            'started_at': datetime.now().isoformat(),
            'processed_count': 0,
            'total_count': 0,
            'last_processed_id': last_processed_id
        }
        save_processing_state(store_url, state)

        def process_single_product(product_id: int, thread_id: int):
            """Обработка одного товара в отдельном потоке"""
            try:
                # Создаем отдельное подключение к БД для каждого товара
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def process_product():
                    async with get_db_connection() as conn:
                        try:
                            # Проверяем, не была ли запрошена остановка
                            current_state = load_processing_state(store_url)
                            if current_state.get('status') == 'stopped':
                                logger.info(f"Processing stopped for store {store_url} in thread {thread_id}")
                                return

                            product_data = await get_product_data(conn, product_id)
                            if product_data:
                                await create_woocommerce_product(product_data, wcapi, conn, category_id)
                                logger.info(f"Thread {thread_id}: Processed product {product_id}")
                        except Exception as e:
                            logger.error(f"Thread {thread_id}: Error processing product {product_id}: {e}")
                
                loop.run_until_complete(process_product())
                loop.close()
            except Exception as e:
                logger.error(f"Thread {thread_id}: Error in thread processing: {e}")

        async def get_products():
            async with get_db_connection() as conn:
                # Получаем список необработанных товаров
                product_ids = await get_unprocessed_products(conn, last_processed_id, store_url)
                
                # Обновляем общее количество
                state['total_count'] = len(product_ids)
                save_processing_state(store_url, state)
                
                # Используем ThreadPoolExecutor для истинного параллельного выполнения
                with ThreadPoolExecutor(max_workers=15) as executor:
                    futures = []
                    batch_size = 100  # Размер пакета для обработки
                    
                    for i in range(0, len(product_ids), batch_size):
                        batch = product_ids[i:i + batch_size]
                        
                        # Проверяем, не была ли запрошена остановка
                        current_state = load_processing_state(store_url)
                        if current_state.get('status') == 'stopped':
                            logger.info(f"Processing stopped for store {store_url}")
                            break
                        
                        # Обрабатываем пакет
                        for product_id in batch:
                            future = executor.submit(process_single_product, product_id, i + 1)
                            futures.append(future)
                        
                        # Ждем завершения текущего пакета
                        for future in futures[-len(batch):]:
                            future.result()
                        
                        # Сохраняем обработанные товары в Redis
                        save_processed_products(store_url, batch)
                        
                        # Обновляем прогресс после каждого пакета
                        state['processed_count'] = min(i + batch_size, len(product_ids))
                        save_processing_state(store_url, state)
                        
                        # Небольшая пауза между пакетами
                        time.sleep(1)
                
                # Проверяем, не была ли запрошена остановка
                current_state = load_processing_state(store_url)
                if current_state.get('status') == 'stopped':
                    logger.info(f"Processing stopped for store {store_url}")
                    return
                
                # Обновляем прогресс
                state['processed_count'] = len(product_ids)
                state['last_processed_id'] = product_ids[-1] if product_ids else last_processed_id
                save_processing_state(store_url, state)
        
        # Запускаем асинхронную обработку
        asyncio.run(get_products())
        
        # Проверяем финальное состояние
        final_state = load_processing_state(store_url)
        if final_state.get('status') != 'stopped':
            # Обновляем финальное состояние только если не было остановки
            state['status'] = 'completed'
            state['completed_at'] = datetime.now().isoformat()
            save_processing_state(store_url, state)
        
        return {"status": state['status'], "store_url": store_url}
    except Exception as e:
        logger.error(f"Error processing products for store {store_url}: {e}")
        state['status'] = 'error'
        state['error'] = str(e)
        state['error_at'] = datetime.now().isoformat()
        save_processing_state(store_url, state)
        raise

@celery_app.task
def stop_processing(store_url: str):
    """Останавливает обработку для указанного магазина"""
    store_url = f"https://{store_url}"
    try:
        # Сохраняем состояние остановки в Redis
        state = load_processing_state(store_url)
        if not state:
            raise ValueError(f"No processing state found for store: {store_url}")
            
        state['status'] = 'stopped'
        state['stopped_at'] = datetime.now().isoformat()
        save_processing_state(store_url, state)
        
        # Отменяем все активные задачи для этого магазина
        try:
            celery_app.control.cancel_consumer(f'store_{store_url}')
        except Exception as e:
            logger.warning(f"Could not cancel consumer for store {store_url}: {e}")
        
        # Очищаем состояние обработки
        processing_states.pop(store_url, None)
        processed_ids.pop(store_url, None)
        
        logger.info(f"Processing stopped for store: {store_url}")
        return {"status": "stopped", "store_url": store_url}
    except Exception as e:
        logger.error(f"Error stopping processing for store {store_url}: {e}")
        raise

@celery_app.task
def resume_processing(store_url: str):
    """Возобновляет обработку для указанного магазина"""
    store_url = f"https://{store_url}"
    try:
        # Загружаем предыдущее состояние
        state = load_processing_state(store_url)
        if not state:
            raise ValueError(f"No processing state found for store: {store_url}")
            
        # Проверяем, что предыдущее состояние было остановлено
        if state.get('status') != 'stopped':
            raise ValueError(f"Store {store_url} is not in stopped state")
            
        # Обновляем состояние
        state['status'] = 'running'
        state['resumed_at'] = datetime.now().isoformat()
        save_processing_state(store_url, state)
        
        # Возобновляем обработку с последнего обработанного ID
        last_processed_id = state.get('last_processed_id')
        if last_processed_id:
            # Создаем новую задачу с теми же параметрами
            process_products.delay(
                store_url,
                state['consumer_key'],
                state['consumer_secret'],
                state['category_id'],
                last_processed_id=last_processed_id
            )
            logger.info(f"Processing resumed for store: {store_url} from ID: {last_processed_id}")
        else:
            # Если нет last_processed_id, начинаем с начала
            process_products.delay(
                store_url,
                state['consumer_key'],
                state['consumer_secret'],
                state['category_id']
            )
            logger.info(f"Processing resumed for store: {store_url} from beginning")
        
        return {"status": "resumed", "store_url": store_url}
    except Exception as e:
        logger.error(f"Error resuming processing for store {store_url}: {e}")
        raise

@celery_app.task(bind=True, max_retries=3)
def process_single_product_task(self, store_url: str, consumer_key: str, consumer_secret: str, category_id: int, product_id: int):
    """Обрабатывает один товар для указанного магазина"""
    try:
        # Инициализируем WooCommerce API
        wcapi = API(
            url=str(store_url),
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            version="wc/v3",
            timeout=60
        )
        
        # Сохраняем начальное состояние
        state = {
            'status': 'running',
            'store_url': store_url,
            'consumer_key': consumer_key,
            'consumer_secret': consumer_secret,
            'category_id': category_id,
            'started_at': datetime.now().isoformat(),
            'processed_count': 0,
            'total_count': 1,
            'product_id': product_id
        }
        save_processing_state(store_url, state)

        async def process_product():
            async with get_db_connection() as conn:
                try:
                    # Получаем данные товара
                    product_data = await get_product_data(conn, product_id)
                    if product_data:
                        # Создаем товар в WooCommerce
                        await create_woocommerce_product(product_data, wcapi, conn, category_id)
                        # Сохраняем ID обработанного товара
                        save_processed_products(store_url, [product_id])
                        state['status'] = 'completed'
                        state['completed_at'] = datetime.now().isoformat()
                        state['processed_count'] = 1
                    else:
                        state['status'] = 'error'
                        state['error'] = f"Product {product_id} not found"
                        state['error_at'] = datetime.now().isoformat()
                except Exception as e:
                    state['status'] = 'error'
                    state['error'] = str(e)
                    state['error_at'] = datetime.now().isoformat()
                    logger.error(f"Error processing product {product_id}: {e}")
                finally:
                    save_processing_state(store_url, state)

        # Запускаем асинхронную обработку
        asyncio.run(process_product())
        
        return {"status": state['status'], "store_url": store_url, "product_id": product_id}
    except Exception as e:
        logger.error(f"Error processing product {product_id} for store {store_url}: {e}")
        state['status'] = 'error'
        state['error'] = str(e)
        state['error_at'] = datetime.now().isoformat()
        save_processing_state(store_url, state)
        raise