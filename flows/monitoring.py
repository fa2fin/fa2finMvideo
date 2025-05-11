import io
import json
from typing import Any, Dict, List
from prefect import flow, task
from datetime import timedelta, datetime
from scraper.main import scrape_mvideo
from scraper.analyzedata import DataAnalyzer
from scraper.database import DatabaseManager
import logging
import sys
import uuid
from prefect.settings import PREFECT_API_DATABASE_CONNECTION_URL



PREFECT_API_DATABASE_CONNECTION_URL = "sqlite+aiosqlite:///prefect.db?journal_mode=WAL"

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


logger = logging.getLogger("prefect")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


def ensure_utf8(data: Any) -> Any:
    """Рекурсивно преобразует строки в данных к UTF-8"""
    if isinstance(data, bytes):
        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            return data.decode('utf-8', errors='replace')
    elif isinstance(data, str):
        return data
    elif isinstance(data, dict):
        return {ensure_utf8(k): ensure_utf8(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        return [ensure_utf8(item) for item in data]
    return data


@task(retries=2, retry_delay_seconds=60)
def scrape_task(url: str):
    """Задача для скрапинга данных"""
    try:
        return scrape_mvideo(url)
    except Exception as e:
        logger.error(f"Ошибка при скрапинге: {str(e)}")
        raise


@task
def save_task(data: Dict, filename: str):
    """Задача для сохранения данных с обработкой кодировки"""
    try:

        processed_data = ensure_utf8(data)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Данные успешно сохранены в {filename}")
        return filename
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла: {str(e)}")
        raise

def save_products(self, products: list, scrape_id: str):
    with self.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN TRANSACTION")
            for product in products:
                cursor.execute(
                    "INSERT INTO products (...) VALUES (...)",
                    (...,)
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise

@task
def analyze_task(data: List[Dict]):
    """Задача для анализа данных"""
    try:
        analyzer = DataAnalyzer()
        return analyzer.analyze_last_scrape()
    except Exception as e:
        logger.error(f"Ошибка при анализе данных: {str(e)}")
        raise


@flow(name="MVideo Price Monitoring", log_prints=True)
def monitor_prices(url: str = "https://www.mvideo.ru/smartfony-i-svyaz-10/smartfony-205"):
    """Основной flow для мониторинга цен"""
    logger.info("Starting MVideo price monitoring flow")
    db = DatabaseManager()

    try:
        # Генерируем уникальный ID для этого сбора данных
        scrape_id = str(uuid.uuid4())

        # 1. Сбор данных
        scraped_data = scrape_task(url)

        # 2. Сохранение в БД
        db.save_products(scraped_data, scrape_id)

        # 3. Анализ данных
        analysis_result = analyze_task(scraped_data)

        # 4. Сохранение результатов анализа
        db.save_analysis(analysis_result, scrape_id)

        logger.info(f"Flow completed. Scrape ID: {scrape_id}")
        return analysis_result

    except Exception as e:
        logger.error(f"Ошибка в основном flow: {str(e)}")
        raise


if __name__ == "__main__":
    monitor_prices.serve(
        name="mvideo-price-monitoring",
        interval=timedelta(hours=5),
        parameters={
            "url": "https://www.mvideo.ru/smartfony-i-svyaz-10/smartfony-205"
        }
    )