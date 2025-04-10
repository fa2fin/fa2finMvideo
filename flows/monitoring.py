from prefect import flow, task
from datetime import timedelta, datetime
from scraper.main import scrape_mvideo
from scraper.analyzedata import analyze_data
import logging
import sys
import io
import json
from typing import Any, Dict


sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.setStream(io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8'))
logging.basicConfig(handlers=[handler], level=logging.INFO)

logger = logging.getLogger("prefect")


def ensure_utf8(data: Any) -> Any:
    """Рекурсивно преобразует строки в данных к UTF-8"""
    if isinstance(data, str):
        try:
            return data.encode('utf-8').decode('utf-8')
        except UnicodeError:
            # Тут я заколебался с кодировками и проверяю другие
            for encoding in ['windows-1251', 'cp1252', 'iso-8859-1']:
                try:
                    return data.encode(encoding).decode('utf-8')
                except UnicodeError:
                    continue
            return data.encode('utf-8', errors='replace').decode('utf-8')
    elif isinstance(data, dict):
        return {ensure_utf8(key): ensure_utf8(value) for key, value in data.items()}
    elif isinstance(data, (list, tuple)):
        return [ensure_utf8(item) for item in data]
    return data


@task(retries=2, retry_delay_seconds=60)
def scrape_task(url: str):
    """Задача для скрапинга данных"""
    try:
        data = scrape_mvideo(url)
        return ensure_utf8(data)
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


@task
def analyze_task(data: Dict):
    """Задача для анализа данных"""
    try:
        processed_data = ensure_utf8(data)
        return analyze_data(processed_data)
    except Exception as e:
        logger.error(f"Ошибка при анализе данных: {str(e)}")
        raise


@flow(name="MVideo Price Monitoring", log_prints=True)
def monitor_prices(url: str = "https://www.mvideo.ru/smartfony-i-svyaz-10/smartfony-205"):
    """Основной flow для мониторинга цен"""
    logger.info("Starting MVideo price monitoring flow")

    try:

        scraped_data = scrape_task(url)


        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/data/mvideo_smartphones_{timestamp}.json"
        saved_file = save_task(scraped_data, filename)


        analysis_result = analyze_task(scraped_data)

        logger.info(f"Flow completed. Saved to {saved_file}")
        return analysis_result
    except Exception as e:
        logger.error(f"Ошибка в основном flow: {str(e)}")
        raise


if __name__ == "__main__":

    import os

    os.makedirs("data", exist_ok=True)


    import os

    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTHONIOENCODING"] = "utf-8"

    monitor_prices.serve(
        name="mvideo-price-monitoring",
        interval=timedelta(hours=24),
        parameters={
            "url": "https://www.mvideo.ru/smartfony-i-svyaz-10/smartfony-205"
        }
    )