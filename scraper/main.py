from scraper.database import DatabaseManager
from scraper.utils import save_to_json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
from datetime import datetime
from typing import List, Dict
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)


def configure_driver() -> webdriver.Chrome:
    """Настройка и возврат драйвера Chrome"""
    chrome_options = Options()
    # Сделал без фона, ибо не видно когда тебя мвидео банит
    ##chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-web-security")

    driver_path = "C:\\Program Files\\Google\\Chrome\\Application\\chromedriver.exe"
    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def scrape_mvideo(url: str) -> List[Dict]:
    """Основная функция скрапинга данных с MVideo"""
    driver = configure_driver()
    products = []
    try:
        logger.info(f"Начинаем скрапинг страницы: {url}")
        driver.get(url)
        time.sleep(30)


        def smooth_scroll_to_bottom():
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            while current_position < scroll_height:
                driver.execute_script(f"window.scrollTo(0, {current_position});")
                current_position += 500  # Шаг скролла
                time.sleep(0.2)  # Интервал между шагами

        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            smooth_scroll_to_bottom()
            time.sleep(5)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        soup = BeautifulSoup(driver.page_source, 'html.parser')


        product_cards = soup.find_all('div', class_='product-cards-layout__item')
        for card in product_cards:
            try:
                product = extract_product_data(card)
                if product:
                    products.append(product)
            except Exception as e:
                logger.error(f"Ошибка при обработке карточки товара: {e}")
                continue
    except Exception as e:
        logger.error(f"Ошибка при скрапинге: {e}")
    finally:
        try:
            driver.quit()  # Используйте quit() вместо close()
        except Exception as e:
            logger.error(f"Ошибка при закрытии драйвера: {e}")
    return products


def extract_product_data(card) -> Dict:
    try:
        # Проверка наличия элемента с названием
        name_element = card.find('a', {'class': 'product-title__text'})
        if not name_element:
            logger.warning("Элемент названия товара не найден")
            return None
        name = name_element.text.strip()

        # Проверка наличия элемента с ценой
        price_element = card.find('span', {'class': 'price__main-value'})
        if not price_element:
            logger.warning("Элемент цены не найден")
            return None
        price = price_element.text.strip()

        # Конвертация цены с обработкой ошибок
        try:
            price_clean = int(''.join(c for c in price if c.isdigit()))
        except ValueError:
            logger.error(f"Некорректная цена: {price}")
            return None

        # Проверка наличия атрибута href
        if 'href' not in name_element.attrs:
            logger.warning("Отсутствует атрибут href в названии товара")
            return None
        url = f"https://www.mvideo.ru{name_element['href']}"

        # Извлечение бренда (первое слово после "Смартфон" или первое слово)
        brand = "Unknown"
        parts = name.split()
        if "Смартфон" in parts:
            brand_index = parts.index("Смартфон") + 1
            if brand_index < len(parts):
                brand = parts[brand_index]
        else:
            brand = parts[0] if parts else "Unknown"

        logger.debug(f"Успешно обработан товар: {name}, {brand}, {price_clean}")
        return {
            "name": name,
            "price": price_clean,
            "url": url,
            "brand": brand,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = scrape_mvideo("https://www.mvideo.ru/smartfony-i-svyaz-10/smartfony-205")
    db = DatabaseManager()
    scrape_id = "manual_run_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    db.save_products(data, scrape_id)