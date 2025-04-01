from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
from datetime import datetime
from typing import List, Dict
import logging
from scraper.utils import save_to_json

logger = logging.getLogger(__name__)


def configure_driver() -> webdriver.Chrome:
    """Настройка и возврат драйвера Chrome"""
    chrome_options = Options()
    # Сделал без фона, ибо не видно когда тебя мвидео банит
    ##chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

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
        time.sleep(20)  # Даем время странице загрузиться


        def smooth_scroll_to_bottom():
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            while current_position < scroll_height:
                driver.execute_script(f"window.scrollTo(0, {current_position});")
                current_position += 500  # Шаг скролла (можно регулировать)
                time.sleep(0.2)  # Интервал между шагами (можно регулировать)

        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            smooth_scroll_to_bottom()
            time.sleep(2)  # Даем время для подгрузки контента
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
        driver.quit()

    return products


def extract_product_data(card) -> Dict:
    """Извлечение данных из карточки товара"""
    try:
        name = card.find('a', class_='product-title__text').text.strip()
        price = card.find('span', class_='price__main-value').text.strip()
        price_clean = ''.join(c for c in price if c.isdigit())
        product_url = card.find('a', class_='product-title__text')['href']
        full_url = f"https://www.mvideo.ru{product_url}"

        return {
            "name": name,
            "price": price_clean,
            "url": full_url,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.warning(f"Не удалось извлечь данные из карточки: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = scrape_mvideo("https://www.mvideo.ru/smartfony-i-svyaz-10/smartfony-205")
    save_to_json(data, "data/latest_scrape.json")
    print(f"Собрано {len(data)} товаров")