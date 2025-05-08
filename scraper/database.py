import sqlite3
from datetime import datetime
from typing import List, Dict
from pathlib import Path
import logging
import time
from contextlib import contextmanager
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_path: str = "data/mvideo_monitoring.db"):
        self.db_path = db_path
        self.max_retries = 10
        self.retry_delay = 0.5
        self._init_db()

    def _init_db(self):
        """Инициализация базы данных и таблиц"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Таблица для хранения продуктов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    brand TEXT,
                    timestamp DATETIME NOT NULL,
                    scrape_id TEXT NOT NULL
                )
            """)
            # Таблица для хранения результатов анализа
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analysis_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scrape_id TEXT NOT NULL,
                    total_products INTEGER NOT NULL,
                    min_price INTEGER NOT NULL,
                    max_price INTEGER NOT NULL,
                    unique_brands INTEGER NOT NULL,
                    plot_path TEXT,
                    timestamp DATETIME NOT NULL
                )
            """)
            conn.commit()

    @contextmanager
    def get_connection(self):
        """Контекстный менеджер с обработкой блокировок"""
        conn = None
        for attempt in range(self.max_retries):
            try:
                conn = sqlite3.connect(
                    self.db_path,
                    timeout=30,
                    check_same_thread=False
                )
                conn.execute("PRAGMA journal_mode=WAL")  # Включите WAL-режим
                conn.execute("PRAGMA busy_timeout=30000")  # 10 секунд ожидания
                yield conn
                break
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                raise
            finally:
                if conn:
                    conn.close()

    def save_products(self, products: List[Dict], scrape_id: str) -> None:
        """Сохраняет список продуктов в базу данных"""
        if not products:
            logger.warning("Пустой список продуктов для сохранения")
            return
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for product in products:
                cursor.execute("""
                    INSERT INTO products (name, price, url, brand, timestamp, scrape_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    product.get("name"),
                    product.get("price"),
                    product.get("url"),
                    self._extract_brand(product.get("name")),
                    product.get("timestamp"),
                    scrape_id
                ))
            conn.commit()
            cursor.close()

    def save_analysis(self, analysis: Dict, scrape_id: str) -> None:
        """Сохраняет результаты анализа в базу данных"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO analysis_results (
                    scrape_id, total_products, min_price, max_price, 
                    unique_brands, plot_path, timestamp
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                scrape_id,
                analysis.get("total_products", 0),
                analysis.get("min_price", 0),
                analysis.get("max_price", 0),
                analysis.get("unique_brands", 0),
                analysis.get("plot_path"),
                analysis.get("timestamp", datetime.now().isoformat())
            ))
            conn.commit()
        logger.info(f"Сохранены результаты анализа (scrape_id: {scrape_id})")

    def _extract_brand(self, product_name: str) -> str:
        """Извлекает бренд из названия продукта (простая реализация)"""
        if not product_name:
            return None
        # Простая логика извлечения бренда - первое слово в названии
        return product_name.split()[0] if product_name else None

    def get_last_scrape_data(self) -> List[Dict]:
        """Получает данные последнего сбора"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # Получаем последний scrape_id
            cursor.execute("""
                SELECT scrape_id FROM products 
                ORDER BY timestamp DESC LIMIT 1
            """)
            last_scrape = cursor.fetchone()
            if not last_scrape:
                return []

            # Получаем все продукты для этого scrape_id
            cursor.execute("""
                SELECT name, price, url, timestamp 
                FROM products 
                WHERE scrape_id = ?
            """, (last_scrape["scrape_id"],))
            return [dict(row) for row in cursor.fetchall()]