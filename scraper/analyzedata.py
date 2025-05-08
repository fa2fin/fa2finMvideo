import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict
import sqlite3
from scraper.database import DatabaseManager


class DataAnalyzer:
    def __init__(self, db_manager: DatabaseManager = None):
        self.db = db_manager if db_manager else DatabaseManager()

    def analyze_last_scrape(self) -> Dict:
        """
        Анализирует данные последнего сбора из базы данных
        Возвращает статистику без генерации графиков
        """
        try:
            products = self.db.get_last_scrape_data()
            if not products:
                return {}

            df = pd.DataFrame(products)
            return self._process_data(df)

        except Exception as e:
            print(f"Ошибка при анализе данных: {e}")
            return {}

    def analyze_by_scrape_id(self, scrape_id: str) -> Dict:
        """
        Анализирует данные по конкретному scrape_id
        """
        try:
            conn = sqlite3.connect(self.db.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT name, price, url, timestamp 
                FROM products 
                WHERE scrape_id = ?
            """, (scrape_id,))

            products = [dict(row) for row in cursor.fetchall()]
            conn.close()

            if not products:
                return {}

            df = pd.DataFrame(products)
            return self._process_data(df)

        except Exception as e:
            print(f"Ошибка при анализе данных: {e}")
            return {}
        finally:
            conn.close()
    def _process_data(self, df: pd.DataFrame) -> Dict:
        """Общая обработка данных без генерации графиков"""
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df = df.dropna(subset=['price'])
        df['brand'] = df['name'].str.split().str[0]

        return {
            "total_products": len(df),
            "min_price": int(df['price'].min()),
            "max_price": int(df['price'].max()),
            "avg_price": int(df['price'].mean()),
            "unique_brands": df['brand'].nunique(),
            "timestamp": datetime.now().isoformat()
        }


def analyze_all_scrapes():
    """Анализирует все имеющиеся в БД сборы данных без графиков"""
    db = DatabaseManager()
    analyzer = DataAnalyzer(db)

    try:
        conn = sqlite3.connect(db.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT scrape_id FROM products ORDER BY timestamp DESC")
        scrape_ids = [row[0] for row in cursor.fetchall()]
        conn.close()

        print(f"Найдено {len(scrape_ids)} сборов данных для анализа...")

        for scrape_id in scrape_ids:
            print(f"\nАнализирую сбор данных: {scrape_id}")

            results = analyzer.analyze_by_scrape_id(scrape_id)

            if results:
                db.save_analysis(results, scrape_id)
                print("Статистика:")
                for key, value in results.items():
                    print(f"  {key}: {value}")

    except Exception as e:
        print(f"Ошибка при анализе всех сборов данных: {e}")


if __name__ == "__main__":
    analyzer = DataAnalyzer()

    last_scrape_stats = analyzer.analyze_last_scrape()
    print("Статистика последнего сбора данных:")
    print(last_scrape_stats)

    analyze_all_scrapes()
    print("\nАнализ всех сборов данных завершен!")