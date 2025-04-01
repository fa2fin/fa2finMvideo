import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import pandas as pd
import matplotlib.pyplot as plt


def analyze_data(data: List[Dict]) -> Dict:
    """
    Анализирует данные и генерирует статистику
    """
    if not data:
        return {}

    try:
        df = pd.DataFrame(data)

        stats = {
            "total_products": len(df),
            "min_price": int(df['price'].min()),
            "max_price": int(df['price'].max()),
            "unique_brands": df['brand'].nunique() if 'brand' in df.columns else 0,
            "timestamp": datetime.now().isoformat()
        }

        if len(df) > 1:
            plt.figure(figsize=(12, 6))
            df['price'].hist(bins=20)
            plt.title('Распределение цен на смартфоны')
            plt.xlabel('Цена (руб)')
            plt.ylabel('Количество')
            plot_path = Path("data/results") / f"price_distribution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.savefig(plot_path)
            plt.close()
            stats['plot_path'] = str(plot_path)

        return stats

    except Exception as e:
        print(f"Ошибка при анализе данных: {e}")
        return {}


def process_all_data_files():
    Path("data/results").mkdir(parents=True, exist_ok=True)


    data_files = list(Path("data/data").glob("*.json"))

    if not data_files:
        print("Не найдено JSON-файлов в директории /data/data")
        return

    print(f"Найдено {len(data_files)} файлов для анализа...")

    for file_path in data_files:
        print(f"\nАнализирую файл: {file_path.name}")

        try:

            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)


            results = analyze_data(data)


            result_file = Path("data/results") / f"analysis_{file_path.stem}.json"
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=4, ensure_ascii=False)


            print(f"Результаты сохранены в: {result_file}")
            print("Основная статистика:")
            for key, value in results.items():
                if key != 'plot_path':
                    print(f"  {key}: {value}")
            if 'plot_path' in results:
                print(f"  График: {results['plot_path']}")

        except Exception as e:
            print(f"Ошибка обработки файла {file_path.name}: {e}")
            continue


if __name__ == "__main__":
    process_all_data_files()
    print("\nАнализ всех файлов завершен!")