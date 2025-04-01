import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict



def save_to_json(data: List[Dict], filename: str) -> None:
    """
    Сохраняет данные в JSON файл
    :param data: Список словарей с данными о товарах
    :param filename: Имя файла для сохранения
    """
    try:

        Path("data").mkdir(exist_ok=True)


        filepath = Path("data") / filename


        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False, default=json_serializer)

        print(f"Данные успешно сохранены в {filepath}")
    except Exception as e:
        print(f"Ошибка при сохранении в JSON: {e}")
        raise