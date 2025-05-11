import json

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from scraper.database import DatabaseManager
import sqlite3
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
app = Flask(__name__)
CORS(app)
db = DatabaseManager()  # Используем существующий DatabaseManager


@app.route('/api/products', methods=['GET'])
def get_products():
    try:
        # Валидация параметров
        page = max(int(request.args.get('page', 1)), 1)
        per_page = min(int(request.args.get('per_page', 10)), 100)

        # Фильтры
        filters = {
            'min_price': request.args.get('min_price'),
            'max_price': request.args.get('max_price'),
            'brand': request.args.get('brand')
        }

        # Безопасный SQL-запрос через DatabaseManager
        with db.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Динамическое построение запроса
            query = "SELECT * FROM products WHERE 1=1"
            params = []
            if filters['min_price']:
                query += " AND price >= ?"
                params.append(int(filters['min_price']))
            if filters['max_price']:
                query += " AND price <= ?"
                params.append(int(filters['max_price']))
            if filters['brand']:
                query += " AND brand = ?"
                params.append(filters['brand'])

            # Пагинация
            query += " LIMIT ? OFFSET ?"
            params.extend([per_page, (page - 1) * per_page])

            cursor.execute(query, params)
            products = [dict(row) for row in cursor.fetchall()]

        return Response(
            json.dumps({"data": products, "page": page}, ensure_ascii=False),
            mimetype='application/json; charset=utf-8'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 400



if __name__ == '__main__':
    app.run(port=5000, debug=True)  # Убедитесь, что эта строка присутствует