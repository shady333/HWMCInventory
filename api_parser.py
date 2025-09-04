import requests
import csv
import os
import json
from urllib.parse import urlparse, urljoin

# Функція для видалення параметрів із URL
def remove_url_params(url):
    if not url or not isinstance(url, str):  # Перевіряємо, чи URL не None і є рядком
        return ''
    try:
        # Розбиваємо URL по ? і беремо першу частину
        return url.split('?')[0]
    except Exception as e:
        print(f"Помилка обробки URL {url}: {e}")
        return ''

# Функція для отримання даних із API
def fetch_data_from_api():
    api_url = (
        "https://mattel-creations-searchspring-proxy.netlify.app/api/search?"
        "domain=%2Fcollections%2Fhot-wheels-collectors%3F&resultsFormat=native&"
        "resultsPerPage=99&filter.ss_availability_filter=Available%20Now&"
        "bgfilter.ss_is_past_project=false&bgfilter.collection_handle=hot-wheels-collectors&"
        "ts=1756990390440"
    )
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # Перевіряємо, чи запит успішний
        data = response.json()
        return data.get('results', [])
    except (requests.RequestException, ValueError) as e:
        print(f"Помилка при отриманні даних з API: {e}")
        return []

# Функція для обробки даних із API
def process_api_results(results):
    data_list = []
    for item in results:
        # Фільтруємо тільки елементи з tags_category: ["Vehicles"]
        if item.get('tags_category') == ['Vehicles']:
            page_name = item.get('url', '').split('/')[-1]  # Витягуємо page_name з url
            data = {
                'car_name': item.get('name', ''),
                'SKU': item.get('sku', ''),
                'page_name': page_name,
                'current_qty': item.get('ss_inventory_count', None),
                'image_url': remove_url_params(item.get('imageUrl', '')),
                'price': item.get('price', '')
            }
            data_list.append(data)
    return data_list

# Функція для оновлення або додавання даних у CSV
def update_csv(data, csv_file='output.csv'):
    if not data or data['current_qty'] is None:
        return

    # Визначаємо поля CSV
    fieldnames = ['car_name', 'SKU', 'page_name', 'max_qty', 'current_qty', 'image_url', 'price']
    rows = []
    updated = False

    # Перевіряємо, чи існує CSV-файл і чи він не порожній
    file_exists = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
    if file_exists:
        try:
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Додаємо відсутні поля зі значенням за замовчуванням
                    row = {field: row.get(field, '') for field in fieldnames}
                    # Перевіряємо, чи є збіг за page_name, car_name і SKU
                    if (row['page_name'] == data['page_name'] and
                        row['car_name'] == data['car_name'] and
                        row['SKU'] == data['SKU']):
                        row['current_qty'] = str(data['current_qty'])
                        # Зберігаємо попередні значення image_url і price, якщо вони є
                        row['image_url'] = row['image_url'] if row['image_url'] else data['image_url']
                        row['price'] = row['price'] if row['price'] else data['price']
                        row['max_qty'] = str(max(int(row['max_qty'] or 0), data['current_qty']))  # Оновлюємо max_qty
                        updated = True
                    rows.append(row)
        except (csv.Error, ValueError) as e:
            print(f"Помилка читання CSV-файлу {csv_file}: {e}")
            rows = []  # Якщо файл пошкоджений, починаємо з порожнього списку

    # Додаємо новий рядок, якщо не було оновлення
    if not updated:
        rows.append({
            'car_name': data['car_name'],
            'SKU': data['SKU'],
            'page_name': data['page_name'],
            'max_qty': str(data['current_qty']),  # max_qty = current_qty для нового рядка
            'current_qty': str(data['current_qty']),
            'image_url': data['image_url'],
            'price': data['price']
        })

    # Записуємо оновлені дані у CSV
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    except IOError as e:
        print(f"Помилка запису в CSV-файл {csv_file}: {e}")

# Функція для обробки всіх даних
def process_data():
    # Отримуємо дані з API
    results = fetch_data_from_api()
    if not results:
        print("Не отримано даних з API")
        return

    # Обробляємо результати API
    data_list = process_api_results(results)

    # Оновлюємо CSV для кожного елемента
    for data in data_list:
        update_csv(data)

# Основна точка входу
if __name__ == "__main__":
    process_data()