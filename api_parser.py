import requests
import csv
import os
import json
import time
from urllib.parse import urlparse, urljoin
from playwright.sync_api import sync_playwright

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
def fetch_data_from_api(collection_name):
    itemsArray = collection_name.split('|')
    collection = itemsArray[0]
    handle = itemsArray[1]

    # Initialize the list to store all results
    all_results = []

    # Start with page 1
    current_page = 1
    total_pages = 1  # Will be updated after the first request

    while current_page <= total_pages:
        api_url = (
            "https://mattel-creations-searchspring-proxy.netlify.app/api/search?"
            "domain=%2Fcollections%2F"
            f"{collection}&"
            "bgfilter.collection_handle="
            f"{handle}&"
            "resultsFormat=native&"
            "resultsPerPage=999&"
            f"page={current_page}&"
            "bgfilter.ss_is_past_project=false&"
            f"ts={int(time.time() * 1000)}"  # Dynamic timestamp
        )

        try:
            print(f"Fetching page {current_page} of {total_pages} from API: {api_url}")
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()  # Raise an exception for bad status codes
            data = response.json()

            # Append results from the current page
            results = data.get('results', [])
            all_results.extend(results)
            print(f"Received {len(results)} results from page {current_page}")

            # Update total_pages from the pagination data
            pagination = data.get('pagination', {})
            total_pages = pagination.get('totalPages', 1)
            print(f"Total pages: {total_pages}")

            # Move to the next page
            current_page += 1

        except (requests.RequestException, ValueError) as e:
            print(f"Error fetching data from API for page {current_page}: {e}")
            # Optionally, continue to the next page or break based on your needs
            break

    print(f"Total results collected: {len(all_results)}")
    return all_results

# Функція для обробки даних із API
def process_api_results(results):
    data_list = []
    for item in results:
        # Фільтруємо тільки елементи з tags_category: ["Vehicles"]
        if item.get('tags_category') == ['Vehicles'] or item.get('tags_category') == ['Action Figures']:
            page_name = item.get('url', '').split('/')[-1]  # Витягуємо page_name з url
            data = {
                'car_name': item.get('name', ''),
                'SKU': item.get('sku', ''),
                'page_name': page_name,
                # 'current_qty': item.get('ss_inventory_count', None),
                'image_url': remove_url_params(item.get('imageUrl', '')),
                'price': item.get('price', ''),
                'uid': item.get('uid', '')
            }
            # Перевіряємо і конвертуємо current_qty
            # if data['current_qty'] is not None:
            #     try:
            #         data['current_qty'] = int(float(data['current_qty']))  # Дозволяє конвертацію з рядків типу "123.0"
            #     except (ValueError, TypeError):
            #         print(f"Невалідне значення ss_inventory_count для {data['car_name']}: {data['current_qty']}, встановлено None")
            #         data['current_qty'] = None
            print(f"Оброблено елемент: {data}")
            data_list.append(data)
        else:
            print(f"Пропущено елемент: tags_category != ['Vehicles'], item={item.get('name', 'Unknown')}")
    print(f"Загалом оброблено {len(data_list)} елементів із tags_category: ['Vehicles']")
    return data_list

# Функція для оновлення або додавання даних у CSV
def update_csv(data, csv_file='output.csv'):
    if not data or data['current_qty'] is None:
        print(f"Пропущено запис для {data.get('car_name', 'Unknown')}: current_qty is None")
        return

    if 'F1' in data['car_name']:
        print("")

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
                        old_qty = int(row.get('current_qty') or 0)
                        new_qty = int(data.get('current_qty') or 0)

                        # Оновлюємо current_qty мінімальним значенням
                        row['current_qty'] = str(min(old_qty, new_qty))

                        print(f"Знайдено збіг для {data['car_name']}, current_qty: {old_qty} → {row['current_qty']}")

                        # Якщо image_url або price порожні — підставляємо з data
                        row['image_url'] = row['image_url'] if row['image_url'] else data.get('image_url', '')
                        row['price'] = row['price'] if row['price'] else data.get('price', '')

                        # max_qty — оновлюємо до більшого значення
                        row['max_qty'] = str(max(int(row.get('max_qty') or 0), new_qty))

                        updated = True
                    rows.append(row)
        except (csv.Error, ValueError) as e:
            print(f"Помилка читання CSV-файлу {csv_file}: {e}")
            rows = []  # Якщо файл пошкоджений, починаємо з порожнього списку

    # Додаємо новий рядок, якщо не було оновлення
    if not updated:
        print(f"Додано новий рядок для {data['car_name']}")
        max_qty = int(data.get('max_qty') or 0)
        current_qty = int(data.get('current_qty') or 0)

        # Якщо max_qty менше, ніж current_qty — оновлюємо max_qty
        if max_qty < current_qty:
            max_qty = current_qty

        rows.append({
            'car_name': data['car_name'],
            'SKU': data['SKU'],
            'page_name': data['page_name'],
            'max_qty': str(max_qty),
            'current_qty': str(current_qty),
            'image_url': data['image_url'],
            'price': data['price']
        })

    # Записуємо оновлені дані у CSV
    try:
        print(f"Записуємо {len(rows)} рядків у {csv_file}")
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Успішно записано в {csv_file}")
    except IOError as e:
        print(f"Помилка запису в CSV-файл {csv_file}: {e}")

# Функція для обробки всіх даних із захистом від винятків
def process_data(collection_name):
    try:
        # Отримуємо дані з API
        results = fetch_data_from_api(collection_name)
        if not results:
            print("Не отримано даних з API, створюємо порожній CSV")
            # Створюємо порожній CSV із заголовком, якщо немає даних
            update_csv({'car_name': '', 'SKU': '', 'page_name': '', 'current_qty': None, 'image_url': '', 'price': ''})
            return

        # Обробляємо результати API
        data_list = process_api_results(results)

        data_updated = update_products_qty(data_list)

        # Оновлюємо CSV для кожного елемента
        for data in data_updated:
            update_csv(data)
    except Exception as e:
        print(f"Виникла помилка під час обробки даних: {e}")
        raise  # Повторно викликаємо виняток, щоб GitHub Actions зафіксував його


def get_token_with_playwright():
    browser = None
    try:
        with sync_playwright() as p:
            print("Запуск Chromium...")
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--window-size=1920,1080',
                    '--disable-blink-features=AutomationControlled'
                ]
            )

            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
                bypass_csp=True
            )

            # Приховуємо автоматизацію
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                window.chrome = { runtime: {}, app: {}, loadTimes: () => {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """)

            page = context.new_page()
            token = None

            # Перехоплюємо токен
            def intercept(route, request):
                nonlocal token
                if 'mattel-checkout-prd.fly.dev/api/product-inventory' in request.url:
                    auth = request.headers.get('authorization') or request.headers.get('Authorization')
                    if auth and auth.startswith('Bearer '):
                        token = auth
                        print(f"ТОКЕН ЗНАЙДЕНО: {auth[:70]}...")
                route.continue_()

            page.route('**/*', intercept)

            print("Перехід на checkout URL...")
            page.goto(
                'https://creations.mattel.com/checkouts/cn/hWN4eQSmROJAn1IYF6ZTjU27/en-us?auto_redirect=false&edge_redirect=true&skip_shop_pay=true',
                timeout=60000
            )

            print("Чекаємо до 20 сек на завантаження (без networkidle)...")
            page.wait_for_timeout(5000)  # Дозволяємо завантажитись

            # Чекаємо токен до 20 сек
            start = time.time()
            while not token and time.time() - start < 20:
                page.wait_for_timeout(1000)
                print(f"Чекаємо токен... ({int(time.time() - start)} сек)")

            # Повертаємо токен, навіть якщо сторінка не "networkidle"
            if token:
                print("ТОКЕН УСПІШНО ОТРИМАНО!")
                return token
            else:
                print("Токен не знайдено за 20 сек.")
                return None

    except Exception as e:
        print("Playwright помилка:", str(e))
        return None
    finally:
        # Безпечне завершення Playwright
        if browser:
            try:
                # Спробуємо прибрати маршрути перед закриттям
                try:
                    for context in browser.contexts:
                        for page in context.pages:
                            try:
                                page.unroute('**/*')
                            except Exception:
                                pass
                except Exception:
                    pass

                browser.close()
                print("Браузер закрито без помилок.")
            except Exception as e:
                # Ігноруємо типові помилки закриття
                if "TargetClosedError" in str(e) or "CancelledError" in str(e):
                    print("⚠️ Попередження: браузер уже був закритий (ігноруємо).")
                else:
                    print(f"⚠️ Помилка при закритті браузера: {e}")


def get_item_details(token, id):
    url = "https://mattel-checkout-prd.fly.dev/api/product-inventory"

    querystring = {
        "authorization": token,
        "productIds": "gid://shopify/Product/"+id}

    payload = ""
    headers = {
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "uk,en-US;q=0.9,en;q=0.8,hr;q=0.7",
        "Authorization": token,
        "Content-Type": "application/json",
        "Origin": "https://extensions.shopifycdn.com",
        "Referer": "https://extensions.shopifycdn.com/",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    }

    try:
        resp = requests.get(url, headers=headers, params=querystring, timeout=15)
    except Exception as e:
        print(f"❌ HTTP request failed for id={id}: {e}")
        return 0, 0

    if resp.status_code != 200:
        print(f"❌ Non-200 response ({resp.status_code}) for id={id}")
        return 0, 0

    try:
        data = resp.json()
    except json.JSONDecodeError:
        print(f"❌ Response is not valid JSON for id={id}")
        return 0, 0

        # Очікуємо список із одним елементом — захищено від порожнього списку
    if not data or not isinstance(data, list):
        return 0, 0

    item = data[0] if len(data) > 0 else {}
    total_inventory = item.get("totalInventory", 0)

    # Якщо variantMeta немає або воно None — повертаємо 0 для variant_qty
    variant_meta = item.get("variantMeta")
    if not variant_meta:
        return total_inventory or 0, 0

    value = variant_meta.get("value")
    if not value:
        return total_inventory or 0, 0

    # variantMeta.value може бути рядком JSON — пробуємо розпарсити
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        # Якщо не вдається розпарсити — повертаємо 0 для qty
        return total_inventory or 0, 0

    # parsed очікуємо як список з об'єктом, всередині variant_inventory
    if not parsed or not isinstance(parsed, list):
        return total_inventory or 0, 0

    vm = parsed[0] if len(parsed) > 0 else {}
    variant_inventory = vm.get("variant_inventory")
    if not variant_inventory or not isinstance(variant_inventory, list):
        return total_inventory or 0, 0

    # Шукаємо перший елемент з inventorystatus == "Available"
    for entry in variant_inventory:
        try:
            if entry.get("variant_inventorystatus") == "Available":
                qty = entry.get("variant_qty", 0) or 0
                # гарантуємо, що повертаємо int
                return int(total_inventory or 0), int(qty)
        except Exception:
            continue

    # Якщо не знайдено Available — повертаємо 0 для qty
    return int(total_inventory or 0), 0


def update_products_qty(data_list):
    token = get_token_with_playwright()
    if not token:
        print("❌ Не вдалося отримати токен.")
        return data_list

    for data in data_list:
        print(f"Updating details for - " + data['car_name'])
        item_id = data.get('uid', '')
        if item_id:
            try:
                qty, total = get_item_details(token, item_id)
                data['max_qty'] = total
                data['current_qty'] = qty
            except Exception as e:
                print(f"⚠️ Помилка при оновленні {item_id}: {e}")
                data['max_qty'] = 0
                data['current_qty'] = 0
        else:
            data['max_qty'] = 0
            data['current_qty'] = 0

    return data_list



# Основна точка входу
if __name__ == "__main__":
    collections = ['hot-wheels-collectors|hot-wheels-collectors',
                   'hot-wheels-collectors|hot-wheels-f1-collector-vehicles',
                   'matchbox-collectors|matchbox-collectors',
                   'mattel-creations|mattel-creations']
    for item in collections:
        process_data(item)
