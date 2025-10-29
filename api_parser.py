import requests
import csv
import os
import json
import time
from playwright.sync_api import sync_playwright

def remove_url_params(url):
    if not url or not isinstance(url, str):
        return ''
    try:
        return url.split('?')[0]
    except Exception as e:
        print(f"Помилка обробки URL {url}: {e}")
        return ''

def fetch_data_from_api(collection_name):
    itemsArray = collection_name.split('|')
    collection = itemsArray[0]
    handle = itemsArray[1]

    all_results = []

    current_page = 1
    total_pages = 1

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
            response.raise_for_status()
            data = response.json()

            results = data.get('results', [])
            all_results.extend(results)
            print(f"Received {len(results)} results from page {current_page}")

            pagination = data.get('pagination', {})
            total_pages = pagination.get('totalPages', 1)
            print(f"Total pages: {total_pages}")

            current_page += 1

        except (requests.RequestException, ValueError) as e:
            print(f"Error fetching data from API for page {current_page}: {e}")
            break

    print(f"Total results collected: {len(all_results)}")
    return all_results

def process_api_results(results):
    data_list = []
    for item in results:
        if item.get('tags_category') == ['Vehicles'] or item.get('tags_category') == ['Action Figures']:
            page_name = item.get('url', '').split('/')[-1]
            data = {
                'car_name': item.get('name', ''),
                'SKU': item.get('sku', ''),
                'page_name': page_name,
                'image_url': remove_url_params(item.get('imageUrl', '')),
                'price': item.get('price', ''),
                'uid': item.get('uid', '')
            }
            print(f"Оброблено елемент: {data}")
            data_list.append(data)
        else:
            print(f"Пропущено елемент: tags_category != ['Vehicles'], item={item.get('name', 'Unknown')}")
    print(f"Загалом оброблено {len(data_list)} елементів із tags_category: ['Vehicles']")
    return data_list

def update_csv(data, csv_file='output.csv'):
    if not data or data['current_qty'] is None:
        print(f"Пропущено запис для {data.get('car_name', 'Unknown')}: current_qty is None")
        return

    if 'F1' in data['car_name']:
        print("")

    fieldnames = ['car_name', 'SKU', 'page_name', 'max_qty', 'current_qty', 'image_url', 'price']
    rows = []
    updated = False

    file_exists = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
    if file_exists:
        try:
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row = {field: row.get(field, '') for field in fieldnames}

                    if (row['page_name'] == data['page_name'] and
                            row['car_name'] == data['car_name'] and
                            row['SKU'] == data['SKU']):
                        old_qty = int(row.get('current_qty') or 0)
                        new_qty = int(data.get('current_qty') or 0)

                        row['current_qty'] = str(min(old_qty, new_qty))

                        print(f"Знайдено збіг для {data['car_name']}, current_qty: {old_qty} → {row['current_qty']}")

                        row['image_url'] = row['image_url'] if row['image_url'] else data.get('image_url', '')
                        row['price'] = row['price'] if row['price'] else data.get('price', '')

                        row['max_qty'] = str(max(int(row.get('max_qty') or 0), new_qty))

                        updated = True
                    rows.append(row)
        except (csv.Error, ValueError) as e:
            print(f"Помилка читання CSV-файлу {csv_file}: {e}")
            rows = []

    if not updated:
        print(f"Додано новий рядок для {data['car_name']}")
        max_qty = int(data.get('max_qty') or 0)
        current_qty = int(data.get('current_qty') or 0)

        rows.append({
            'car_name': data['car_name'],
            'SKU': data['SKU'],
            'page_name': data['page_name'],
            'max_qty': str(max_qty),
            'current_qty': str(current_qty),
            'image_url': data['image_url'],
            'price': data['price']
        })

    try:
        print(f"Записуємо {len(rows)} рядків у {csv_file}")
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Успішно записано в {csv_file}")
    except IOError as e:
        print(f"Помилка запису в CSV-файл {csv_file}: {e}")

def process_data(collection_name):
    try:
        results = fetch_data_from_api(collection_name)
        if not results:
            print("Не отримано даних з API, створюємо порожній CSV")
            update_csv({'car_name': '', 'SKU': '', 'page_name': '', 'current_qty': None, 'image_url': '', 'price': ''})
            return

        data_list = process_api_results(results)

        data_updated = update_products_qty(data_list)

        for data in data_updated:
            update_csv(data)
    except Exception as e:
        print(f"Виникла помилка під час обробки даних: {e}")
        raise


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


def get_item_details(token, product_id):

    url = "https://mattel-checkout-prd.fly.dev/api/product-inventory"

    querystring = {
        "productIds": f"gid://shopify/Product/{product_id}"
    }

    headers = {
        "Authorization": token,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        "Origin": "https://extensions.shopifycdn.com",
        "Referer": "https://extensions.shopifycdn.com/",
    }

    try:
        resp = requests.get(url, headers=headers, params=querystring, timeout=15)
    except Exception as e:
        print(f"HTTP request failed for id={product_id}: {e}")
        return 0, 0

    if resp.status_code != 200:
        print(f"Non-200 response ({resp.status_code}) for id={product_id}")
        return 0, 0

    try:
        data = resp.json()
    except json.JSONDecodeError:
        print(f"Response is not valid JSON for id={product_id}")
        return 0, 0

    if not data or not isinstance(data, list) or len(data) == 0:
        return 0, 0

    item = data[0]
    total_inventory = item.get("totalInventory", 0) or 0

    # === Обробка variantMeta ===
    variant_meta = item.get("variantMeta")
    if not variant_meta or not variant_meta.get("value"):
        return int(total_inventory), 0

    value = variant_meta["value"]

    # Спроба розпарсити JSON
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return int(total_inventory), 0

    if not parsed or not isinstance(parsed, list) or len(parsed) == 0:
        return int(total_inventory), 0

    first_variant = parsed[0]
    variant_inventory = first_variant.get("variant_inventory", [])

    if not variant_inventory or not isinstance(variant_inventory, list):
        return int(total_inventory), 0

    # === ЛОГІКА ВИБОРУ qty ===
    available_qty = None
    backordered_qty = None

    for entry in variant_inventory:
        status = entry.get("variant_inventorystatus")
        qty = entry.get("variant_qty", 0) or 0

        if status == "Available":
            available_qty = qty
        elif status == "Backordered" and backordered_qty is None:
            backordered_qty = qty  # беремо перший Backordered

    # 1. Якщо є "Available" → повертаємо його
    if available_qty is not None:
        return int(total_inventory), int(available_qty)

    # 2. Якщо немає "Available", але є "Backordered" → повертаємо його qty
    if backordered_qty is not None:
        return int(total_inventory), int(backordered_qty)

    # 3. Якщо нічого немає → повертаємо 0
    return int(total_inventory), 0


def update_products_qty(data_list, token):
    if not token:
        print("❌ Токен не отримано, пропускаємо оновлення.")
        return data_list

    for data in data_list:
        print(f"Updating details for - {data['car_name']}")
        item_id = data.get('uid', '')

        if not item_id:
            data['max_qty'] = 0
            data['current_qty'] = 0
            continue

        try:
            qty, total = get_item_details(token, item_id)

            # Якщо повертає 0, 0 — не обов'язково помилка, може бути порожній stock
            data['max_qty'] = total
            data['current_qty'] = qty

        except requests.exceptions.HTTPError as e:
            # Якщо 401 — пробуємо отримати новий токен
            if e.response.status_code == 401:
                print("⚠️ Токен недійсний, пробуємо оновити...")
                token = get_token_with_playwright()
                if token:
                    try:
                        qty, total = get_item_details(token, item_id)
                        data['max_qty'] = total
                        data['current_qty'] = qty
                    except Exception as e:
                        print(f"❌ Не вдалося повторно оновити {item_id}: {e}")
                        data['max_qty'] = 0
                        data['current_qty'] = 0
                else:
                    print("❌ Не вдалося оновити токен.")
                    data['max_qty'] = 0
                    data['current_qty'] = 0
            else:
                print(f"❌ HTTP помилка для {item_id}: {e}")
                data['max_qty'] = 0
                data['current_qty'] = 0
        except Exception as e:
            print(f"⚠️ Помилка при оновленні {item_id}: {e}")
            data['max_qty'] = 0
            data['current_qty'] = 0

    return data_list


if __name__ == "__main__":
    collections = ['hot-wheels-collectors|hot-wheels-collectors',
                   'hot-wheels-collectors|hot-wheels-f1-collector-vehicles',
                   'matchbox-collectors|matchbox-collectors',
                   'mattel-creations|mattel-creations']

    token = get_token_with_playwright()

    for item in collections:
        try:
            results = fetch_data_from_api(item)
            data_list = process_api_results(results)

            # Передаємо поточний токен у функцію
            updated_data = update_products_qty(data_list, token)

            # Оновлюємо CSV
            for data in updated_data:
                update_csv(data)
        except Exception as e:
            print(f"❌ Помилка при обробці колекції {item}: {e}")
