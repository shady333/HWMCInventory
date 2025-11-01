import requests
import csv
import os
import json
import time
from typing import List, Dict, Tuple, Optional
from playwright.sync_api import sync_playwright, Browser
from dataclasses import dataclass, asdict
from functools import wraps

# Константи
API_BASE_URL = "https://mattel-creations-searchspring-proxy.netlify.app/api/search"
INVENTORY_API_URL = "https://mattel-checkout-prd.fly.dev/api/product-inventory"
CHECKOUT_URL = "https://creations.mattel.com/checkouts/cn/hWN4eQSmROJAn1IYF6ZTjU27/en-us?auto_redirect=false&edge_redirect=true&skip_shop_pay=true"

CSV_FIELDNAMES = ['car_name', 'SKU', 'page_name', 'max_qty', 'current_qty', 'image_url', 'price']
TARGET_CATEGORIES = [['Vehicles'], ['Action Figures']]

COLLECTIONS = [
    'hot-wheels-collectors|hot-wheels-collectors',
    'hot-wheels-collectors|hot-wheels-f1-collector-vehicles',
    'matchbox-collectors|matchbox-collectors',
    'mattel-creations|mattel-creations'
]

# Налаштування retry
MAX_RETRIES = 3
RETRY_DELAY = 2  # секунд
TOKEN_WAIT_TIMEOUT = 20  # секунд


@dataclass
class Product:
    """Клас для представлення продукту."""
    car_name: str
    SKU: str
    page_name: str
    image_url: str
    price: str
    uid: str
    max_qty: int = 0
    current_qty: int = 0

    def to_csv_dict(self) -> Dict:
        """Конвертує продукт у словник для CSV."""
        return {
            'car_name': self.car_name,
            'SKU': self.SKU,
            'page_name': self.page_name,
            'max_qty': str(self.max_qty),
            'current_qty': str(self.current_qty),
            'image_url': self.image_url,
            'price': self.price
        }

    def matches(self, other: 'Product') -> bool:
        """Перевіряє чи продукти співпадають."""
        return (self.page_name == other.page_name and
                self.car_name == other.car_name and
                self.SKU == other.SKU)


def retry_on_failure(max_attempts: int = MAX_RETRIES, delay: int = RETRY_DELAY):
    """Декоратор для повторення запитів при помилках."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    if attempt == max_attempts:
                        print(f"❌ Всі {max_attempts} спроби невдалі: {e}")
                        raise
                    print(f"⚠️ Спроба {attempt}/{max_attempts} невдала, повтор через {delay}с...")
                    time.sleep(delay)
            return None

        return wrapper

    return decorator


def remove_url_params(url: str) -> str:
    """Видаляє параметри з URL."""
    if not url or not isinstance(url, str):
        return ''
    return url.split('?')[0]


@retry_on_failure(max_attempts=2)
def fetch_data_from_api(collection_name: str) -> List[Dict]:
    """Отримує всі дані з API для заданої колекції."""
    collection, handle = collection_name.split('|')
    all_results = []
    current_page = 1

    # Спочатку отримуємо першу сторінку, щоб дізнатися total_pages
    while True:
        params = {
            "domain": f"/collections/{collection}",
            "bgfilter.collection_handle": handle,
            "resultsFormat": "native",
            "resultsPerPage": "999",
            "page": str(current_page),
            "bgfilter.ss_is_past_project": "false",
            "ts": str(int(time.time() * 1000))
        }

        response = requests.get(API_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        results = data.get('results', [])
        if not results:
            break

        all_results.extend(results)
        print(f"📥 Сторінка {current_page}: отримано {len(results)} елементів")

        # Перевіряємо чи є ще сторінки
        pagination = data.get('pagination', {})
        total_pages = pagination.get('totalPages', current_page)

        if current_page >= total_pages:
            break

        current_page += 1

    print(f"✅ Всього отримано {len(all_results)} елементів з '{collection}'")
    return all_results


def process_api_results(results: List[Dict]) -> List[Product]:
    """Фільтрує та обробляє результати API."""
    products = []

    for item in results:
        category = item.get('tags_category', [])

        # Перевіряємо чи категорія в списку дозволених
        if category in TARGET_CATEGORIES:
            products.append(Product(
                car_name=item.get('name', ''),
                SKU=item.get('sku', ''),
                page_name=item.get('url', '').split('/')[-1],
                image_url=remove_url_params(item.get('imageUrl', '')),
                price=item.get('price', ''),
                uid=item.get('uid', '')
            ))

    print(f"🔍 Відфільтровано {len(products)} продуктів")
    return products


class TokenManager:
    """Клас для управління токеном авторизації."""

    def __init__(self):
        self.token: Optional[str] = None
        self.token_obtained_at: Optional[float] = None
        self.token_lifetime = 240  # 4 хвилини (токен живе ~5 хв, беремо з запасом)
        self.refresh_attempts = 0
        self.max_refresh_attempts = 3

    def is_token_valid(self) -> bool:
        """Перевіряє чи токен ще дійсний."""
        if not self.token or not self.token_obtained_at:
            return False
        elapsed = time.time() - self.token_obtained_at
        return elapsed < self.token_lifetime

    def get_token(self, force_refresh: bool = False) -> Optional[str]:
        """Отримує токен (з кешу або новий)."""
        if not force_refresh and self.is_token_valid():
            elapsed = int(time.time() - self.token_obtained_at)
            remaining = self.token_lifetime - elapsed
            print(f"♻️ Використовуємо кешований токен (залишилось ~{remaining}с)")
            return self.token

        # Перевірка на занадто часті оновлення
        if self.refresh_attempts >= self.max_refresh_attempts:
            print(f"❌ Перевищено ліміт оновлень токена ({self.max_refresh_attempts})")
            return None

        self.refresh_attempts += 1
        print(f"🌐 Отримуємо новий токен (спроба {self.refresh_attempts}/{self.max_refresh_attempts})...")

        self.token = self._fetch_token_with_playwright()
        if self.token:
            self.token_obtained_at = time.time()
            self.refresh_attempts = 0  # Скидаємо лічильник після успіху
            print(f"✅ Токен отримано (дійсний ~{self.token_lifetime}с)")
        else:
            print("❌ Не вдалося отримати токен")

        return self.token

    def _fetch_token_with_playwright(self) -> Optional[str]:
        """Отримує токен авторизації через Playwright."""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--disable-blink-features=AutomationControlled'
                    ]
                )

                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    bypass_csp=True
                )

                context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', { get: () => false });
                    window.chrome = { runtime: {}, app: {}, loadTimes: () => {} };
                """)

                page = context.new_page()
                token = None

                def intercept(route, request):
                    nonlocal token
                    if 'mattel-checkout-prd.fly.dev/api/product-inventory' in request.url:
                        auth = request.headers.get('authorization') or request.headers.get('Authorization')
                        if auth and auth.startswith('Bearer '):
                            token = auth
                            print(f"✅ Токен отримано")
                    route.continue_()

                page.route('**/*', intercept)
                page.goto(CHECKOUT_URL, timeout=60000)

                # Чекаємо токен
                start_time = time.time()
                while not token and (time.time() - start_time) < TOKEN_WAIT_TIMEOUT:
                    page.wait_for_timeout(1000)

                browser.close()
                return token

        except Exception as e:
            print(f"❌ Помилка Playwright: {e}")
            return None


def get_item_inventory(token: str, product_id: str) -> Tuple[int, int, bool]:
    """
    Отримує інформацію про інвентар продукту.

    Returns:
        (max_qty, current_qty, success)
        max_qty - максимальна кількість з variant_inventory (що була відома)
        current_qty - totalInventory (скільки залишилось зараз, може бути від'ємним)
    """
    headers = {
        "Authorization": token,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Origin": "https://extensions.shopifycdn.com",
        "Referer": "https://extensions.shopifycdn.com/",
    }

    params = {"productIds": f"gid://shopify/Product/{product_id}"}

    try:
        resp = requests.get(INVENTORY_API_URL, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as e:
        # Пробрасываем HTTPError наверх для обработки 401
        raise
    except Exception as e:
        print(f"⚠️ Помилка запиту для {product_id}: {e}")
        return 0, 0, False

    if not data or not isinstance(data, list) or len(data) == 0:
        return 0, 0, True  # Порожній інвентар - це не помилка

    item = data[0]
    # totalInventory = скільки залишилось (може бути від'ємним)
    total_inventory = item.get("totalInventory", 0) or 0

    # Обробка variantMeta для отримання max_qty
    variant_meta = item.get("variantMeta")
    if not variant_meta or not variant_meta.get("value"):
        # Якщо немає варіантів, max_qty невідомий (0)
        return 0, int(total_inventory), True

    try:
        parsed = json.loads(variant_meta["value"])
        if not parsed or not isinstance(parsed, list):
            return 0, int(total_inventory), True

        variant_inventory = parsed[0].get("variant_inventory", [])
    except (TypeError, json.JSONDecodeError, IndexError, KeyError):
        return 0, int(total_inventory), True

    # Шукаємо максимальну кількість з варіантів
    # Пріоритет: Available → Backordered
    max_qty = 0

    for entry in variant_inventory:
        if entry.get("variant_inventorystatus") == "Available":
            qty = entry.get("variant_qty", 0) or 0
            max_qty = int(qty)
            break  # Available має найвищий пріоритет

    # Якщо немає Available, шукаємо Backordered
    if max_qty == 0:
        for entry in variant_inventory:
            if entry.get("variant_inventorystatus") == "Backordered":
                qty = entry.get("variant_qty", 0) or 0
                max_qty = int(qty)
                break

    # Повертаємо (max_qty з варіантів, current_qty з totalInventory)
    return max_qty, int(total_inventory), True


def update_products_qty(products: List[Product], token_manager: TokenManager) -> List[Product]:
    """Оновлює кількість для кожного продукту."""
    token = token_manager.get_token()
    if not token:
        print("❌ Токен відсутній, пропускаємо оновлення")
        return products

    failed_products = []  # Продукти, які не вдалося оновити

    for i, product in enumerate(products, 1):
        if not product.uid:
            print(f"⏭️ [{i}/{len(products)}] Пропущено {product.car_name[:40]}: немає UID")
            continue

        print(f"🔄 [{i}/{len(products)}] {product.car_name[:50]}...")

        # Перевіряємо токен перед кожним запитом (якщо минуло багато часу)
        if not token_manager.is_token_valid():
            print("⏰ Токен застарів, оновлюємо превентивно...")
            token = token_manager.get_token(force_refresh=True)
            if not token:
                print("❌ Не вдалося оновити токен, припиняємо обробку")
                break

        try:
            max_qty, current_qty, success = get_item_inventory(token, product.uid)

            if success:
                product.max_qty = max_qty
                product.current_qty = current_qty

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("🔄 401 Unauthorized - токен недійсний, оновлюємо...")
                token = token_manager.get_token(force_refresh=True)

                if token:
                    try:
                        max_qty, current_qty, success = get_item_inventory(token, product.uid)
                        if success:
                            product.max_qty = max_qty
                            product.current_qty = current_qty
                        else:
                            failed_products.append(product.car_name)
                    except Exception as retry_e:
                        print(f"❌ Повторна спроба невдала: {retry_e}")
                        failed_products.append(product.car_name)
                else:
                    print("❌ Не вдалося оновити токен, припиняємо")
                    failed_products.extend([p.car_name for p in products[i - 1:]])
                    break
            else:
                print(f"❌ HTTP {e.response.status_code} для {product.uid}")
                failed_products.append(product.car_name)

        except Exception as e:
            print(f"⚠️ Несподівана помилка для {product.uid}: {e}")
            failed_products.append(product.car_name)

    if failed_products:
        print(f"\n⚠️ Не вдалося оновити {len(failed_products)} продуктів:")
        for name in failed_products[:5]:  # Показуємо перші 5
            print(f"  - {name[:60]}")
        if len(failed_products) > 5:
            print(f"  ... та ще {len(failed_products) - 5}")

    return products


class CSVManager:
    """Клас для роботи з CSV файлом."""

    def __init__(self, csv_file: str = 'output.csv'):
        self.csv_file = csv_file
        self._cache: Optional[List[Product]] = None

    def remove_duplicates(self) -> int:
        """Видаляє дублікати з CSV файлу. Повертає кількість видалених дублікатів."""
        products = self._load_cache()
        original_count = len(products)

        # Використовуємо словник для відстеження унікальних продуктів
        # Ключ: (page_name, car_name, SKU)
        unique_products = {}

        for product in products:
            key = (product.page_name, product.car_name, product.SKU)

            if key in unique_products:
                existing = unique_products[key]

                # При дублікатах:
                # - current_qty: беремо МЕНШЕ (товари розкуповують), але мінімум 0
                # - max_qty: беремо БІЛЬШЕ (максимальна кількість, що була)
                # - image_url, price: беремо непорожні

                merged_current_qty = min(existing.current_qty, product.current_qty)
                # Якщо кількість від'ємна - ставимо 0
                if merged_current_qty < 0:
                    merged_current_qty = 0

                merged = Product(
                    car_name=product.car_name,
                    SKU=product.SKU,
                    page_name=product.page_name,
                    image_url=existing.image_url or product.image_url,
                    price=existing.price or product.price,
                    uid=product.uid or existing.uid,
                    max_qty=max(existing.max_qty, product.max_qty),
                    current_qty=merged_current_qty
                )

                unique_products[key] = merged
            else:
                # Нормалізуємо й для нових продуктів
                if product.current_qty < 0:
                    product.current_qty = 0
                unique_products[key] = product

        # Оновлюємо кеш унікальними продуктами
        self._cache = list(unique_products.values())
        duplicates_removed = original_count - len(self._cache)

        if duplicates_removed > 0:
            print(f"🧹 Видалено {duplicates_removed} дублікатів ({original_count} → {len(self._cache)})")

        return duplicates_removed

    def _load_cache(self) -> List[Product]:
        """Завантажує існуючі дані з CSV у пам'ять."""
        if self._cache is not None:
            return self._cache

        products = []

        if os.path.exists(self.csv_file) and os.path.getsize(self.csv_file) > 0:
            try:
                with open(self.csv_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            max_qty = int(row.get('max_qty', 0) or 0)
                            current_qty = int(row.get('current_qty', 0) or 0)

                            # Нормалізація від'ємних значень
                            # max_qty може бути від'ємним (totalInventory від API)
                            # current_qty теж може бути від'ємним, але ми його обнуляємо
                            if current_qty < 0:
                                current_qty = 0

                            products.append(Product(
                                car_name=row.get('car_name', ''),
                                SKU=row.get('SKU', ''),
                                page_name=row.get('page_name', ''),
                                image_url=row.get('image_url', ''),
                                price=row.get('price', ''),
                                uid='',  # UID не зберігається в CSV
                                max_qty=max_qty,
                                current_qty=current_qty
                            ))
                        except (ValueError, TypeError) as e:
                            print(f"⚠️ Помилка парсингу рядка CSV: {e}, рядок: {row}")
                            continue
            except Exception as e:
                print(f"⚠️ Помилка читання CSV: {e}")

        self._cache = products
        return products

    def update_or_add(self, new_product: Product) -> None:
        """Оновлює існуючий продукт або додає новий."""
        if new_product.current_qty is None:
            return

        existing_products = self._load_cache()
        found = False

        for existing in existing_products:
            if existing.matches(new_product):
                old_qty = existing.current_qty
                old_max = existing.max_qty

                # Оновлюємо дані
                existing.current_qty = new_product.current_qty
                existing.max_qty = new_product.max_qty

                # Оновлюємо тільки якщо порожні
                if not existing.image_url:
                    existing.image_url = new_product.image_url
                if not existing.price:
                    existing.price = new_product.price

                # Логуємо тільки реальні зміни
                if old_qty != new_product.current_qty or old_max != new_product.max_qty:
                    print(
                        f"📝 Оновлено {new_product.car_name[:40]}: qty {old_qty}→{new_product.current_qty}, max {old_max}→{new_product.max_qty}")

                found = True
                break

        if not found:
            print(f"➕ Новий: {new_product.car_name[:40]}")
            existing_products.append(new_product)

    def save(self) -> None:
        """Зберігає всі дані в CSV файл."""
        if self._cache is None:
            return

        try:
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                writer.writeheader()
                writer.writerows([p.to_csv_dict() for p in self._cache])
            print(f"💾 Збережено {len(self._cache)} записів у {self.csv_file}")
        except IOError as e:
            print(f"❌ Помилка запису в CSV: {e}")


def main():
    """Основна функція обробки всіх колекцій."""
    print("🚀 Початок обробки колекцій Mattel\n")

    token_manager = TokenManager()
    csv_manager = CSVManager()

    # Очищаємо дублікати перед початком
    csv_manager.remove_duplicates()
    csv_manager.save()

    for collection in COLLECTIONS:
        print(f"\n{'=' * 60}")
        print(f"📦 Колекція: {collection}")
        print('=' * 60)

        try:
            # Отримання даних з API
            results = fetch_data_from_api(collection)
            if not results:
                print("⚠️ Немає результатів, пропускаємо колекцію")
                continue

            # Обробка результатів
            products = process_api_results(results)
            if not products:
                print("⚠️ Немає продуктів після фільтрації")
                continue

            # Оновлення кількості
            products = update_products_qty(products, token_manager)

            # Збереження в CSV (batch update)
            for product in products:
                csv_manager.update_or_add(product)

            csv_manager.save()
            print(f"✅ Колекція оброблена")

        except Exception as e:
            print(f"❌ Помилка: {e}")
            continue

    print("\n🎉 Обробка завершена!")


if __name__ == "__main__":
    main()