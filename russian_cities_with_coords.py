"""
Скрипт для получения списка городов России с Википедии, их фильтрации и последующего 
геокодирования (добавления координат широты и долготы).

Что делает скрипт:
1. Загружает список городов России с Википедии.
2. Очищает и форматирует данные (названия, регионы, население).
3. Выделяет непризнанные города (с пометкой "не призн.") в отдельный CSV-файл
   (геокодирование непризнанных городов в рамках этого скрипта не проводится).
4. Геокодирует оставшиеся города через OpenStreetMap (Nominatim).
5. Кэширует координаты для избежания повторных запросов.
6. Сохраняет результат в файл с координатами.

Требования:
  - Python 3.10+

Зависимости:
  - pandas requests tqdm geopy

Файлы:
- `russian_cities.csv` — очищенные и признанные города России.
- `unrecognized_cities.csv` — города с пометкой "не призн.".
- `cities_with_coordinates.csv` — города с координатами.
- `geo_cache.json` — кэш координат, чтобы не перегружать API.

⚠ Требует интернет-соединения и может занять время (около 20 мин), особенно при первом запуске.

⚙ Использование:
    python russian_cities_with_coords.py
"""

# Импорты и основной код ниже


import os
import time
import json
import requests
import pandas as pd
from io import StringIO
from tqdm import tqdm
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

def fetch_and_geocode_russian_cities(csv_path='russian_cities.csv',
                                      unrecognized_path='unrecognized_cities.csv',
                                      output_path='cities_with_coordinates.csv',
                                      cache_file='geo_cache.json'):
    # --- Шаг 1: Получение и обработка списка городов ---
    print("Получаем список городов России с Википедии...")
    api_url = "https://ru.wikipedia.org/w/api.php"
    params = {
        'action': 'parse',
        'page': 'Список городов России',
        'format': 'json',
        'prop': 'text',
        'section': 1
    }

    response = requests.get(api_url, params=params)
    data = response.json()
    html = data['parse']['text']['*']
    dfs = pd.read_html(StringIO(html))
    df = dfs[0]

    df = df.rename(columns={
        'Город': 'city',
        'Регион': 'region',
        'Население': 'population'
    })
    df = df[['city', 'region', 'population']]
    df['population'] = df['population'].astype(str).str.replace(r'\D', '', regex=True)
    df['population'] = pd.to_numeric(df['population'], errors='coerce').fillna(0).astype(int)

    region_map = {
        "Ханты-Мансийский АО": "Ханты-Мансийский автономный округ",
        "Чукотский АО": "Чукотский автономный округ",
        "Еврейская АО": "Еврейская автономная область",
        "Ямало-Ненецкий АО": "Ямало-Ненецкий автономный округ",
        "Ненецкий АО": "Ненецкий автономный округ"
    }
    df['region'] = df['region'].replace(region_map)

    mask_unrecognized = df['city'].str.contains("не призн\\.", case=False, na=False)
    df_unrecognized = df[mask_unrecognized].copy()
    df_recognized = df[~mask_unrecognized].copy()

    df_recognized.to_csv(csv_path, index=False, encoding='utf-8')
    df_unrecognized.to_csv(unrecognized_path, index=False, encoding='utf-8')
    print(f"Основной файл сохранён: {csv_path}")
    print(f"Города с 'не призн.': сохранены отдельно: {unrecognized_path}")

    # --- Шаг 2: Геокодирование городов ---
    print("\n Начинаем геокодирование городов...")
    geolocator = Nominatim(user_agent="geoapi")

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cache = json.load(f)
    else:
        cache = {}

    def get_coordinates(city, region):
        key = f"{city}, {region}"
        if key in cache:
            return cache[key]
        try:
            location = geolocator.geocode(f"{city}, {region}, Russia", timeout=10)
            if location:
                coords = (location.latitude, location.longitude)
            else:
                coords = (None, None)
        except GeocoderTimedOut:
            time.sleep(1)
            return get_coordinates(city, region)
        cache[key] = coords
        return coords

    df = pd.read_csv(csv_path)
    tqdm.pandas()

    def fill_missing_coords(row):
        return pd.Series(get_coordinates(row['city'], row['region']))

    df[['latitude', 'longitude']] = df.progress_apply(fill_missing_coords, axis=1)

    df.to_csv(output_path, index=False)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    missing_coords = df[df['latitude'].isna() | df['longitude'].isna()]
    if not missing_coords.empty:
        print("\n Не удалось получить координаты для следующих городов:")
        for _, row in missing_coords.iterrows():
            print(f"- {row['city']} ({row['region']})")
    else:
        print("\n Все города успешно геокодированы.")

    print(f"\n Данные с координатами сохранены в {output_path}")


# Запуск скрипта
if __name__ == "__main__":
    fetch_and_geocode_russian_cities()
