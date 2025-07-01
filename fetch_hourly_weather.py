"""
Скрипт для получения погодных данных с 6-часовым шагом по координатам городов через Open-Meteo API.

📌 Что делает скрипт:
1. Использует координаты городов из CSV-файла 
   с обязательными полями city, latitude и longitude (по умолчанию `cities_with_coordinates.csv`).
2. Получает погодные данные с шагом 6 часов для каждого города за указанный период.
3. Сохраняет результаты в CSV-файл (по умолчанию `hourly_weather.csv`).
4. Повторно не запрашивает данные для уже обработанных городов (если выходной файл существует).
5. Добавляет задержку между запросами для защиты от превышения лимитов API.

📌 Примечание:
Если необходимо повторно загрузить данные для уже обработанных городов (например, для другого периода),
укажите новый путь к файлу с помощью параметра `--output` или удалите существующий выходной файл.

🐍 Требования:
  - Python 3.10+

📦 Зависимости:
  - openmeteo-requests requests-cache retry-requests numpy pandas

🛠 Аргументы командной строки:
--input       Путь к входному CSV с координатами городов (по умолчанию `cities_with_coordinates.csv`)
--output      Путь к выходному CSV для сохранения погодных данных (по умолчанию `hourly_weather.csv`)
--start       Дата начала (обязательный аргумент), формат: YYYY-MM-DD
--end         Дата окончания (обязательный аргумент), формат: YYYY-MM-DD
--delay       Задержка между запросами в секундах (по умолчанию 30)

⚙ Использование:
    python fetch_hourly_weather.py --input 'вх.файл' --output 'выход.файл' --start 2023-01-01 --end 2023-01-10 --delay 15

⚠ Требуется подключение к интернету.
⚠ Использует кэширование и повторные попытки при сбоях подключения.

"""


import os
import time
import json
import argparse  
import requests
import requests_cache
import pandas as pd
from retry_requests import retry
import openmeteo_requests

def main():
    # --- Аргументы командной строки ---
    parser = argparse.ArgumentParser(description="Получение погодных данных с шагом 6 часов")
    parser.add_argument("--input", default="cities_with_coordinates.csv", help="Входной CSV с координатами")
    parser.add_argument("--output", default="hourly_weather.csv", help="Файл для сохранения данных")
    parser.add_argument("--start", required=True, help="Дата начала в формате YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Дата окончания в формате YYYY-MM-DD")
    parser.add_argument("--delay", type=int, default=30, help="Задержка между запросами в секундах")
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output
    start_date = args.start
    end_date = args.end
    delay = args.delay

    # --- Проверка уже обработанных городов ---
    processed_cities = set()
    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        processed_cities = set(existing_df["city"].unique())
        print(f"🟡 Уже обработаны города: {processed_cities}")

    data = pd.read_csv(input_path)
    data = data[~data["city"].isin(processed_cities)]

    # --- Настройка клиента с кэшем и ретраями ---
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    all_results = []

    for index, row in data.iterrows():
        params = {
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": [
                "temperature_2m", "relative_humidity_2m", "rain", "snowfall", "snow_depth",
                "is_day", "precipitation", "wind_direction_100m", "wind_speed_100m"
            ],
            "temporal_resolution": "hourly_6"
        }

        try:
            responses = openmeteo.weather_api("https://archive-api.open-meteo.com/v1/archive", params=params)
            response = responses[0]
            hourly = response.Hourly()

            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                ),
                "city": row["city"],
                "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
                "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
                "rain": hourly.Variables(2).ValuesAsNumpy(),
                "snowfall": hourly.Variables(3).ValuesAsNumpy(),
                "snow_depth": hourly.Variables(4).ValuesAsNumpy(),
                "is_day": hourly.Variables(5).ValuesAsNumpy(),
                "precipitation": hourly.Variables(6).ValuesAsNumpy(),
                "wind_direction_100m": hourly.Variables(7).ValuesAsNumpy(),
                "wind_speed_100m": hourly.Variables(8).ValuesAsNumpy(),
            }

            df = pd.DataFrame(hourly_data)
            all_results.append(df)
            print(f"✅ Данные получены для города {row['city']}. Ждём {delay} сек...")
            time.sleep(delay)

        except Exception as e:
            print(f"❌ Ошибка при обработке города {row['city']}: {e}")
            if all_results:
                print("💾 Сохраняем уже полученные данные перед выходом...")
                new_df = pd.concat(all_results, ignore_index=True)
                if os.path.exists(output_path):
                    existing_df = pd.read_csv(output_path)
                    final_df = pd.concat([existing_df, new_df], ignore_index=True)
                else:
                    final_df = new_df
                final_df.to_csv(output_path, index=False)
                print(f"📁 Данные сохранены в {output_path}")
            else:
                print("⚠ Нет данных для сохранения.")
            break

    else:
        if all_results:
            new_df = pd.concat(all_results, ignore_index=True)
            if os.path.exists(output_path):
                existing_df = pd.read_csv(output_path)
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                final_df = new_df
            final_df.to_csv(output_path, index=False)
            print(f"💾 Все данные успешно сохранены в {output_path}")
        else:
            print("⚠ Нет новых данных для сохранения.")

if __name__ == "__main__":
    main()
