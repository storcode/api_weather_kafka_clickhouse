import logging
from clickhouse_driver import Client
from datetime import datetime
import pytz
import key_CH
from typing import List, Dict

logger = logging.getLogger(__name__)

def create_clickhouse_connection():
    """Создание подключения к ClickHouse с оптимизированными настройками"""
    try:
        client = Client(
            host='clickhouse_db',
            port=9000,
            user=key_CH.user,
            password=key_CH.password,
            database=key_CH.database,
            settings={
                'max_execution_time': 60,
                'receive_timeout': 30,
                'send_timeout': 30,
                'insert_deduplicate': 0,  # Отключаем дедупликацию для производительности
            }
        )
        logger.info("Подключение к ClickHouse успешно")
        return client
    except Exception as e:
        logger.error(f"Ошибка подключения к ClickHouse: {e}")
        return None

def process_weather_batch_clickhouse(client, batch_data: List[Dict]):
    """Пакетная вставка данных погоды в ClickHouse"""
    if not batch_data:
        logger.warning("Пустой батч для вставки")
        return True
        
    try:
        msc = pytz.timezone('europe/moscow')
        now = datetime.now(msc)
        rows = []
        
        for weather_data in batch_data:
            # Безопасное извлечение данных из JSON
            weather_info = weather_data.get('weather', [{}])[0] if weather_data.get('weather') else {}
            coord = weather_data.get('coord', {})
            main_data = weather_data.get('main', {})
            wind = weather_data.get('wind', {})
            clouds = weather_data.get('clouds', {})
            sys_data = weather_data.get('sys', {})
            
            # Обработка временных меток sunrise/sunset
            sunrise_unix = sys_data.get('sunrise')
            sunset_unix = sys_data.get('sunset')
            
            sunrise_dt = datetime.fromtimestamp(sunrise_unix) if sunrise_unix else None
            sunset_dt = datetime.fromtimestamp(sunset_unix) if sunset_unix else None
            
            # Подготовка строки для вставки
            row = (
                now.date(),
                now,
                weather_data.get('name', ''),
                weather_data.get('timezone', 0),
                sys_data.get('country', ''),
                coord.get('lon', 0),
                coord.get('lat', 0),
                weather_info.get('main', ''),
                weather_info.get('description', ''),
                float(main_data.get('temp', 0)),
                float(main_data.get('feels_like', 0)),
                float(main_data.get('temp_min', 0)),
                float(main_data.get('temp_max', 0)),
                int(main_data.get('pressure', 0)),
                int(main_data.get('humidity', 0)),
                int(weather_data.get('visibility', 0)),
                float(wind.get('speed', 0)),
                int(wind.get('deg', 0)),
                float(wind.get('gust', 0)),
                int(clouds.get('all', 0)),
                sunrise_dt,
                sunset_dt
            )
            rows.append(row)
        
        # Пакетная вставка в БУФЕРИЗОВАННУЮ таблицу
        query = """
        INSERT INTO weather_fact_buffer (
            event_date, event_time, city_name, timezone, country, longitude, latitude,
            weather_main, weather_description, temperature, feels_like, temp_min, temp_max,
            pressure, humidity, visibility, wind_speed, wind_degree, wind_gust, cloudiness,
            sunrise, sunset
        ) VALUES
        """
        
        client.execute(query, rows)
        logger.debug(f"Батч из {len(rows)} записей успешно вставлен в ClickHouse")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка пакетной обработки данных для ClickHouse: {e}")
        return False

# Старая функция для обратной совместимости (можно удалить после перехода на батчинг)
def process_weather_data_clickhouse(client, weather_data):
    """Старая функция для одиночных вставок (deprecated)"""
    return process_weather_batch_clickhouse(client, [weather_data])