import logging
from clickhouse_driver import Client
from datetime import datetime
import pytz
import key_CH

logger = logging.getLogger(__name__)

def create_clickhouse_connection():
    """Создание подключения к ClickHouse"""
    try:
        client = Client(
            host='clickhouse_db',
            port=9000,
            user=key_CH.user,
            password=key_CH.password,
            database=key_CH.database
        )
        logger.info("Подключение к ClickHouse успешно")
        return client
    except Exception as e:
        logger.error(f"Ошибка подключения к ClickHouse: {e}")
        return None

def process_weather_data_clickhouse(client, weather_data):
    """Обработка и вставка данных погоды в ClickHouse"""
    try:
        msc = pytz.timezone('europe/moscow')
        now = datetime.now(msc)
        
        # Безопасное извлечение данных из JSON
        weather_info = weather_data.get('weather', [{}])[0] if weather_data.get('weather') else {}
        coord = weather_data.get('coord', {})
        main_data = weather_data.get('main', {})
        wind = weather_data.get('wind', {})
        clouds = weather_data.get('clouds', {})
        sys_data = weather_data.get('sys', {})
        
        # Подготовка данных для вставки
        row = {
            'event_date': now.date(),
            'event_time': now,
            'city_name': weather_data.get('name', ''),
            'timezone': weather_data.get('timezone', 0),
            'country': sys_data.get('country', ''),
            'longitude': coord.get('lon', 0),
            'latitude': coord.get('lat', 0),
            'weather_main': weather_info.get('main', ''),
            'weather_description': weather_info.get('description', ''),
            'temperature': main_data.get('temp', 0),
            'feels_like': main_data.get('feels_like', 0),
            'temp_min': main_data.get('temp_min', 0),
            'temp_max': main_data.get('temp_max', 0),
            'pressure': main_data.get('pressure', 0),
            'humidity': main_data.get('humidity', 0),
            'visibility': weather_data.get('visibility', 0),
            'wind_speed': wind.get('speed', 0),
            'wind_degree': wind.get('deg', 0),
            'wind_gust': wind.get('gust', 0),
            'cloudiness': clouds.get('all', 0),
            'sunrise_unix': sys_data.get('sunrise', 0),
            'sunset_unix': sys_data.get('sunset', 0)
        }
        
        # Обработка временных меток sunrise/sunset
        sunrise_unix = sys_data.get('sunrise')
        sunset_unix = sys_data.get('sunset')
        
        row['sunrise'] = datetime.fromtimestamp(sunrise_unix) if sunrise_unix else None
        row['sunset'] = datetime.fromtimestamp(sunset_unix) if sunset_unix else None
        
        # Вставка данных
        query = """
        INSERT INTO weather_fact (
            event_date, event_time, city_name, timezone, country, longitude, latitude,
            weather_main, weather_description, temperature, feels_like, temp_min, temp_max,
            pressure, humidity, visibility, wind_speed, wind_degree, wind_gust, cloudiness,
            sunrise, sunset
        ) VALUES
        """
        
        client.execute(query, [row])
        logging.info(f"Данные для города {row['city_name']} успешно вставлены в ClickHouse")
        
    except Exception as e:
        logging.error(f"Ошибка обработки данных для ClickHouse: {e}")
        raise