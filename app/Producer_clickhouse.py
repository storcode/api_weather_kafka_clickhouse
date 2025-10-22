import json
import requests
import logging
from confluent_kafka import Producer, KafkaException
import os
from city_loader import load_russian_cities, distribute_cities_to_topics, get_all_cities_coordinates

# Настройка логирования
logging.basicConfig(level=logging.INFO)

def create_producer():
    producer_conf = {
        'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
        'acks': 'all',
        'delivery.report.only.error': False,
        'retries': 3,
    }
    try:
        return Producer(producer_conf)
    except KafkaException as e:
        logging.error(f"Ошибка при создании продюсера Kafka: {e}")
        raise

def on_delivery(err, msg):
    if err is not None:
        logging.error(f"Сообщение {msg.key()} не доставлено {err}")
    else:
        logging.info(f"Сообщение доставлено в топик {msg.topic()} [{msg.partition()}] по смещению {msg.offset()}")

def download_weather_data(lat, lon):
    try:
        from key_appid import key_appid
        url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&APPID={key_appid}&units=metric'
        response = requests.get(url=url)
        response.raise_for_status()
        data = response.json()
        if not data:
            raise ValueError("Получены пустые данные от API")
        return data
    except requests.RequestException as e:
        logging.error(f"Ошибка при скачивании данных о погоде: {e}")
        raise

def save_weather_data(city, data, topic):
    directory = '/home/downloads_weather'
    os.makedirs(directory, exist_ok=True)
    filename = f'/home/downloads_weather/{topic}_{city}_weather.json'
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logging.info(f"Данные о погоде для {city} сохранены в файл {filename}")

def get_topic_by_city(city_name, city_to_topic):
    """Определяет топик для города на основе распределения"""
    for topic, cities in city_to_topic.items():
        if city_name in cities:
            return topic
    # Если город не найден в распределении, используем топик по умолчанию
    return 'weather_topic_3'

def main():
    try:
        # Загружаем и распределяем города
        russian_cities = load_russian_cities()
        if not russian_cities:
            logging.error("Не удалось загрузить российские города")
            return
            
        city_to_topic = distribute_cities_to_topics(russian_cities)
        cities_coordinates = get_all_cities_coordinates(russian_cities)
        
        # Логируем распределение для проверки
        for topic, cities in city_to_topic.items():
            logging.info(f"Топик {topic}: {len(cities)} городов")
            if len(cities) > 0:
                logging.info(f"  Примеры: {cities[:3]}...")
        
        producer = create_producer()
        
        # Обрабатываем каждый город
        for city_name, coords in cities_coordinates.items():
            try:
                weather_data = download_weather_data(coords['lat'], coords['lon'])
                if weather_data is None:
                    logging.warning(f"Пропуск города {city_name} из-за ошибок загрузки данных.")
                    continue
                    
                topic = get_topic_by_city(city_name, city_to_topic)
                send_weather_data(producer, weather_data, city_name, topic)
                save_weather_data(city_name, weather_data, topic)
                
            except Exception as e:
                logging.error(f"Ошибка обработки города {city_name}: {e}")
                continue
                
        producer.flush()
        logging.info("Данные о погоде отправлены в Kafka.")
        
    except Exception as e:
        logging.error(f"Ошибка при выполнении программы: {e}")

def send_weather_data(producer, data, city, topic):
    """Отправка данных в Kafka"""
    try:
        message = json.dumps(data)
        producer.produce(topic, message, callback=on_delivery, key=city.encode('utf-8'))
        logging.info(f"Отправлены данные для {city} в топик {topic}")
    except KafkaException as e:
        logging.error(f"Ошибка при отправке сообщения в Kafka: {e}")
        raise

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Действие прервано')