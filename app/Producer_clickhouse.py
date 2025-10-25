import json
import requests
import logging
import sys 
import time
from confluent_kafka import Producer, KafkaException
import os
from city_loader import load_russian_cities, distribute_cities_to_topics, get_all_cities_coordinates

# Настройка логирования
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Очистка старых обработчиков
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # stdout для INFO
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
    
    # stderr для WARNING/ERROR
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    
    formatter = logging.Formatter(
        '%(asctime)s - PRODUCER - %(levelname)s - %(message)s'
    )
    stdout_handler.setFormatter(formatter)
    stderr_handler.setFormatter(formatter)
    
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
    
    # Kafka логи только ERROR
    kafka_logger = logging.getLogger('confluent_kafka')
    kafka_logger.setLevel(logging.ERROR)
    kafka_logger.propagate = False

def create_producer():
    producer_conf = {
        'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
        'acks': 'all',
        'delivery.report.only.error': False,
        'retries': 3,
        'log_level': 4,  # Только ERROR для Kafka библиотеки
    }
    try:
        return Producer(producer_conf)
    except KafkaException as e:
        logging.error(f"Ошибка при создании продюсера Kafka: {e}")
        raise

def on_delivery(err, msg):
    city_name = msg.key().decode('utf-8') if msg.key() else 'unknown'
    
    if err is not None:
        logging.error(f"Город {city_name} - доставка failed: {err} (топик: {msg.topic()}, partition: {msg.partition()})")
    else:
        logging.info(f"Город {city_name} - успешно доставлено (топик: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()})")

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
    setup_logging()
    
    """
    Бесконечный цикл, так как этим файлом управляет 'supervisord.conf'
    """
    while True:
        try:
            # Загружаем и распределяем города
            russian_cities = load_russian_cities()
            if not russian_cities:
                logging.error("Не удалось загрузить российские города")
                time.sleep(300)  # Ждем 5 минут перед повторной попыткой
                continue
                
            city_to_topic = distribute_cities_to_topics(russian_cities)
            cities_coordinates = get_all_cities_coordinates(russian_cities)
            
            producer = create_producer()
            
            # Обрабатываем каждый город
            processed_cities = 0
            for city_name, coords in cities_coordinates.items():
                try:
                    weather_data = download_weather_data(coords['lat'], coords['lon'])
                    if weather_data is None:
                        logging.warning(f"Пропуск города {city_name} из-за ошибок загрузки данных.")
                        continue
                        
                    topic = get_topic_by_city(city_name, city_to_topic)
                    send_weather_data(producer, weather_data, city_name, topic)
                    save_weather_data(city_name, weather_data, topic)
                    processed_cities += 1
                    
                except Exception as e:
                    logging.error(f"Ошибка обработки города {city_name}: {e}")
                    continue
                    
            producer.flush()
            logging.info(f"Данные о погоде для {processed_cities} городов отправлены в Kafka.")

            # Ждем 5 минут перед следующим запуском
            logging.info("Ожидание 5 минут до следующего сбора данных...")
            time.sleep(300)
            
        except Exception as e:
            logging.error(f"Ошибка при выполнении программы: {e}")
            logging.info("Повторная попытка через 1 минуту...")
            time.sleep(60)  # Ждем минуту перед повторной попыткой

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