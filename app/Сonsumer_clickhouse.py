import json
from confluent_kafka import Consumer, KafkaError
import logging
from clickhouse_db import create_clickhouse_connection, process_weather_data_clickhouse

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Подключение к ClickHouse
    client = create_clickhouse_connection()
    if client is None:
        logging.error("Не удалось подключиться к ClickHouse. Завершение работы.")
        return

    # Настройка Kafka Consumer
    topics = ['weather_topic_1', 'weather_topic_2', 'weather_topic_3']
    consumer_conf = {
        'bootstrap.servers': 'kafka-1:9091,kafka-2:9092,kafka-3:9093',
        'group.id': 'weather_consumer_group_clickhouse',
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(topics)
    logging.info(' [*] Ожидание сообщения для ClickHouse. Нажмите <CTRL+C> для выхода')

    try:
        while True:
            msg = consumer.poll(2.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Ошибка Consumer: {msg.error()}")
                    continue
            try:
                body = msg.value().decode('utf-8')
                logging.info(f"Получено сообщение: {body}")
                weather_data = json.loads(body)
                
                # Обработка данных для ClickHouse
                process_weather_data_clickhouse(client, weather_data)
                
                consumer.commit(asynchronous=False)
            except json.JSONDecodeError as e:
                logging.error(f"Ошибка JSON декодирования: {e}")
            except Exception as e:
                logging.error(f"Ошибка обработки сообщения: {e}")
    except KeyboardInterrupt:
        logging.info('Действие прервано')
    finally:
        consumer.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Действие прервано')