import json
from confluent_kafka import Consumer, KafkaError
import logging
import sys
from clickhouse_db import create_clickhouse_connection, process_weather_batch_clickhouse
from datetime import datetime
from typing import List, Dict

# Настройка логирования
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
    
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    
    formatter = logging.Formatter(
        '%(asctime)s - CONSUMER - %(levelname)s - %(message)s'
    )
    stdout_handler.setFormatter(formatter)
    stderr_handler.setFormatter(formatter)
    
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
    
    kafka_logger = logging.getLogger('confluent_kafka')
    kafka_logger.setLevel(logging.ERROR)
    kafka_logger.propagate = False

class WeatherBatchProcessor:
    def __init__(self, client, batch_size: int = 90, max_wait_seconds: int = 300):
        self.client = client
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds
        self.batch: List[Dict] = []
        self.last_insert_time = datetime.now()
        self.processed_count = 0
        self.batch_start_time = datetime.now()
        
    def add_to_batch(self, weather_data: Dict) -> bool:
        """Добавляет данные в батч и возвращает True если нужно выполнить вставку"""
        self.batch.append(weather_data)
        
        current_time = datetime.now()
        time_since_last_insert = (current_time - self.last_insert_time).total_seconds()
        
        # Условия для вставки: достигли размера батча или прошло много времени
        needs_flush = (len(self.batch) >= self.batch_size or 
                      time_since_last_insert >= self.max_wait_seconds)
        
        if needs_flush:
            batch_duration = (current_time - self.batch_start_time).total_seconds()
            logging.info(f"Батч готов к вставке: {len(self.batch)} записей, сбор занял {batch_duration:.1f} сек")
            success = self.flush()
            self.batch_start_time = current_time
            return success
        return False
    
    def flush(self) -> bool:
        """Принудительная вставка накопленных данных"""
        if not self.batch:
            return True
            
        try:
            success = process_weather_batch_clickhouse(self.client, self.batch)
            if success:
                logging.info(f"Успешно вставлено {len(self.batch)} записей в ClickHouse")
                self.processed_count += len(self.batch)
                self.batch.clear()
                self.last_insert_time = datetime.now()
                return True
            else:
                logging.error("Ошибка при вставке батча в ClickHouse")
                return False
        except Exception as e:
            logging.error(f"Ошибка при flush батча: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Возвращает статистику обработки"""
        current_time = datetime.now()
        batch_duration = (current_time - self.batch_start_time).total_seconds()
        return {
            'current_batch_size': len(self.batch),
            'total_processed': self.processed_count,
            'batch_duration_sec': batch_duration,
            'time_since_last_insert': (current_time - self.last_insert_time).total_seconds()
        }

def main():
    setup_logging() 
    
    # Подключение к ClickHouse
    client = create_clickhouse_connection()
    if client is None:
        logging.error("Не удалось подключиться к ClickHouse. Завершение работы.")
        return

    # Инициализация батч-процессора
    batch_processor = WeatherBatchProcessor(
        client=client,
        batch_size=90,       
        max_wait_seconds=300 
    )

    # Настройка Kafka Consumer
    topics = ['weather_topic_1', 'weather_topic_2', 'weather_topic_3']
    consumer_conf = {
        'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
        'group.id': 'weather_consumer_group_clickhouse_optimized',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.poll.interval.ms': 360000,
        'session.timeout.ms': 15000
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(topics)
    
    logging.info('[*] Ожидается один батч из ~81 записи каждые 5 минут')

    try:
        last_stats_log = datetime.now()
        
        while True:
            msg = consumer.poll(1.0)  # 1 секунда таймаута
            
            if msg is None:
                # Проверяем таймаут батча
                current_time = datetime.now()
                batch_duration = (current_time - batch_processor.batch_start_time).total_seconds()
            
                if batch_duration >= 300 and batch_processor.batch:
                    logging.info("Таймаут батча (300 сек) - выполняем вставку")
                    if batch_processor.flush():
                        consumer.commit(asynchronous=False)
                        logging.debug("Коммит офсетов выполнен")
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Ошибка Consumer: {msg.error()}")
                    continue

            try:
                # Декодирование и парсинг сообщения
                body = msg.value().decode('utf-8')
                weather_data = json.loads(body)
                
                # Добавление в батч
                should_commit = batch_processor.add_to_batch(weather_data)
                
                # Коммит офсетов только после успешной вставки батча
                if should_commit:
                    consumer.commit(asynchronous=False)
                    logging.debug("Коммит офсетов в Kafka выполнен")
                
                # Логирование статистики
                current_time = datetime.now()
                if (current_time - last_stats_log).total_seconds() >= 60:
                    stats = batch_processor.get_stats()
                    logging.info(f"Статистика: {stats}")
                    last_stats_log = current_time
                    
            except json.JSONDecodeError as e:
                logging.error(f"Ошибка JSON декодирования: {e}")
            except Exception as e:
                logging.error(f"Ошибка обработки сообщения: {e}")

    except KeyboardInterrupt:
        logging.info('Получен сигнал прерывания')
    finally:
        # Принудительная вставка оставшихся данных перед закрытием
        if batch_processor.batch:
            logging.info("Завершение работы - вставка оставшихся данных...")
            batch_processor.flush()
        consumer.commit(asynchronous=False)
        consumer.close()
        logging.info(f"Всего обработано записей: {batch_processor.processed_count}")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info('Действие прервано')
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        raise