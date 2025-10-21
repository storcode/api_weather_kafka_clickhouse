import json
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

def load_russian_cities(json_file_path: str = '/home/city.list.json') -> List[Dict]:
    """Загружает российские города из JSON файла"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            cities_data = json.load(f)
        
        # Фильтруем только российские города
        russian_cities = [
            city for city in cities_data 
            if city.get('country') == 'RU'
        ]
        
        logger.info(f"Загружено {len(russian_cities)} российских городов")
        return russian_cities
        
    except Exception as e:
        logger.error(f"Ошибка загрузки файла городов: {e}")
        return []

def distribute_cities_to_topics(cities: List[Dict], topics_count: int = 3) -> Dict[str, List[str]]:
    """Распределяет города по топикам равномерно"""
    # Сортируем города по имени для consistent распределения
    sorted_cities = sorted(cities, key=lambda x: x['name'])
    
    # Создаем словарь для распределения
    city_to_topic = {f'weather_topic_{i+1}': [] for i in range(topics_count)}
    
    # Распределяем города по топикам round-robin
    for i, city in enumerate(sorted_cities):
        topic_index = i % topics_count + 1
        topic_name = f'weather_topic_{topic_index}'
        city_to_topic[topic_name].append(city['name'])
    
    # Логируем распределение
    for topic, cities_list in city_to_topic.items():
        logger.info(f"{topic}: {len(cities_list)} городов")
    
    return city_to_topic

def get_city_coordinates(cities: List[Dict], city_name: str) -> Dict[str, float]:
    """Возвращает координаты города по имени"""
    for city in cities:
        if city['name'] == city_name:
            return city['coord']
    return None

def get_all_cities_coordinates(cities: List[Dict]) -> Dict[str, Dict]:
    """Создает словарь с координатами всех городов"""
    return {
        city['name']: city['coord'] 
        for city in cities
    }