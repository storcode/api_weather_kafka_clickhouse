-- Создание базы данных
CREATE DATABASE IF NOT EXISTS dwh;

-- Основная таблица с оптимизированными типами
CREATE TABLE IF NOT EXISTS dwh.weather_fact
(
    event_date Date DEFAULT today() COMMENT 'Дата измерения',
    event_time DateTime DEFAULT now() COMMENT 'Время измерения',
    city_name LowCardinality(String) COMMENT 'Название города',
    timezone UInt16 COMMENT 'Часовой пояс в секундах от UTC',
    country FixedString(2) COMMENT 'ISO код страны (2 символа)',
    longitude Float32 COMMENT 'Географическая долгота',
    latitude Float32 COMMENT 'Географическая широта',
    weather_main LowCardinality(String) COMMENT 'Основная категория погоды',
    weather_description LowCardinality(String) COMMENT 'Детальное описание погоды',
    temperature Decimal(5,2) COMMENT 'Температура в градусах Цельсия',
    feels_like Decimal(5,2) COMMENT 'Ощущаемая температура',
    temp_min Decimal(5,2) COMMENT 'Минимальная температура за период',
    temp_max Decimal(5,2) COMMENT 'Максимальная температура за период',
    pressure UInt16 COMMENT 'Атмосферное давление в hPa',
    humidity UInt8 COMMENT 'Относительная влажность в процентах',
    visibility UInt16 COMMENT 'Видимость в метрах',
    wind_speed Decimal(4,2) COMMENT 'Скорость ветра в м/с',
    wind_degree UInt16 COMMENT 'Направление ветра в градусах (0-360)',
    wind_gust Decimal(4,2) DEFAULT 0 COMMENT 'Порывы ветра в м/с (0 если нет данных)',
    cloudiness UInt8 COMMENT 'Облачность в процентах',
    sunrise DateTime COMMENT 'Время восхода солнца',
    sunset DateTime COMMENT 'Время заката солнца'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, city_name, event_time)
COMMENT 'Таблица фактов погодных данных. Содержит сырые данные с метеостанций.';

-- Буферизованная таблица для частых вставок
CREATE TABLE IF NOT EXISTS dwh.weather_fact_buffer AS dwh.weather_fact
ENGINE = Buffer(
    'dwh', 'weather_fact', 
    1,      -- num_layers
    30,     -- min_time (секунды) - ждем 30 секунд
    60,     -- max_time (секунды) - максимум 60 секунд
    100,    -- min_rows - минимум 100 строк
    10000,  -- max_rows - максимум 10000 строк
    0,      -- min_bytes
    0       -- max_bytes
);

