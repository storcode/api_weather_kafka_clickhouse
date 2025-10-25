FROM python:3.12

RUN apt update && apt -y install supervisor && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip --root-user-action=ignore

# Устанавить рабочую директорию в контейнере
WORKDIR /home

# Копируем файл requirements.txt в рабочую директорию контейнера
COPY requirements.txt .

# Устанавливаем зависимости из requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Создать директорию для supervisord
RUN mkdir -p /var/log/supervisor
 
# Копируем конфиг supervisord
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
 
# Запустить команду при запуске контейнера
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]