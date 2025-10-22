FROM python:3.12

RUN apt update && apt -y install cron supervisor
RUN pip install --upgrade pip --root-user-action=ignore

# Устанавить рабочую директорию в контейнере
WORKDIR /home

# Копируем файл requirements.txt в рабочую директорию контейнера
COPY requirements.txt .

# Устанавливаем зависимости из requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Скопировать cron-docker файл в каталог cron.d 
COPY ./cron_docker /etc/cron.d/crontab
 
# Предоставить права на выполнение задания cron
RUN chmod 0644 /etc/cron.d/crontab

# Применить задание cron
RUN /usr/bin/crontab /etc/cron.d/crontab
 
# Создать файл журнала для cron
RUN touch /var/log/cron_docker_producer.log /var/log/cron_docker_consumer.log

# Создать директорию для supervisord
RUN mkdir -p /var/log/supervisor
 
# Копируем конфиг supervisord
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
 
# Запустить команду при запуске контейнера
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]