FROM apache/airflow:2.7.0-python3.10

# Дополнительные Python-зависимости проекта поверх базового образа Airflow.
# Версии фиксированы, чтобы сборка была воспроизводимой.
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# Каталоги, которые позже перекрываются томами из docker-compose.yml.
RUN mkdir -p /opt/airflow/dags /opt/airflow/include /opt/airflow/scripts /opt/airflow/sql /opt/airflow/logs

# Общий код (include/, dags/, scripts/) импортируется как пакеты верхнего уровня.
ENV PYTHONPATH=/opt/airflow

# Явно задаём пользователя из базового образа, чтобы образ не запускался от root.
USER airflow