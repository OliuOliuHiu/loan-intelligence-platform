FROM apache/airflow:2.8.4-python3.11

USER root
RUN apt-get update && apt-get install -y unixodbc unixodbc-dev gcc g++ && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    "protobuf<5.0.0" \
    requests pandas SQLAlchemy psycopg2-binary pyodbc \
    dbt-core dbt-postgres
COPY ./dbt_project /opt/airflow/dbt_project
RUN cd /opt/airflow/dbt_project && dbt deps