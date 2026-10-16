"""
DAG: connection_test

Назначение:
    Ручной smoke-тест доступности внешних систем из окружения Airflow:
    ClickHouse, Spark -> ClickHouse, Spark -> S3 (MinIO) и MongoDB.

Что проверяется:
    1. connection_to_CH
       Подключение Airflow к ClickHouse через Airflow Connection
       `clickhouse_default` и ClickHouseHook.
       Выполняется `SELECT version()`, то есть проверяется нативное
       подключение к ClickHouse (обычно порт 9000).

    2. connection_from_spark_to_ch
       Проверка чтения данных из ClickHouse через Apache Spark.
       Параметры подключения берутся из Airflow Connection
       `clickhouse_http` (host, port, login, password, schema).
       Spark использует JDBC-драйвер ClickHouse и обычно обращается
       к HTTP-интерфейсу ClickHouse на порту 8123.
       Выполняется тестовый запрос:
           SELECT 1 AS test, now() AS time

    3. connection_from_spark_to_s3
       Проверка записи и обратного чтения тестового DataFrame
       из Spark в S3-совместимое хранилище MinIO.
       Запись выполняется в формате Parquet по пути:
           s3a://report-clickhouse/FINAL-SUCCESS.parquet

    4. connection_to_mongo
       Проверка доступности MongoDB через PyMongo.
       Выполняется команда `ping`, определяется версия MongoDB.
       Подключение выполняется по URI:
           mongodb://mongo:27017/

Структура DAG:
    start ──> connection_to_CH
    start ──> connection_from_spark_to_ch
    start ──> connection_from_spark_to_s3
    start ──> connection_to_mongo

Особенности:
    - DAG запускается только вручную (`schedule_interval=None`).
    - `catchup=False`: Airflow не создаёт исторические запуски.
    - Все проверки независимы и запускаются параллельно после `start`.
    - Ошибка любой проверки приводит к падению соответствующей задачи.
    - DAG предназначен для диагностики инфраструктуры, а не для
      регулярной обработки данных.

Используемые Airflow Connections:
    - clickhouse_default : нативное подключение Airflow -> ClickHouse
    - clickhouse_http    : параметры для Spark JDBC -> ClickHouse

Требования к окружению:
    - Установленный airflow-clickhouse-plugin.
    - PyMongo.
    - PySpark.
    - ClickHouse JDBC driver:
      /opt/spark/jars/clickhouse-jdbc-0.4.6.jar
    - Hadoop AWS / AWS SDK JAR для работы Spark с S3/MinIO:
      /opt/spark/jars/hadoop-aws-3.3.4.jar
      /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
      /opt/spark/jars/commons-compress-1.20.jar

Как использовать:
    1. Открыть Airflow UI.
    2. Найти DAG `connection_test`.
    3. Нажать Trigger DAG.
    4. Проверить статус задач и логи:
       - успешное подключение должно содержать `✅`;
       - при ошибке задача упадёт и в логе будет причина.
"""
from airflow import DAG
from airflow.hooks.base import BaseHook  # для подключения к Spark
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from pymongo import MongoClient 
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from pyspark.sql import SparkSession
from datetime import datetime
from airflow.exceptions import AirflowException
import logging

# проверка соединения Clickhouse
def check_clickhouse_connection():
    try:
        ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
        version = ch_hook.execute('SELECT version()')  # или get_first()
        logging.info(f"✅ Успешное подключение к Clickhouse: {version}")
    except Exception as e:
        logging.error('❌ ClickHouse Connection failed: %s', str(e))
        raise AirflowException(f"ClickHouse connection failed: {e}")

# проверка соединения из SPARK->CLICKHOUSE
def conn_from_spark():
    conn = BaseHook.get_connection("clickhouse_http")
    host = conn.host              # "clickhouse"
    port = conn.port              # 8123
    user = conn.login             # "default"
    password = conn.password      # "password"
    database = conn.schema or "default"

    url = f"jdbc:clickhouse://{host}:{port}/{database}"

    spark = SparkSession.builder \
        .appName("ReadFromClickHouse") \
        .master("local[*]") \
        .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.4.6.jar") \
        .getOrCreate()

    logging.info("✅ Spark session opened")

    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("query", "SELECT 1 AS test, now() AS time") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .load()

        df.show(truncate=False)

        df_transformed = df.select("test", "time")
        df_transformed.show()

    except Exception as e:
        logging.error(" ❌ Spark JDBC read failed: %s", str(e))
        raise
    finally:
        spark.stop()
        logging.info("Spark session stopped")
# проверка соединения SPARK->S3
def run_spark_s3():
    spark = SparkSession.builder \
        .appName("SUCCESS") \
        .config("spark.jars", 
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/opt/spark/jars/commons-compress-1.20.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "StrongPassword123!") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logging.info(f"✅ SparkSession готова {datetime.now()}")

    # Создание тестового DF с дальнейшей загрузкой в MiniO
    df = spark.createDataFrame([
        ("test1", 1, datetime.now()), 
        ("test2", 2, datetime.now())
    ], ["name", "value", "date"])

    df.write.mode("overwrite").parquet("s3a://report-clickhouse/FINAL-SUCCESS.parquet")
    logging.info(f"✅ ЗАПИСЬ УСПЕШНА!")

    # Чтение обратно
    logging.info(f"✅ пример чтениия из S3!")
    spark.read.parquet("s3a://report-clickhouse/FINAL-SUCCESS.parquet").show()

    spark.stop()
    logging.info(f"✓ SparkSession остановлена {datetime.now()}")

# Проверка соединения к MONGODB
def test_pymongo():
    # Подключение к MongoDB
    client = MongoClient("mongodb://mongo:27017/", serverSelectionTimeoutMS=5000) 
    try:
        # Ping для проверки соединения + получение версии
        server_info = client.admin.command('ping')
        version_info = client.server_info()
        
        version = version_info['version']
        version_array = version_info['versionArray']
        
        print(f"✅ Подключение OK")
        print(f"📊 Версия MongoDB: {version}")
        print(f"🔢 Версия (массив): {version_array}")
        print(f"🏓 Ping выполнен успешно")
        
        return f"MongoDB {version} доступен"
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"❌ Ошибка подключения: {str(e)}")
        raise AirflowException(f"MongoDB недоступен: {str(e)}")
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {str(e)}")
        raise  AirflowException(f"❌ Неожиданная ошибка: {str(e)}")




with DAG(
    'connection_test',
    schedule_interval=None,
    start_date=datetime(2026, 2, 2),
    catchup=False,
    tags=['test_CH'],
) as dag:

    start = EmptyOperator(task_id='start')

    test_CH_connection_task = PythonOperator(
        task_id='connection_to_CH',
        python_callable=check_clickhouse_connection,
    )

    run_connection_from_spark_to_ch = PythonOperator(
            task_id="connection_from_spark_to_ch",
            python_callable=conn_from_spark,
        )

    run_connection_from_spark_to_s3 = PythonOperator(
        task_id="connection_from_spark_to_s3",
        python_callable= run_spark_s3,
    )

    run_connection_to_mongo = PythonOperator( 
            task_id="connection_to_mongo",     
            python_callable=test_pymongo,
        )
    finish = EmptyOperator(task_id='finish')

    #start >> [ test_CH_connection_task, run_connection_from_spark_to_ch, run_connection_from_spark_to_s3, run_connection_to_mongo] >> finish
    start >> [
    test_CH_connection_task,
    run_connection_from_spark_to_ch,
    run_connection_from_spark_to_s3,
    run_connection_to_mongo,
    ] >> finish