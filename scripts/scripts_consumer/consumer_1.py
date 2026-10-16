from kafka import KafkaConsumer
import json
#import clickhouse_connect
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


import os
from dotenv import load_dotenv
# загружаю переменные окружения
# load_dotenv()
# CLICKHOUSE_USER='default'
# CLICKHOUSE_PASSWORD='password'

consumer = KafkaConsumer(
    "raw-products-kbju",
    bootstrap_servers="kafka:29092",
    group_id= None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=5000
    
)

client = clickhouse_connect.get_client(host='clickhouse', port=8123, username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD) 
print("\n⏳  читаю raw_products_kbju")


for message in consumer:
    doc = message.value
    row = [(
        doc.get('prod_id'),
        doc.get('calories'),
        doc.get('carbohydrates'),
        doc.get('fat'),
        doc.get('protein')
    )]

    client.insert(table='raw_products_kbju', data=row, column_names=['product_id', 'calories', 'carbohydrates', 'fat', 'protein'])
   # print(f'✅ запись {row} вставлена в таблицу raw_products_kbju')

consumer.close()
client.close()
print(f'✅ Топик raw_products_kbju прочитан')
