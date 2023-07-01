from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
from datetime import datetime
import json, random
import socket

es = Elasticsearch(
    cloud_id='',
    basic_auth=('elastic', ''),
)

index_realtime = "realtime"

mapping_realtime = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "Date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "Open": {"type": "double"},
            "High": {"type": "double"},
            "Low": {"type": "double"},
            "Close": {"type": "double"},
            "Volume": {"type": "double"},
            "RSI": {"type": "double"},
            "MACD": {"type": "double"},
            "MACD_Signal": {"type": "double"},
            "CCI": {"type": "double"},
            "IsBull": {"type": "integer"},
            "IsBullishEngulfing": {"type": "integer"},
        }
    }
}

response_realtime = None
# raw-custom 인덱스 생성
if es.indices.exists(index=index_realtime):
    es.indices.delete(index=index_realtime)
    response_raw_custom = es.indices.create(index=index_realtime, body=mapping_realtime)
else:
    response_raw_custom = es.indices.create(index=index_realtime, body=mapping_realtime)    

# 응답 확인
if response_raw_custom["acknowledged"]:
    print(f"인덱스 '{index_realtime}'가 성공적으로 생성되었습니다.")
else:
    print(f"인덱스 '{index_realtime}' 생성에 실패했습니다.")


num = random.randrange(1, 10000)
consumer_realtime = Consumer({'bootstrap.servers': "",
        'group.id': f'1raw{num}',
        'auto.offset.reset': 'earliest'})

realtime = 'indicator.json'
consumer_realtime.subscribe([realtime])

while True:
    msg1 = consumer_realtime.poll(0)
    if msg1 is not None:
        record1 = msg1.value().decode('utf-8')
        dict_data = json.loads(record1)
        for key, value in dict_data.items():
            if key == '@timestamp' or key == 'Date':
                pass
            elif key == 'IsBull' or key == 'IsBullishEngulfing':
                dict_data[key] = int(value)
            else:
                dict_data[key] = float(value)
        
        es.index(index=index_realtime, document=dict_data)

