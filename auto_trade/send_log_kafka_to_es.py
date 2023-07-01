from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
from datetime import datetime
import json, random
import socket

es = Elasticsearch(
    cloud_id='',
    basic_auth=('elastic', ''),
)

index_raw_custom = "eth2_price1"
index_raw_basic = "eth2_price2"
index_trade_custom = "eth2_transaction1"
index_trade_basic = "eth2_transaction2"

mapping_raw_custom = {
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
            "RoR": {"type": "double"}
        }
    }
}

mapping_raw_basic = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "Date2": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "Open2": {"type": "double"},
            "High2": {"type": "double"},
            "Low2": {"type": "double"},
            "Close2": {"type": "double"},
            "Volume2": {"type": "double"},
            "RSI2": {"type": "double"},
            "MACD2": {"type": "double"},
            "MACD_Signal2": {"type": "double"},
            "CCI2": {"type": "double"},
            "IsBull2": {"type": "integer"},
            "IsBullishEngulfing2": {"type": "integer"},
            "RoR2": {"type": "double"}
        }
    }
}

mapping_trade_custom = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "Datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "Position": {"type": "keyword"},
            "State": {"type": "keyword"},
            "Side": {"type": "keyword"},
            "Price": {"type": "double"},
            "PnL": {"type": "double"},
            "Amount": {"type": "double"},
            "Total": {"type": "double"}
        }
    }
}

mapping_trade_basic = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "Datetime2": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "Position2": {"type": "keyword"},
            "State2": {"type": "keyword"},
            "Side2": {"type": "keyword"},
            "Price2": {"type": "double"},
            "PnL2": {"type": "double"},
            "Amount2": {"type": "double"},
            "Total2": {"type": "double"}
        }
    }
}

response_raw_custom = None
response_raw_basic = None
response_trade_custom = None
response_trade_basic = None
# raw-custom 인덱스 생성
if es.indices.exists(index=index_raw_custom):
    es.indices.delete(index=index_raw_custom)
    response_raw_custom = es.indices.create(index=index_raw_custom, body=mapping_raw_custom)
else:
    response_raw_custom = es.indices.create(index=index_raw_custom, body=mapping_raw_custom)    

# 응답 확인
if response_raw_custom["acknowledged"]:
    print(f"인덱스 '{index_raw_custom}'가 성공적으로 생성되었습니다.")
else:
    print(f"인덱스 '{index_raw_custom}' 생성에 실패했습니다.")

# raw-basic 인덱스 생성
if es.indices.exists(index=index_raw_basic):
    es.indices.delete(index=index_raw_basic)
    response_raw_basic = es.indices.create(index=index_raw_basic, body=mapping_raw_basic)
else:
    response_raw_basic = es.indices.create(index=index_raw_basic, body=mapping_raw_basic)    

# 응답 확인
if response_raw_basic["acknowledged"]:
    print(f"인덱스 '{index_raw_basic}'가 성공적으로 생성되었습니다.")
else:
    print(f"인덱스 '{index_raw_basic}' 생성에 실패했습니다.")

# custom-transaction 인덱스 생성
if es.indices.exists(index=index_trade_custom):
    es.indices.delete(index=index_trade_custom)
    response_trade_custom = es.indices.create(index=index_trade_custom, body=mapping_trade_custom)
else:
    response_trade_custom = es.indices.create(index=index_trade_custom, body=mapping_trade_custom)    

# 응답 확인
if response_trade_custom["acknowledged"]:
    print(f"인덱스 '{index_trade_custom}'가 성공적으로 생성되었습니다.")
else:
    print(f"인덱스 '{index_trade_custom}' 생성에 실패했습니다.")

# basic-transaction 인덱스 생성
if es.indices.exists(index=index_trade_basic):
    es.indices.delete(index=index_trade_basic)
    response_trade_basic = es.indices.create(index=index_trade_basic, body=mapping_trade_basic)
else:
    response_trade_basic = es.indices.create(index=index_trade_basic, body=mapping_trade_basic)    

# 응답 확인
if response_trade_basic["acknowledged"]:
    print(f"인덱스 '{index_trade_basic}'가 성공적으로 생성되었습니다.")
else:
    print(f"인덱스 '{index_trade_basic}' 생성에 실패했습니다.")

num = random.randrange(1, 10000)
consumer_raw_custom = Consumer({'bootstrap.servers': "",
        'group.id': f'1raw{num}',
        'auto.offset.reset': 'earliest'})

consumer_raw_basic = Consumer({'bootstrap.servers': "",
        'group.id': f'2raw{num}',
        'auto.offset.reset': 'earliest'})

consumer_trade_custom = Consumer({'bootstrap.servers': "",
        'group.id': f'custom{num}',
        'auto.offset.reset': 'earliest'})

consumer_trade_basic = Consumer({'bootstrap.servers': "",
        'group.id': f'basic{num}',
        'auto.offset.reset': 'earliest'})

raw_data_custom = 'raw-data-custom.json'
consumer_raw_custom.subscribe([raw_data_custom])

raw_data_basic = 'raw-data-basic.json'
consumer_raw_basic.subscribe([raw_data_basic])

custom = 'custom-transaction.json'
consumer_trade_custom.subscribe([custom])

basic = 'basic-transaction.json'
consumer_trade_basic.subscribe([basic])


while True:
    msg1 = consumer_raw_custom.poll(0)
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
        
        es.index(index=index_raw_custom, document=dict_data)

    msg2 = consumer_raw_basic.poll(0)
    if msg2 is not None:
        record2 = msg2.value().decode('utf-8')
        dict_data = json.loads(record2)
        for key, value in dict_data.items():
            if key == '@timestamp' or key == 'Date2':
                pass
            elif key == 'IsBull2' or key == 'IsBullishEngulfing2':
                dict_data[key] = int(value)
            else:
                dict_data[key] = float(value)
        
        es.index(index=index_raw_basic, document=dict_data)

    msg3 = consumer_trade_custom.poll(0)
    if msg3 is not None:
        record3 = msg3.value().decode('utf-8')
        dict_data = json.loads(record3)
        for key, value in dict_data.items():
            if key == 'Price' or key == 'PnL' or key == 'Amount' or key == 'Total':
                dict_data[key] = float(value)
        
        es.index(index=index_trade_custom, document=dict_data)

    msg4 = consumer_trade_basic.poll(0)
    if msg4 is not None:
        record4 = msg4.value().decode('utf-8')
        dict_data = json.loads(record4)
        for key, value in dict_data.items():
            if key == 'Price2' or key == 'PnL2' or key == 'Amount2' or key == 'Total2':
                dict_data[key] = float(value)
        
        es.index(index=index_trade_basic, document=dict_data)

    

