from fastapi import FastAPI
import json
import os
import threading
import time
from kafka import KafkaConsumer

app = FastAPI()

kafka_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092').split(',')
kafka_topic = os.environ.get('KAFKA_TOPIC', 'messages-topic')
service_id = os.environ.get('SERVICE_ID', '1')

messages = {}

def consume_messages():    
    while True:
        try:
            consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=kafka_servers,
                group_id='messages-service-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=True
            )
            for message in consumer:
                data = message.value
                if 'UUID' in data and 'msg' in data:
                    messages[data['UUID']] = data['msg']
                    print(f"Service {service_id} received message: {data}")
        except Exception as e:
            time.sleep(5)

consumer_thread = threading.Thread(target=consume_messages, daemon=True)
consumer_thread.start()

@app.get("/")
async def get_messages():
    return {"service_id": service_id, "messages": messages}
