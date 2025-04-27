from fastapi import FastAPI
import json
import os
import threading
import time
import uuid
import consul
from kafka import KafkaConsumer

app = FastAPI()

CONSUL_HOST = os.environ.get('CONSUL_HOST', 'consul-server')
CONSUL_PORT = int(os.environ.get('CONSUL_PORT', 8500))
SERVICE_NAME = os.environ.get('SERVICE_NAME', 'messages-service')
SERVICE_ID = f"{SERVICE_NAME}-{uuid.uuid4()}"
SERVICE_PORT = int(os.environ.get('SERVICE_PORT', 8004))
SERVICE_INSTANCE_ID = os.environ.get('SERVICE_ID', '1')

consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)

messages = {}
consumer_thread = None

def register_service():
    hostname = os.environ.get('HOSTNAME', 'localhost')
    check = {
        "name": f"HTTP API on port {SERVICE_PORT}",
        "http": f"http://{hostname}:{SERVICE_PORT}/health",
        "interval": "15s",
        "timeout": "5s",
    }
    
    consul_client.agent.service.register(
        name=SERVICE_NAME,
        service_id=SERVICE_ID,
        address=os.environ.get('HOSTNAME', 'localhost'),
        port=SERVICE_PORT,
        check=check
    )
    print(f"Registered service {SERVICE_NAME} with ID {SERVICE_ID}")

def get_kafka_config():
    index, data = consul_client.kv.get('config/kafka/bootstrap_servers')
    kafka_servers = data['Value'].decode('utf-8') if data else 'kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092'
    
    index, data = consul_client.kv.get('config/kafka/topic')
    kafka_topic = data['Value'].decode('utf-8') if data else 'messages-topic'
    
    return {
        'bootstrap_servers': kafka_servers.split(','),
        'topic': kafka_topic
    }

def consume_messages():
    kafka_config = get_kafka_config()
    while True:
        try:
            consumer = KafkaConsumer(
                kafka_config['topic'],
                bootstrap_servers=kafka_config['bootstrap_servers'],
                group_id='messages-service-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=True
            )
            for message in consumer:
                data = message.value
                if 'UUID' in data and 'msg' in data:
                    messages[data['UUID']] = data['msg']
                    print(f"Service {SERVICE_INSTANCE_ID} received message: {data}")
        except Exception as e:
            print(f"Kafka consumer error: {e}")
            time.sleep(5)

@app.on_event("startup")
async def startup_event():
    register_service()
    global consumer_thread
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/")
async def get_messages():
    return {"service_id": SERVICE_INSTANCE_ID, "messages": messages}

@app.on_event("shutdown")
def shutdown_event():
    consul_client.agent.service.deregister(SERVICE_ID)
    print(f"Deregistered service {SERVICE_ID}")