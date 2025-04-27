from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uuid
import random
import json
import time
import os
import consul
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

app = FastAPI()

class Message(BaseModel):
    msg: str

CONSUL_HOST = os.environ.get('CONSUL_HOST', 'consul-server')
CONSUL_PORT = int(os.environ.get('CONSUL_PORT', 8500))
SERVICE_NAME = os.environ.get('SERVICE_NAME', 'facade-service')
SERVICE_ID = f"{SERVICE_NAME}-{uuid.uuid4()}"
SERVICE_PORT = int(os.environ.get('SERVICE_PORT', 8000))

consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)

producer = None
kafka_config = {}

def get_service_urls(service_name):
    _, services = consul_client.health.service(service_name, passing=True)
    if not services:
        raise HTTPException(status_code=503, detail=f"No healthy {service_name} instances available")
    
    urls = [f"http://{service['Service']['Address']}:{service['Service']['Port']}" 
            for service in services]
    return urls

def get_kafka_config():
    index, data = consul_client.kv.get('config/kafka/bootstrap_servers')
    kafka_servers = data['Value'].decode('utf-8') if data else 'kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092'
    
    index, data = consul_client.kv.get('config/kafka/topic')
    kafka_topic = data['Value'].decode('utf-8') if data else 'messages-topic'
    
    return {
        'bootstrap_servers': kafka_servers.split(','),
        'topic': kafka_topic
    }

def init_kafka_producer():
    global producer, kafka_config
    kafka_config = get_kafka_config()
    max_retries = 10
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            create_topic_if_not_exists()
            return True
        except Exception as e:
            print(f"Kafka connection error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
    return False

def create_topic_if_not_exists():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_config['bootstrap_servers'])
        topic_list = [
            NewTopic(
                name=kafka_config['topic'], 
                num_partitions=3,
                replication_factor=3
            )
        ]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic {kafka_config['topic']} created")
    except Exception as e:
        print(f"Topic error: {e}")

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
        address="facade-service",
        port=SERVICE_PORT,
        check=check
    )
    print(f"Registered service {SERVICE_NAME} with ID {SERVICE_ID}")

@app.on_event("startup")
async def startup_event():
    register_service()
    init_kafka_producer()

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/")
async def post_message(message: Message):
    msg_id = str(uuid.uuid4())
    data = {"UUID": msg_id, "msg": message.msg}
    
    try:
        logging_services = get_service_urls("logging-service")
        if not logging_services:
            print("No logging services available")
        else:
            logging_service = random.choice(logging_services)
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{logging_service}/", json=data)
                if response.status_code != 200:
                    print(f"Logging service error: {response.text}")
    except Exception as e:
        print(f"Logging service error: {e}")

    if producer is None:
        if not init_kafka_producer():
            raise HTTPException(status_code=503, detail="Kafka unavailable")
    try:
        future = producer.send(kafka_config['topic'], data)
        record_metadata = future.get(timeout=10)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send to Kafka: {str(e)}")

    return {"UUID": msg_id, "status": "Message logged and sent to queue"}

@app.get("/")
async def get_messages():
    try:
        logging_services = get_service_urls("logging-service")
        messages_services = get_service_urls("messages-service")
        
        logging_service_url = random.choice(logging_services)
        messages_service_url = random.choice(messages_services)
        
        async with httpx.AsyncClient() as client:
            logging_response = await client.get(f"{logging_service_url}/")
            messages_response = await client.get(f"{messages_service_url}/")
            
            return {
                "logging_messages": logging_response.json(),
                "messages_service": messages_response.json(),
                "selected_logging_service": logging_service_url,
                "selected_messages_service": messages_service_url
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("shutdown")
def shutdown_event():
    consul_client.agent.service.deregister(SERVICE_ID)
    print(f"Deregistered service {SERVICE_ID}")
    if producer:
        producer.close()