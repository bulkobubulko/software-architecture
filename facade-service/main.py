from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uuid
import random
import json
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

app = FastAPI()

class Message(BaseModel):
    msg: str

KAFKA_SERVERS = ['kafka-broker-1:9092', 'kafka-broker-2:9092', 'kafka-broker-3:9092']
KAFKA_TOPIC = 'messages-topic'

LOGGING_SERVICE_URLS = [
    "http://logging-service-1:8001",
    "http://logging-service-2:8001",
    "http://logging-service-3:8001"
]

MESSAGES_SERVICE_URLS = [
    "http://messages-service-1:8004",
    "http://messages-service-2:8004"
]

producer = None

def init_kafka_producer():
    global producer
    max_retries = 10
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
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
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS)
        topic_list = [
            NewTopic(
                name=KAFKA_TOPIC, 
                num_partitions=3,
                replication_factor=3
            )
        ]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic {KAFKA_TOPIC} created")
    except Exception as e:
        print(f"Topic error: {e}")

@app.on_event("startup")
async def startup_event():
    init_kafka_producer()

@app.post("/")
async def post_message(message: Message):
    msg_id = str(uuid.uuid4())
    data = {"UUID": msg_id, "msg": message.msg}
    try:
        async with httpx.AsyncClient() as client:
            logging_service = random.choice(LOGGING_SERVICE_URLS)
            response = await client.post(f"{logging_service}/", json=data)
            if response.status_code != 200:
                print(f"Logging service error: {response.text}")
    except Exception as e:
        print(f"Logging service error: {e}")
    if producer is None:
        if not init_kafka_producer():
            raise HTTPException(status_code=503, detail="Kafka unavailable")
    try:
        future = producer.send(KAFKA_TOPIC, data)
        record_metadata = future.get(timeout=10)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send to Kafka: {str(e)}")

    return {"UUID": msg_id, "status": "Message logged and sent to queue"}

@app.get("/")
async def get_messages():
    try:
        async with httpx.AsyncClient() as client:
            logging_service_url = random.choice(LOGGING_SERVICE_URLS)
            logging_response = await client.get(f"{logging_service_url}/")
            messages_service_url = random.choice(MESSAGES_SERVICE_URLS)
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
    if producer:
        producer.close()
