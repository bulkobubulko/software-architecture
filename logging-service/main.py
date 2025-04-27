from fastapi import FastAPI
from pydantic import BaseModel
import hazelcast
import consul
import uuid
import os

app = FastAPI()

class LogMessage(BaseModel):
    UUID: str
    msg: str

CONSUL_HOST = os.environ.get('CONSUL_HOST', 'consul-server')
CONSUL_PORT = int(os.environ.get('CONSUL_PORT', 8500))
SERVICE_NAME = os.environ.get('SERVICE_NAME', 'logging-service')
SERVICE_ID = f"{SERVICE_NAME}-{uuid.uuid4()}"
SERVICE_PORT = int(os.environ.get('SERVICE_PORT', 8001))

consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)

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
        address=hostname,
        port=SERVICE_PORT,
        check=check
    )
    print(f"Registered service {SERVICE_NAME} with ID {SERVICE_ID}")

def get_hazelcast_config():
    index, data = consul_client.kv.get('config/hazelcast/cluster_name')
    cluster_name = data['Value'].decode('utf-8') if data else 'dev'
    
    index, data = consul_client.kv.get('config/hazelcast/cluster_members')
    cluster_members = data['Value'].decode('utf-8').split(',') if data else ["hazelcast1:5701", "hazelcast2:5701", "hazelcast3:5701"]
    
    return {
        'cluster_name': cluster_name,
        'cluster_members': [member.strip() for member in cluster_members]
    }

hazelcast_config = get_hazelcast_config()
client = hazelcast.HazelcastClient(
    cluster_name=hazelcast_config['cluster_name'],
    cluster_members=hazelcast_config['cluster_members']
)
logs_map = client.get_map("logging-map").blocking()

@app.on_event("startup")
async def startup_event():
    register_service()

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/")
async def post_message(message: LogMessage):
    if logs_map.contains_key(message.UUID):
        return {"status": "duplicate", "msg": "Message already logged"}

    logs_map.put(message.UUID, message.msg)
    print(f"Logged message: {message}")
    return {"status": "logged", "msg": message.msg}

@app.get("/")
async def get_log():
    return [logs_map.get(key) for key in logs_map.key_set()]

@app.on_event("shutdown")
def shutdown_event():
    consul_client.agent.service.deregister(SERVICE_ID)
    print(f"Deregistered service {SERVICE_ID}")
    
    if client:
        client.shutdown()