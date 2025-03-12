from fastapi import FastAPI
from pydantic import BaseModel
import hazelcast 

app = FastAPI()

class LogMessage(BaseModel):
    UUID: str
    msg: str

client = hazelcast.HazelcastClient(cluster_members = [
    "hazelcast1:5701",
    "hazelcast2:5701",
    "hazelcast3:5701"
])
logs_map = client.get_map("logging-map").blocking()

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