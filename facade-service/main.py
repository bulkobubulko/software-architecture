from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uuid
import asyncio
import random

app = FastAPI()

class Message(BaseModel):
    msg: str

LOGGING_SERVICE_URLS = [
    "http://logging-service-1:8001",
    "http://logging-service-2:8001",
    "http://logging-service-3:8001"
]

MESSAGES_SERVICE_URL = "http://messages-service:8004"

MAX_RETRIES = 3
RETRY_DELAY = 2

async def send_with_retry(data, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(f"{random.choice(LOGGING_SERVICE_URLS)}/", json=data)
                if response.status_code == 200:
                    return response.json()
        except httpx.RequestError as e:
            print(f"Retry {attempt + 1}/{retries} failed: {e}")
        await asyncio.sleep(RETRY_DELAY)
    raise HTTPException(status_code=500, detail="Logging service failed after retries")

@app.post("/")
async def post_message(message: Message):
    msg_id = str(uuid.uuid4())
    data = {"UUID": msg_id, "msg": message.msg}
    response = await send_with_retry(data)
    return {"UUID": msg_id, "status": "Message logged"}

@app.get("/")
async def get_messages():
    try:
        async with httpx.AsyncClient() as client:
            logging_response = await client.get(f"{random.choice(LOGGING_SERVICE_URLS)}/")
            messages_response = await client.get(f"{MESSAGES_SERVICE_URL}/")

            if logging_response.status_code != 200:
                raise HTTPException(status_code=500, detail="Logging service failed")
            if messages_response.status_code != 200:
                raise HTTPException(status_code=500, detail="Mesages service failed")
            
            return f"{logging_response.json()} {messages_response.json()}"        
    except Exception as exception:
        raise HTTPException(status_code=500, detail=str(exception))