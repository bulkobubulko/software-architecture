from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class LogMessage(BaseModel):
    UUID: str
    msg: str

logs = {}

@app.post("/")
async def post_message(message: LogMessage):
    # deduplication check
    if message.UUID in logs:
        return {"status": "duplicate", "msg": "Message already logged"}
    
    logs[message.UUID] = message.msg
    print(f"Logged message: {message}")
    return {"status": "logged", "msg": message.msg}

@app.get("/")
async def get_log():
    return list(logs.values())