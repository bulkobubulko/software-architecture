from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def get_message():
    return "messages-service is not implemented yet"