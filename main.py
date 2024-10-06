from fastapi import FastAPI
from routes import stats
from database import async_session

app = FastAPI()

# Include routers
app.include_router(stats.router)

@app.get("/")
async def getHomePage():
    return "hello world"