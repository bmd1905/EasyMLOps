import os

import uvicorn
from fastapi import FastAPI
from loguru import logger

from src.config import settings
from src.processor import Processor
from src.schema import TopLevelSchema

app = FastAPI(
    docs_url=f"/api/{settings.V_API}/docs",
    redoc_url=f"/api/{settings.V_API}/redoc",
    openapi_url=f"/api/{settings.V_API}/openapi.json",
)


@app.get(f"/api/{settings.V_API}")
async def root():
    return {"message": f"Welcome to the collector {os.environ['HOSTNAME']}!"}


@app.post(f"/api/{settings.V_API}{settings.COLLECTOR_PATH}")
async def write_data(data: TopLevelSchema):
    try:
        processed_data = Processor(data.dict()).processed_event
        logger.info(f"Emitting event {processed_data}")
    except Exception as e:
        logger.error(f"Failed to process event: {data}, error: {e}")

    return {"message": f"Emitting event {processed_data}"}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=80)
