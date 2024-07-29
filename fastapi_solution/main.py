from typing import List

from fastapi_solution.src.api.v1 import films
from fastapi_solution.src.core import config
from fastapi_solution.src.db import elastic, redis

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis
from elasticsearch import AsyncElasticsearch

app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
)


@app.on_event('startup')
async def startup():
    redis.redis = Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)
    elastic.es = AsyncElasticsearch(hosts=[f'http://{config.ELASTIC_HOST}:{config.ELASTIC_PORT}'])


@app.on_event('shutdown')
async def shutdown():
    await redis.redis.close()
    await elastic.es.close()


app.include_router(films.router, prefix='/api/v1/films', tags=['films'])
