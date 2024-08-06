import logging
from typing import Optional
from datetime import datetime as dt
from functools import lru_cache

from ..db.elastic import get_elastic
from ..db.redis import get_redis
from ..models.models import FilmRequest

from fastapi import Depends
from redis.asyncio import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self.index = 'movies'
        self.log = logging.getLogger('main')

    async def get_by_id(self, film_id: str) -> Optional[FilmRequest]:
        film = await self._film_from_cache(film_id)

        if not film:
            film = await self._get_from_elastic_by_id(film_id)
            if not film:
                return None
            await self._put_film_to_cache(film)

        return film

    async def get_all_films(self) -> Optional[list[FilmRequest]]:
        films = await self._get_from_elastic_all_films()

        if not films:
            return None

        return films

    async def get_by_search(self, search_text) -> Optional[list[FilmRequest]]:
        films = await self._get_from_elastic_by_search(search_text)

        if not films:
            return None

        return films

    async def _get_from_elastic_by_id(self, film_id: str) -> Optional[FilmRequest]:
        try:
            doc = await self.elastic.get(index=self.index, id=film_id)
        except NotFoundError:
            return None
        return FilmRequest(**doc['_source'])

    async def _get_from_elastic_all_films(self) -> Optional[list[FilmRequest]]:
        try:
            docs = await self.elastic.search(index=self.index, size=1000, query={"match_all": {}})
            movies_list = [
                FilmRequest(
                    id=dict_['_source']['id'],
                    title=dict_['_source']['title'],
                    imdb_rating=dict_['_source']['imdb_rating'] if dict_['_source']['imdb_rating'] is not None else 0,
                    creation_date=dict_['_source']['creation_date'] if dict_['_source']['creation_date'] is not None
                    else str(dt.min),
                    genres=dict_['_source']['genres'],
                    description=dict_['_source']['description'],
                    file_path=dict_['_source']['file_path'],
                    directors_names=dict_['_source']['directors_names'],
                    actors_names=dict_['_source']['actors_names'],
                    writers_names=dict_['_source']['writers_names'],
                    directors=dict_['_source']['directors'],
                    actors=dict_['_source']['actors'],
                    writers=dict_['_source']['writers'],
                )
                for dict_ in docs['hits']['hits']
            ]
        except NotFoundError:
            return None
        return movies_list

    async def _get_from_elastic_by_search(self, search_text) -> Optional[list[FilmRequest]]:
        try:
            docs = await self.elastic.search(index=self.index, size=1000, query={
                "match": {
                    "title": {
                        "query": search_text,
                        "fuzziness": "auto"
                    }
                }
            })
            movies_list = [FilmRequest(**dict_['_source']) for dict_ in docs['hits']['hits']]
        except NotFoundError:
            return None
        return movies_list

    async def _film_from_cache(self, film_id: str) -> Optional[FilmRequest]:
        data = await self.redis.get(film_id)
        self.log.info(f'redis: {data}')
        if not data:
            return None

        film = FilmRequest.parse_raw(data)
        return film

    async def _all_films_from_cache(self):
        keys = await self.redis.scan()
        self.log.info(f'keys: {keys}')
        data = await self.redis.keys(keys[1])
        self.log.info(f'data: {data}')

    async def _put_film_to_cache(self, film: FilmRequest):
        await self.redis.set(film.id, film.json(), FILM_CACHE_EXPIRE_IN_SECONDS)

    async def _put_all_films_to_cache(self, films):
        data = {film.id: film.json() for film in films}
        await self.redis.mset(data)


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
