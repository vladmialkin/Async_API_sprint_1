import logging
from typing import Optional
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

    async def get_by_rating(self, film_rating: str) -> Optional[list[FilmRequest]]:
        films = await self._get_from_elastic_by_rating(film_rating)
        if not films:
            return None
        return films

    async def get_all_films(self) -> Optional[list[FilmRequest]]:
        films = await self._get_from_elastic_all_films()
        if not films:
            return None
        return films

    async def _get_from_elastic_by_id(self, film_id: str) -> Optional[FilmRequest]:
        try:
            doc = await self.elastic.get(index=self.index, id=film_id)
        except NotFoundError:
            return None
        return FilmRequest(**doc['_source'])

    async def _get_from_elastic_by_rating(self, film_rating: str) -> Optional[list[FilmRequest]]:
        try:
            docs = await self.elastic.search(index=self.index, size=1000, query={"match": {"imdb_rating": film_rating}})
            movies_list = [FilmRequest(**dict_['_source']) for dict_ in docs['hits']['hits']]
        except NotFoundError:
            return None
        return movies_list

    async def _get_from_elastic_all_films(self) -> Optional[list[FilmRequest]]:
        try:
            docs = await self.elastic.search(index=self.index, size=1000, query={"match_all": {}})
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
        data = {i.json() for i in films}
        await self.redis.mset(data)


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
