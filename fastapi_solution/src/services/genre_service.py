import logging
from typing import Optional
from functools import lru_cache

from pydantic import ValidationError

from ..db.elastic import get_elastic
from ..db.redis import get_redis
from ..models.models import Genre

from fastapi import Depends
from redis.asyncio import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5

get_genres_query = {
    "size": 0,
    "aggs": {
        "genres": {
            "nested": {
                "path": "genres"
            },
            "aggs": {
                "unique_genres": {
                    "terms": {
                        "field": "genres.id",
                        "size": 10000
                    },
                    "aggs": {
                        "genre_details": {
                            "top_hits": {
                                "_source": {
                                    "includes": ["genres.id", "genres.name", "genres.description"]
                                },
                                "size": 1
                            }
                        }
                    }
                }
            }
        }
    }
}


class GenreService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self.index = 'movies'
        self.log = logging.getLogger('main')

    async def get_by_id(self, genre_id: str) -> Optional[Genre]:
        genre = await self._genre_from_cache(genre_id)

        if not genre:
            genre = await self._get_from_elastic_by_id(genre_id)
            if not genre:
                return None
            await self._put_genre_to_cache(genre)

        return genre

    async def get_all_genres(self) -> Optional[list[Genre]]:
        genres = await self._all_genres_from_cache()

        if not genres:
            genres = await self._get_from_elastic_all_genres()
            if not genres:
                return None
            await self._put_all_genres_to_cache(genres)

        return genres

    async def _get_from_elastic_by_id(self, genre_id: str) -> Optional[Genre]:
        try:
            get_genre_by_id_query = {
                "query": {
                    "nested": {
                        "path": "genres",
                        "query": {
                            "term": {
                                "genres.id": genre_id
                            }
                        }
                    }
                }
            }

            docs = await self.elastic.search(index=self.index, body=get_genre_by_id_query)

            if docs['hits']['total']['value'] > 0:
                for hit in docs['hits']['hits']:
                    for genre in hit['_source'].get('genres', []):
                        if genre['id'] == genre_id:
                            return Genre(
                                id=genre['id'],
                                name=genre['name'],
                                description=genre.get('description')
                            )

        except NotFoundError:
            return None
        return None

    async def _get_from_elastic_all_genres(self) -> Optional[list[Genre]]:
        try:
            docs = await self.elastic.search(index=self.index, body=get_genres_query)

            genres_list = []

            buckets = docs['aggregations']['genres']['unique_genres']['buckets']
            for bucket in buckets:
                if bucket['doc_count'] > 0:
                    genre_hits = bucket['genre_details']['hits']['hits']
                    if genre_hits:
                        genre_data = genre_hits[0]['_source']
                        genre = Genre(
                            id=genre_data['id'],
                            name=genre_data['name'],
                            description=genre_data['description']
                        )
                        genres_list.append(genre)
        except NotFoundError:
            return None
        return genres_list

    async def _genre_from_cache(self, genre_id: str) -> Optional[Genre]:
        data = await self.redis.get(genre_id)
        self.log.info(f'redis: {data}')
        if not data:
            return None

        genre = Genre.parse_raw(data)
        return genre

    async def _all_genres_from_cache(self):
        keys = await self.redis.keys('*')
        if not keys:
            return None

        data = await self.redis.mget(keys)

        genres = []
        for item in data:
            if item is not None:
                try:
                    genre = Genre.parse_raw(item)
                    genres.append(genre)
                except ValidationError as e:
                    self.log.error(f'Ошибка при парсинге жанра из данных: {item}. Ошибка: {e}')

        self.log.info(f'redis: get {len(genres)} genres')
        return genres if genres else None

    async def _put_genre_to_cache(self, genre: Genre):
        await self.redis.set(genre.id, genre.json(), FILM_CACHE_EXPIRE_IN_SECONDS)

    async def _put_all_genres_to_cache(self, genres: list[Genre]):
        data = {genre.id: genre.json() for genre in genres}
        await self.redis.mset(data)


@lru_cache()
def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
