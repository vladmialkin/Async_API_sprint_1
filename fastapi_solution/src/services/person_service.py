import logging
from typing import Optional
from functools import lru_cache

from ..db.elastic import get_elastic
from ..db.redis import get_redis
from ..models.models import Person

from fastapi import Depends, HTTPException
from redis.asyncio import Redis
from elasticsearch import AsyncElasticsearch, NotFoundError

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class PersonService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self.index = 'movies'
        self.log = logging.getLogger('main')

    async def get_by_id(self, person_id: str) -> Optional[Person]:
        person = await self._person_from_cache(person_id)

        if not person:
            person = await self._get_from_elastic_by_id(person_id)
            if not person:
                return None
            await self._put_person_to_cache(person)

        return person

    async def get_all_persons(self) -> Optional[list[Person]]:
        persons = await self._all_persons_from_cache()

        if not persons:
            persons = await self._get_from_elastic_all_persons()
            if not persons:
                return None
            await self._put_all_persons_to_cache(persons)

        return persons

    async def _get_from_elastic_by_id(self, person_id: str) -> Optional[Person]:
        try:
            response = await self.elastic.get(index='persons', id=person_id)
            return Person(**response["_source"])
        except Exception as e:
            print(f"Ошибка при поиске по ID: {e}")

    async def _get_from_elastic_all_persons(self) -> Optional[list[Person]]:
        try:
            response = await self.elastic.search(index="persons", body={
                "query": {
                    "match_all": {}
                }
            })
            # Возвращаем список документов
            return [Person(**hit["_source"]) for hit in response['hits']['hits']]

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Ошибка при получении всех персонажей: {str(e)}")

    async def _person_from_cache(self, person_id: str) -> Optional[Person]:
        data = await self.redis.get(f"person:{person_id}")
        self.log.info(f'redis: {data}')
        if not data:
            return None

        person = Person.parse_raw(data)
        return person

    async def _all_persons_from_cache(self):
        keys = await self.redis.keys('person:*')
        if not keys:
            return None
        data = await self.redis.mget(keys)

        persons = [Person.parse_raw(item) for item in data if item is not None]
        self.log.info(f'redis: get {len(persons)} persons')
        return persons if persons else None

    async def _put_person_to_cache(self, person: Person):
            await self.redis.set(f"person:{person.id}", person.json(), FILM_CACHE_EXPIRE_IN_SECONDS)

    async def _put_all_persons_to_cache(self, persons: list[Person]):
        data = {f"person:{person.id}": person.json() for person in persons}
        await self.redis.mset(data)


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
