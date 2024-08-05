import logging
from typing import Optional
from functools import lru_cache

from ..db.elastic import get_elastic
from ..db.redis import get_redis
from ..models.models import Person

from fastapi import Depends
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
            person = await self._get_from_elastic_by_id(person)
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
            get_person_by_id_query = {
                "query": {
                    "bool": {
                        "must": {
                            "term": {
                                "actors.id": person_id
                            }
                        },
                        "must_not": {
                            "exists": {
                                "field": "actors.id"
                            }
                        }
                    }
                }
            }
            docs = await self.elastic.search(index=self.index, body=get_person_by_id_query)

            if docs['hits']['total']['value'] > 0:
                for hit in docs['hits']['hits']:
                    for actor in hit['_source'].get('actors', []):
                        if actor['id'] == person_id:
                            return Person(
                                id=actor['id'],
                                full_name=actor['name'],
                                actor=[{'id': hit['_id']}],
                                writer=None,
                                director=None
                            )
                    for director in hit['_source'].get('directors', []):
                        if director['id'] == person_id:
                            return Person(
                                id=director['id'],
                                full_name=director['name'],
                                actor=None,
                                writer=None,
                                director=[{'id': hit['_id']}]
                            )
                    for writer in hit['_source'].get('writers', []):
                        if writer['id'] == person_id:
                            return Person(
                                id=writer['id'],
                                full_name=writer['name'],
                                actor=None,
                                writer=[{'id': hit['_id']}],
                                director=None
                            )

        except NotFoundError:
            return None
        return None

    async def _get_from_elastic_all_persons(self) -> Optional[list[Person]]:
        try:
            docs = await self.elastic.search(index=self.index, body={})

            genres_list = []

            buckets = docs['aggregations']['genres']['unique_genres']['buckets']
            for bucket in buckets:
                if bucket['doc_count'] > 0:
                    genre_hits = bucket['genre_details']['hits']['hits']
                    if genre_hits:
                        genre_data = genre_hits[0]['_source']
                        genre = Person(
                            id=genre_data['id'],
                            name=genre_data['name'],
                            description=genre_data['description']
                        )
                        genres_list.append(genre)
        except NotFoundError:
            return None
        return genres_list

    async def _person_from_cache(self, person_id: str) -> Optional[Person]:
        data = await self.redis.get(person_id)
        self.log.info(f'redis: {data}')
        if not data:
            return None

        person = Person.parse_raw(data)
        return person

    async def _all_persons_from_cache(self):
        keys = await self.redis.keys('*')
        if not keys:
            return None
        data = await self.redis.mget(keys)

        persons = [Person.parse_raw(item) for item in data if item is not None]
        self.log.info(f'redis: get {len(persons)} persons')
        return persons if persons else None

    async def _put_person_to_cache(self, person: Person):
        await self.redis.set(person.id, person.json(), FILM_CACHE_EXPIRE_IN_SECONDS)

    async def _put_all_persons_to_cache(self, persons: list[Person]):
        data = {person.id: person.json() for person in persons}
        await self.redis.mset(data)


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
