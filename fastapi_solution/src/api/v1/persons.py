import logging
from http import HTTPStatus
from fastapi import APIRouter, Depends, HTTPException
from fastapi_pagination import Page, paginate

from ...services.person_service import PersonService, get_person_service
from ...models.models import Person

router = APIRouter()

log = logging.getLogger('main')


@router.get(
    '/{person_id}',
    response_model=Person,
    summary="Поиск персоны по id",
    description="Получение информации по id",
    response_description="Полная информация по персоне"
)
async def genre_details(
        person_id: str,
        person_service: PersonService = Depends(get_person_service)
) -> Person:
    log.info(f'Получение информации по персоне с id: {person_id} ...')
    person = await person_service.get_by_id(person_id)

    if not person:
        log.info(f'Персона с id: {person_id} не найдена.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Person not found')

    log.info(f'Информация по жанру с id: {person_id} получена.')
    return Person(
        id=person.id,
        full_name=person.full_name,
        actor=person.actor,
        writer=person.writer,
        director=person.director,

    )


@router.get(
    '',
    summary='Список персон',
    description='Список персон с пагинацией',
    response_description='Информация по персонам'
)
async def persons(person_service: PersonService = Depends(get_person_service)) -> Page[Person]:
    log.info('Получение персон ...')
    pass
