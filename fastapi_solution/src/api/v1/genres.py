import logging
from http import HTTPStatus
from fastapi import APIRouter, Depends, HTTPException
from fastapi_pagination import Page, paginate

from ...services.genre_service import GenreService, get_genre_service
from ...models.models import Genre

router = APIRouter()

log = logging.getLogger('main')


@router.get(
    '/{genre_id}',
    response_model=Genre,
    summary="Поиск жанра по id",
    description="Получение информации по id",
    response_description="Полная информация по жанру"
)
async def genre_details(genre_id: str, genre_service: GenreService = Depends(get_genre_service)) -> Genre:
    log.info(f'Получение информации по жанру с id: {genre_id} ...')
    genre = await genre_service.get_by_id(genre_id)

    if not genre:
        log.info(f'Жанр с id: {genre_id} не найден.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')

    log.info(f'Информация по жанру с id: {genre_id} получена.')
    return Genre(
        id=genre.id,
        name=genre.name,
        description=genre.description
    )


@router.get(
    '',
    summary='Список жанров',
    description='Список жанров с пагинацией',
    response_description='Информация по жанрам'
)
async def genres(
        genre_service: GenreService = Depends(get_genre_service)) -> Page[Genre]:
    log.info('Получение жанров ...')
    genres_list = await genre_service.get_all_genres()

    if not genres_list:
        log.info('Жанры не найдены.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')

    genres_list = [
        Genre(
            id=cls.id,
            name=cls.name,
            description=cls.description
        )
        for cls in genres_list
    ]

    log.info(f'Получено {len(genres_list)} жанров.')
    return paginate(genres_list)
