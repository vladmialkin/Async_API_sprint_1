import logging
from http import HTTPStatus
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_pagination import Page, paginate

from ...models.models import FilmFullResponse, FilmResponse
from ...services.film_service import FilmService, get_film_service

router = APIRouter()

log = logging.getLogger('main')


@router.get(
    '/{film_id}',
    response_model=FilmFullResponse,
    summary="Поиск фильма по id",
    description="Получение информации по id",
    response_description="Полная информация по фильму"
)
async def film_details(film_id: str, film_service: FilmService = Depends(get_film_service)) -> FilmFullResponse:
    log.info(f'Получение информации по фильму с id: {film_id} ...')
    film = await film_service.get_by_id(film_id)

    if not film:
        log.info(f'Фильм с id: {film_id} не найден.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    log.info(f'Информация по фильму с id: {film_id} получена.')
    return FilmFullResponse(
        id=film.id,
        title=film.title,
        imdb_rating=film.imdb_rating,
        description=film.description,
        creation_date=film.creation_date,
        genres=film.genres,
        actors=film.actors,
        directors=film.directors,
        writers=film.writers
    )


@router.get(
    '',
    summary='Список фильмов',
    description='Список фильмов с пагинацией, фильтрацией по жанрам и сортировкой по названию или рейтингу',
    response_description='Информация по фильмам'
)
async def films(
        film_service: FilmService = Depends(get_film_service),
        rating: Optional[float] = Query(None),
        genre: Optional[str] = Query(None),
        creation_date: Optional[str] = Query(None),
        sort_by: Optional[Literal['imdb_rating', 'creation_date']] = Query(None)
) -> Page[FilmResponse]:
    log.info('Получение фильмов ...')
    films_list = await film_service.get_all_films()

    if not films_list:
        log.info('Фильмы не найдены.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    # Фильтрация по рейтингу.
    if rating is not None:
        films_list = [film for film in films_list if film.imdb_rating >= rating]

    # Фильтрация по жанру.
    if genre is not None:
        films_list = [film for film in films_list if genre in [genre['name'] for genre in film.genres]]

    # Фильтрация по дате создания.
    if creation_date is not None:
        films_list = [film for film in films_list if film.creation_date >= creation_date]

    # Сортировка по рейтингу или дате создания.
    if sort_by is not None:
        films_list = sorted(films_list, key=lambda f: f.imdb_rating, reverse=True)

    log.info(f'Получено {len(films_list)} фильмов.')
    return paginate(films_list)


@router.get(
    '/search/{title_search}',
    summary='Полнотекстовый поиск по фильмам',
    description='Поиск по фильмам',
    response_description='Информация по фильмам'
)
async def film_search(title_search: str, film_service: FilmService = Depends(get_film_service)) -> Page[FilmResponse]:
    log.info(f'Поиск фильмов по названию "{title_search}" ...')
    films_list = await film_service.get_by_search(title_search)

    if not films_list:
        log.info(f'Фильмы с названием "{title_search}" не найдены.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    log.info(f'Получено {len(films_list)} фильмов с названием {title_search}.')
    return paginate(films_list)
