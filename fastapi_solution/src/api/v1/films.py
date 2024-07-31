import logging
from http import HTTPStatus
from typing import Optional, Literal

from ...services.film_service import FilmService, get_film_service

from ...models.models import FilmResponseById, FilmResponseByRating
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_pagination import Page, paginate

router = APIRouter()

log = logging.getLogger('main')


@router.get('/id/{film_id}',
            response_model=FilmResponseById,
            summary="Поиск фильма по id",
            description="Получение информации по id",
            response_description="Полная информация по фильму"
            )
async def film_details(film_id: str, film_service: FilmService = Depends(get_film_service)) -> FilmResponseById:
    log.info(f'Получение информации по фильму с id: {film_id} ...')
    film = await film_service.get_by_id(film_id)

    if not film:
        log.info(f'Фильм с id: {film_id} не найден.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    log.info(f'Информация по фильму с id: {film_id} получена.')

    return FilmResponseById(id=film.id,
                            title=film.title,
                            imdb_rating=film.imdb_rating,
                            description=film.description,
                            creation_date=film.creation_date,
                            genres=film.genres,
                            actors=film.actors,
                            directors=film.directors,
                            writers=film.writers
                            )


@router.get('/rating/{film_rating}',
            summary="Поиск фильмов по рейтингу",
            description="Поиск фильмов по рейтингу",
            response_description="Название и рейтинг фильма"
            )
async def films_by_rating(film_rating: str, film_service: FilmService = Depends(get_film_service))\
        -> Page[FilmResponseByRating]:
    log.info(f'Получение фильмов с imdb_rating: {film_rating} ...')
    films_list = await film_service.get_by_rating(film_rating)

    if not films_list:
        log.info(f'Фильмы с imdb_rating: {film_rating} не найдены.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='films not found')

    films_list = [FilmResponseByRating(id=cls.id, title=cls.title, imdb_rating=cls.imdb_rating) for cls in films_list]
    log.info(f'Фильмы с imdb_rating: {film_rating} получены.')

    return paginate(films_list)


@router.get('/films/',
            summary='Список фильмов',
            description='Список фильмов с пагинацией, фильтрацией по жанрам и сортировкой по названию или рейтингу',
            response_description='Информация по фильмам'
            )
async def films(film_service: FilmService = Depends(get_film_service),
                rating: Optional[float] = Query(None),
                creation_date: Optional[str] = Query(None),
                sort_by: Optional[Literal['imdb_rating', 'creation_date']] = Query(None)
                ) -> Page[FilmResponseById]:
    log.info(f'Получение фильмов ...')
    films_list = await film_service.get_all_films()

    if not films_list:
        log.info(f'Фильмы не найдены.')
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    films_list = [FilmResponseById(id=cls.id,
                                   title=cls.title,
                                   imdb_rating=cls.imdb_rating if cls.imdb_rating is not None else 0,
                                   creation_date=cls.creation_date if cls.creation_date is not None else '2000-01-01',
                                   genres=cls.genres,
                                   description=cls.description,
                                   directors=cls.directors,
                                   actors=cls.actors,
                                   writers=cls.writers)
                  for cls in films_list]

    # Фильтрация по рейтингу
    if rating is not None:
        films_list = [film for film in films_list if film.imdb_rating >= rating]

    # Фильтрация по дате создания
    if creation_date is not None:
        films_list = [film for film in films_list if film.creation_date >= creation_date]

    # Сортировка по рейтингу или дате создания
    if sort_by is not None:
        films_list = sorted(films_list, key=lambda f: f.imdb_rating)

    log.info(f'Получено {len(films_list)} фильмов.')
    return paginate(films_list)
