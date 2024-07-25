from typing import Union

from pydantic import BaseModel


class Film(BaseModel):
    id: str
    title: str
    imdb_rating: Union[float, None]
    creation_date: str
    genres: list[str]
    description: Union[str, None]
    file_path: str
    directors_names: list[str]
    actors_names: list[str]
    writers_names: list[str]
    directors: list[dict[str, str]]
    actors: list[dict[str, str]]
    writers: list[dict[str, str]]


class Genre(BaseModel):
    id: str
    name: str
    description: str


class Actor(BaseModel):
    id: str
    full_name: str


class Director(BaseModel):
    id: str
    full_name: str


class Writer(BaseModel):
    id: str
    full_name: str
