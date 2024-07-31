from typing import Optional

from pydantic import BaseModel


class Movie(BaseModel):
    id: str
    imdb_rating: Optional[float]
    genres: list[dict[str, Optional[str]]]
    title: str
    file_path: Optional[str]
    description: Optional[str]
    creation_date: Optional[str]
    directors_names: list
    actors_names: list
    writers_names: list
    directors: list[dict[str, str]]
    actors: list[dict[str, str]]
    writers: list[dict[str, str]]
