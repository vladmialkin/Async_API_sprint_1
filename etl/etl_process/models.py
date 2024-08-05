from typing import Optional, Union, Any

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


class Person(BaseModel):
    id: str
    full_name: str
    films: Any

# {'film_id': UUID('bfe61bd9-5dfd-41ca-80ae-8eca998bc29d'),
            #  'full_name': 'John Sayles',
            #  'person_id': UUID('0031feab-8f53-412a-8f53-47098a60ac73'),
            #  'role': 'writer'}
            #
            # {'film_id': UUID('bfe61bd9-5dfd-41ca-80ae-8eca998bc29d'),
            #  'full_name': 'John Sayles',
            #  'person_id': UUID('0031feab-8f53-412a-8f53-47098a60ac73'),
            #  'role': 'director'}