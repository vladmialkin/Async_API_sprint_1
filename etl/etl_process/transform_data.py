import logging

from pydantic import ValidationError

from .models import Movie


class DataTransform:
    """
    данные преобразуются из формата Postgres в формат, пригодный для Elasticsearch.
    тот этап можно пропустить, если преобразования не требуется.
    """
    def __init__(self):
        self.logger = logging.getLogger('data_transform')

    def transform_pgdata_to_esdata(self, raw_data: list[dict]):
        """данные преобразуются из формата Postgres в формат, пригодный для Elasticsearch"""
        data_to_transfer = []
        schema = {}
        for raw_dict in raw_data:
            role = raw_dict['role']
            id_ = schema.setdefault('id', str(raw_dict['fw_id']))

            if id_ == str(raw_dict['fw_id']):
                self.transform_raw_dict(schema, role, raw_dict)
            else:
                try:
                    data_to_transfer.append(Movie(**schema))
                except ValidationError as err:
                    self.logger.exception(err)
                schema = {}
                self.transform_raw_dict(schema, role, raw_dict)

        try:
            data_to_transfer.append(Movie(**schema))
        except ValidationError as err:
            self.logger.exception(err)
        return data_to_transfer

    @staticmethod
    def transform_raw_dict(schema: dict, role: str, raw_dict: dict) -> None:
        schema.setdefault('id', str(raw_dict['fw_id']))
        schema.setdefault('imdb_rating', raw_dict['rating'])
        schema.setdefault('title', raw_dict['title'])
        schema.setdefault('creation_date', raw_dict['creation_date'])
        schema.setdefault('description', raw_dict['description'])
        schema.setdefault('file_path', raw_dict['file_path'])
        schema.setdefault('genres', [])
        schema.setdefault('directors_names', [])
        schema.setdefault('actors_names', [])
        schema.setdefault('writers_names', [])
        schema.setdefault('directors', [])
        schema.setdefault('actors', [])
        schema.setdefault('writers', [])

        value = {'id': str(raw_dict['id']), 'name': raw_dict['full_name']}  # for directors/actors/writers
        value_name = value['name']  # for directors_names/actors_names/writers_names
        genre_value = {'id': str(raw_dict['g_id']), 'name': raw_dict['name'], 'description': raw_dict['g_description']}

        genres_list = schema.setdefault('genres', [genre_value])
        if genre_value not in genres_list:
            genres_list.append(genre_value)
            schema.update({'genres': genres_list})

        if role == 'director':
            directors_list = schema.setdefault('directors', [value])
            directors_names_list = schema.setdefault('directors_names', [value_name])
            if value not in directors_list:
                directors_list.append(value)
                directors_names_list.append(value_name)
                schema.update({'directors': directors_list})
                schema.update({'directors_names': directors_names_list})

        if role == 'actor':
            actors_list = schema.setdefault('actors', [value])
            actors_names_list = schema.setdefault('actors_names', [value_name])
            if value not in actors_list:
                actors_list.append(value)
                actors_names_list.append(value_name)
                schema.update({'actors': actors_list})
                schema.update({'actors_names': actors_names_list})

        if role == 'writer':
            writers_list = schema.setdefault('writers', [value])
            writers_names_list = schema.setdefault('writers_names', [value_name])
            if value not in writers_list:
                writers_list.append(value)
                writers_names_list.append(value_name)
                schema.update({'writers': writers_list})
                schema.update({'writers_names': writers_names_list})
