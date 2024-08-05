import logging
import pprint

from pydantic import ValidationError

from .models import Movie, Person


class DataTransform:
    """
    данные преобразуются из формата Postgres в формат, пригодный для Elasticsearch.
    тот этап можно пропустить, если преобразования не требуется.
    """
    def __init__(self):
        self.logger = logging.getLogger('data_transform')

    def transform_movies_pgdata_to_esdata(self, raw_data: list[dict]):
        """Данные преобразуются из формата Postgres в формат, пригодный для Elasticsearch"""
        data_to_transfer = []
        schema = {}
        for dict_ in raw_data:
            role = dict_['role']
            id_ = schema.setdefault('id', str(dict_['fw_id']))

            if id_ == str(dict_['fw_id']):
                self.transform_raw_dict_for_movies(schema, role, dict_)
            else:
                try:
                    data_to_transfer.append(Movie(**schema))
                except ValidationError as err:
                    self.logger.exception(err)
                schema = {}
                self.transform_raw_dict_for_movies(schema, role, dict_)

        try:
            data_to_transfer.append(Movie(**schema))
        except ValidationError as err:
            self.logger.exception(err)
        return data_to_transfer

    def transform_persons_pgdata_to_esdata(self, raw_data: list[dict]):
        """
        Данные преобразуются из формата Postgres в формат, пригодный для Elasticsearch

        person_schema: {id: 22, full_name: George Lucas, films: [{id: 123, roles: 'actor', 'writer'}, ...]}
        film_schema: {id: 123, roles: 'actor', 'writer'}
        """
        data_to_transfer = []
        person_schema = {}
        film_schema = {}
        role_set = set()

        for dict_ in raw_data:
            # pprint.pprint(dict_)
            # print()
            # print(f"person_id: {dict_['person_id']}")
            # films_list = person_schema.setdefault('films', [])
            # print(f'films_list: {films_list}')
            person_schema.setdefault('full_name', dict_['full_name'])
            person_id = person_schema.setdefault('id', str(dict_['person_id']))

            if person_id == str(dict_['person_id']):
                films_list = person_schema.setdefault('films', [])

                film_id = film_schema.setdefault('id', str(dict_['film_id']))
                if film_id == str(dict_['film_id']):
                    role_set.add(dict_['role'])
                    film_schema.update({'roles': [role for role in role_set]})  # {id: 123, roles: ['actor', 'writer']}
                else:
                    if film_schema not in films_list:
                        films_list.append(film_schema)
                    role_set = set()
                    film_schema = {}
                    film_schema.setdefault('id', str(dict_['film_id']))
                    role_set.add(dict_['role'])
                    film_schema.update({'roles': [role for role in role_set]})
                    # print('film_schema', film_schema)
            else:
                # print(111, films_list)
                if film_schema not in films_list:
                    films_list.append(film_schema)
                # print(222, films_list)
                # print(f'films_list: {films_list}')
                person_schema.update({'films': films_list})
                # print(f'person_schema: {person_schema}')
                # data_to_transfer.append(person_schema)
                data_to_transfer.append(Person(**person_schema))
                person_schema = {}
                film_schema = {}
                role_set = set()

                # person_schema.setdefault('full_name', dict_['full_name'])

                film_id = film_schema.setdefault('id', str(dict_['film_id']))  # film_schema = {id: 123}

                if film_id == str(dict_['film_id']):
                    role_set.add(dict_['role'])
                    film_schema.update({'roles': [role for role in role_set]})  # {id: 123, roles: ['actor']}
                    # print(f'film_schema: {film_schema}')


                # try:
                #     data_to_transfer.append(Movie(**schema_person))
                # except ValidationError as err:
                #     self.logger.exception(err)
                # schema_person = {}
                # self.transform_raw_dict_for_persons(schema_person, dict_)
        # print('----------------------------------------------')
        # for i in data_to_transfer[:10]:
        #     print()
        #     pprint.pprint(i)
        # try:
        #     data_to_transfer.append(Person(**person_schema))
        # except ValidationError as err:
        #     self.logger.exception(err)
        return data_to_transfer

    @staticmethod
    def transform_raw_dict_for_movies(schema: dict, role: str, raw_dict: dict) -> None:
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

    @staticmethod
    def transform_raw_dict_for_persons(schema: dict, schema_film: dict, raw_dict: dict) -> None:
        schema.setdefault('id', str(raw_dict['person_id']))
        schema.setdefault('full_name', raw_dict['full_name'])
        schema.setdefault('films', [])

        value = {'id': str(raw_dict['id']), 'name': raw_dict['full_name']}  # for directors/actors/writers
        value_name = value['name']  # for directors_names/actors_names/writers_names
        genre_value = {'id': str(raw_dict['g_id']), 'name': raw_dict['name'], 'description': raw_dict['g_description']}

        films_value = {'id': str(raw_dict['film_id']), 'roles': []}  # ['id': 123, 'roles': []]

        films_list = schema.setdefault('films', [films_value])  # {'id': 123, 'roles': []}
        if films_value not in films_list:
            films_list.append(genre_value)  # {'id': 123, 'roles': ['actor']}
            roles = []
            schema.update({'films': films_list})
