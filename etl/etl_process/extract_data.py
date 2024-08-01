# спокойно переживать падение PostgreSQL;
# начинать читать с последней обработанной записи.
import logging
from typing import Tuple, Union

from .transform_data import DataTransform
from .es_loader import ElasticsearchLoader
from .backoff import backoff
from .settings import PostgresSettings

import psycopg
from psycopg.rows import dict_row

GENRE = 'genre'
FILM_WORK = 'film_work'
PERSON = 'person'
GENRE_FILM_WORK = 'genre_film_work'
PERSON_FILM_WORK = 'person_film_work'


class PostgresExtractor:
    """
    данные, полученные из Postgres, преобразуются во внутренний формат.
    """
    def __init__(self, es_loader: ElasticsearchLoader, data_transformer: DataTransform):
        self.load_data = es_loader
        self.data_transformer = data_transformer
        self.conn = None
        self.cursor = None
        self.db = None
        self.user = None
        self.password = None
        self.host = None
        self.port = None
        self.logger = logging.getLogger('postgres')

        self.init_env()
        self.set_connection_cursor()

    @staticmethod
    def get_placeholders(data: list[str]) -> str:
        return ', '.join(['%s'] * len(data))

    @backoff()
    def make_db_connection(self, dsn):
        try:
            self.logger.info('Подключаемся к Postgres...')
            connection = psycopg.connect(**dsn, row_factory=dict_row)
            self.logger.info('Соединение с Postgres установлено')
        except psycopg.OperationalError:
            connection = None
            self.logger.exception(f"Ошибка подключения к Postgres!")

        return connection

    def set_connection_cursor(self):
        dsn = {
            'dbname': self.db,
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'port': self.port
        }
        self.logger.info(dsn)
        self.conn = self.make_db_connection(dsn)
        self.cursor = self.conn.cursor()

    def init_env(self):
        settings = PostgresSettings()

        self.db = settings.db_name
        self.user = settings.db_user
        self.password = settings.db_password
        self.host = settings.db_host
        self.port = settings.db_port

    def fetch_changed_genres(self, last_updated):
        self.logger.info('Fetch from "genre" if data modified')
        genres_info = self.check_if_data_modified(last_updated, GENRE)

        if genres_info is None:
            self.logger.info('There are no modifications.')
            return

        self.logger.info('Fetched.')
        films_id, film_placeholders = self.get_changed_filmworks_id(GENRE, genres_info[0], genres_info[1])
        self.get_all_films_info(films_id, film_placeholders)

    def fetch_changed_persons(self, last_updated):
        self.logger.info('Fetch from "person" if data modified')

        persons_info = self.check_if_data_modified(last_updated, PERSON)

        if persons_info is None:
            self.logger.info('There are no modifications.')
            return

        self.logger.info('Fetched.')
        films_id, film_placeholders = self.get_changed_filmworks_id(PERSON, persons_info[0], persons_info[1])
        self.get_all_films_info(films_id, film_placeholders)

    def fetch_changed_films(self, last_updated):
        self.logger.info('Fetch from "film_work" if data modified')

        films_info = self.check_if_data_modified(last_updated, FILM_WORK)

        if films_info is None:
            self.logger.info('There are no modifications.')
            return

        self.logger.info('Fetched.')
        self.get_all_films_info(films_info[0], films_info[1])

    def check_if_data_modified(self, last_updated, table_name) -> Union[Tuple[list, str], None]:
        query = f"""
                SELECT id, modified
                FROM content.{table_name}
                WHERE modified > %s
                ORDER BY modified;
                """
        self.cursor.execute(query, (last_updated,))
        changed_rows = self.cursor.fetchall()

        if not changed_rows:
            return None

        changed_rows_id = [i.get('id') for i in changed_rows]
        genre_placeholders = self.get_placeholders(changed_rows_id)

        return changed_rows_id, genre_placeholders

    def get_changed_filmworks_id(self, table_name, changed_rows_id, placeholders) -> Tuple[list, str]:
        query = f"""
                SELECT fw.id, fw.modified
                FROM content.film_work as fw
                LEFT JOIN content.{table_name}_film_work as gfw ON gfw.film_work_id = fw.id
                WHERE gfw.{table_name}_id IN ({placeholders})
                ORDER BY fw.modified;
                """
        self.cursor.execute(query, changed_rows_id)
        changed_films_chunk = self.cursor.fetchall()
        changed_films_id = [i.get('id') for i in changed_films_chunk]
        film_placeholders = self.get_placeholders(changed_films_id)
        return changed_films_id, film_placeholders

    def get_all_films_info(self, films_id, film_placeholders) -> None:
        self.logger.info('Fetching all films information')
        query = f"""SELECT
                        fw.id as fw_id, 
                        fw.title, 
                        fw.description, 
                        fw.rating, 
                        fw.type, 
                        fw.creation_date, 
                        fw.file_path, 
                        pfw.role, 
                        p.id, 
                        p.full_name,
                        g.name,
                        g.id as g_id,
                        g.description as g_description
                    FROM content.film_work as fw
                    LEFT JOIN content.person_film_work as pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person as p ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work as gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre as g ON g.id = gfw.genre_id
                    WHERE fw.id IN ({film_placeholders})
                    ORDER BY fw_id;"""
        self.cursor.execute(query, films_id)

        while True:
            changed_films_chunk = self.cursor.fetchmany(1000)

            if not changed_films_chunk:
                break

            data_to_load = self.data_transformer.transform_pgdata_to_esdata(raw_data=changed_films_chunk)
            self.load_data.index_documents(data_to_load)
