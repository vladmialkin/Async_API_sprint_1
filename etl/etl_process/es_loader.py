import json
import logging
import os

from .backoff import backoff
from .settings import ElasticsearchSettings

import elasticsearch
import elastic_transport
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticsearchLoader:
    """
    забирает данные в подготовленном формате и загружает их в Elasticsearch.
    """
    def __init__(self):
        self.host = None
        self.port = None
        self.connection = None
        self.index_name = 'movies'
        self.logger = logging.getLogger('es')

        self.init_env()
        self.set_connection()
        self.create_index()

    def init_env(self):
        settings = ElasticsearchSettings()

        self.host = settings.elastic_host
        self.port = settings.elastic_port

    @backoff()
    def make_es_connection(self):
        try:
            self.logger.info('Подключение к Elasticsearch...')
            connection = Elasticsearch(f'http://{self.host}:{self.port}')
            if not connection.ping():
                connection = None
            else:
                self.logger.info('Соединение с Elasticsearch установлено')
        except elastic_transport.ConnectionError as err:
            connection = None
            self.logger.exception(f"Ошибка подключения к Elasticsearch!!!\n{err}")
        return connection

    def set_connection(self):
        self.connection = self.make_es_connection()

    def get_index_schema(self) -> dict:
        try:
            with open('/opt/etl/index.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError as err:
            self.logger.exception(err)

    def create_index(self):
        self.logger.info(f'Creating an index {self.index_name}')

        mappings = self.get_index_schema()

        try:
            response = self.connection.indices.create(index=self.index_name, body=mappings)
            if response.body['acknowledged']:
                self.logger.info(f'Index {self.index_name} created')
        except elasticsearch.BadRequestError as err:
            self.logger.exception(err)

    def generate_data(self, data: list):
        for movie in data:
            yield {
                "_index": self.index_name,
                "_id": movie.id,
                "id": movie.id,
                "imdb_rating": movie.imdb_rating,
                "creation_date": movie.creation_date,
                "file_path": movie.file_path,
                "genres": movie.genres,
                "title": movie.title,
                "description": movie.description,
                "directors_names": movie.directors_names,
                "actors_names": movie.actors_names,
                "writers_names": movie.writers_names,
                "directors": movie.directors,
                "actors": movie.actors,
                "writers": movie.writers
            }

    def index_documents(self, index_documents):
        self.logger.info('Indexing documents...')
        success, errors = None, None

        try:
            success, errors = bulk(self.connection, self.generate_data(index_documents))
        except elastic_transport.SerializationError as err:
            self.logger.exception(err)
        except elasticsearch.helpers.BulkIndexError as err:
            self.logger.exception(err)

        return success, errors
