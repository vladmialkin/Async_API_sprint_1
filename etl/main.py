import logging
import time
from datetime import datetime

from config.logging_config import init_logging
from etl_process.es_loader import ElasticsearchLoader
from etl_process.extract_data import PostgresExtractor
from etl_process.transform_data import DataTransform
from state.json_file_storage import JsonFileStorage
from state.state import State

if __name__ == '__main__':
    init_logging()
    logger = logging.getLogger('main')
    logger.info('Starting etl process...')

    state = State(JsonFileStorage('state_file.json'))
    es_loader = ElasticsearchLoader()
    data_transformer = DataTransform()
    pg_extractor = PostgresExtractor(es_loader, data_transformer)

    while True:
        last_updated = state.get_state('state_key') or str(datetime.min)
        logger.info(f'last_updated: {last_updated}')

        pg_extractor.fetch_movies_if_genres_changed(last_updated)
        pg_extractor.fetch_movies_if_persons_changed(last_updated)
        pg_extractor.fetch_movies_if_films_changed(last_updated)

        pg_extractor.fetch_persons_if_persons_changed(last_updated)

        state.set_state('state_key', str(datetime.now()))

        time.sleep(3600)
