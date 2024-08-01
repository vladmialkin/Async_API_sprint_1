import time
import logging
from datetime import datetime

from config.logging_config import init_logging

from state.state import State
from state.json_file_storage import JsonFileStorage
from etl_process.extract_data import PostgresExtractor
from etl_process.transform_data import DataTransform
from etl_process.es_loader import ElasticsearchLoader


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

        pg_extractor.fetch_changed_genres(last_updated)
        pg_extractor.fetch_changed_persons(last_updated)
        pg_extractor.fetch_changed_films(last_updated)

        state.set_state('state_key', str(datetime.now()))

        time.sleep(3600)
