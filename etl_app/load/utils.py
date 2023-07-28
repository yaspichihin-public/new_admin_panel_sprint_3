import inspect
import os
from time import sleep
import requests
import json
from backoff import on_exception, expo
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Загрузка данных из окружения для подключения
load_dotenv()
elastic_url = os.getenv('ELASTIC_HOST')
elastic_port = os.getenv('ELASTIC_PORT')
index = os.getenv('ELASTIC_INDEX')
index_file = os.getenv('ELASTIC_INDEX_FILE')


@on_exception(expo, Exception)
def check_index(
        elastic_url=elastic_url,
        elastic_port=elastic_port,
        index=index,
):
    # logger.info(f'{inspect.currentframe().f_code.co_name} -> Проверка индекса {elastic_url}:{elastic_port}/{index}')
    r = requests.get( f'{elastic_url}:{elastic_port}/{index}')
    # logger.info(f'{inspect.currentframe().f_code.co_name} <- Проверка индекса завершилась с кодом {r.status_code}')
    return r.status_code


@on_exception(expo, Exception)
def create_index_movie(
        elastic_url=elastic_url,
        elastic_port=elastic_port,
        index=index,
):
    # logger.info(f'{inspect.currentframe().f_code.co_name} -> Поиск индекса {index} в {elastic_url}:{elastic_port}')

    while check_index(elastic_url, elastic_port, index) != 200:
        # logger.warning(f'{inspect.currentframe().f_code.co_name} <- Индекс не обнаружен, попытка создать индекс')

        with open(index_file, encoding='utf-8') as file:
            data = json.load(file)

        headers = {
            'Content-Type': 'application/json',
            'charset': 'UTF-8',
        }

        r = requests.put(
            f'{elastic_url}:{elastic_port}/{index}',
            data=json.dumps(data),
            headers=headers
        )
        sleep(1)

    # logger.info(f'{inspect.currentframe().f_code.co_name} <- Индекс {index} доступен')


def get_prepared_data(batch_data):
    prepared_data = []
    for film in batch_data:
        film_data = {
            "_index": "movies",
            "_id": film.id,
            "_source": {
                "id": film.id,
                "imdb_rating": film.imdb_rating,
                "title": film.title,
                "description": film.description,
                "genre": film.genre,
                "director": film.director,
                "actors_names": film.actors_names,
                "writers_names": film.writers_names,
                "actors": film.actors,
                "writers": film.writers,
            }
        }
        prepared_data.append(film_data)
    return prepared_data


@on_exception(expo, Exception)
def insert_data_to_elastic(
        batch_prepared_data,
        elastic_url=elastic_url,
        elastic_port=elastic_port,
):
    es = Elasticsearch(f'{elastic_url}:{elastic_port}')

    response = bulk(
        es,
        batch_prepared_data
    )
