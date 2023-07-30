from time import sleep
import requests
import json
from backoff import on_exception, expo
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


@on_exception(expo, Exception)
def check_index(es_url_with_index: str) -> bool:
    # logger.info(f'{inspect.currentframe().f_code.co_name} -> Проверка индекса {elastic_url}:{elastic_port}/{index}')
    r = requests.get(es_url_with_index)
    # logger.info(f'{inspect.currentframe().f_code.co_name} <- Проверка индекса завершилась с кодом {r.status_code}')
    return r.status_code == 200


@on_exception(expo, Exception)
def create_index_movie(es_url_with_index: str, es_index_file: str) -> None:
    # logger.info(f'{inspect.currentframe().f_code.co_name} -> Поиск индекса {index} в {elastic_url}:{elastic_port}')

    while not check_index(es_url_with_index):
        # logger.warning(f'{inspect.currentframe().f_code.co_name} <- Индекс не обнаружен, попытка создать индекс')

        with open(es_index_file, encoding='utf-8') as file:
            data = json.load(file)

        headers = {
            'Content-Type': 'application/json',
            'charset': 'UTF-8',
        }

        requests.put(
            es_url_with_index,
            data=json.dumps(data),
            headers=headers
        )

        sleep(5)

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
def insert_data_to_elastic(bulk_data, es_url) -> None:
    es = Elasticsearch(es_url)
    response = bulk(es, bulk_data)  # response можно потом обработать
