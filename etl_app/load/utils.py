from time import sleep
from typing import List

import requests
from backoff import on_exception, expo
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from etl_app.logger import logger
from etl_app.transfrom.transform_dataclasses import Movie


@on_exception(expo, Exception)
def check_index(es_url_with_index: str) -> bool:
    """Проверка наличия индекса в Elasticsearch.

    :param es_url_with_index: Ссылка на индекс в Elasticsearch.
    :return: Вернуть True, если запрос индекса вернул код 200.
    """
    logger.info(f'-> Проверка индекса {es_url_with_index}')
    r = requests.get(es_url_with_index)
    logger.info(f' <- Проверка индекса завершилась с кодом {r.status_code}')
    return r.status_code == 200


@on_exception(expo, Exception)
def create_index_movie(es_url_with_index: str, es_index_file: str, es_index_timeout: int) -> None:
    """Проверка наличия индекса в Elasticsearch или его создание, если индекс не найден.

    :param es_url_with_index: Ссылка на индекс в Elasticsearch.
    :param es_index_file: Ссылка на файл, где лежит схема индекса.
    :param es_index_timeout: Длительность ожидания в секундах при следующей попытке создать индекс.
    """
    logger.info(f'-> Поиск индекса {es_url_with_index}')

    # Подготовка данных если индекс не был найден:
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8'}

    while not check_index(es_url_with_index):
        logger.warning(f' - Индекс не обнаружен, попытка создать {es_url_with_index}')

        # Не хочу выносить попытку чтения файла в отдельный while цикл или делать конструкцию if - while.
        # Пускай если индекс не найден в es, то произойдет попытка чтения схемы из файла.
        # Если чтение завершилось ошибкой, то сообщить об этом и уход от сваливания программы
        # через try - except. Заголовки вынес.
        try:
            with open(es_index_file, encoding='utf-8') as file:
                data = file.read()
        except Exception as err:
            logger.warning(f' - Ошибка чтение схемы индекса - {err}')

        requests.put(es_url_with_index, data=data, headers=headers)

        logger.warning(f' - Повторная попытка создания {es_url_with_index} через  {es_index_timeout} сек')
        sleep(es_index_timeout)

    logger.info(f' <- Индекс {es_url_with_index} доступен')


def get_prepared_data(batch_data: List[Movie]) -> List[dict]:
    """Подготовка данных для вставки в Elasticsearch.

    :param batch_data:  Список сгруппированных данных по фильмам.
    :return: Список словарей сгруппированных данных по фильмам для bulk вставки в Elasticsearch.
    """
    logger.info(f' -> Подготовка данных для загрузки в elastic')
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
    logger.info(f' <- Данные подготовлены для загрузки в elastic')
    return prepared_data


@on_exception(expo, Exception)
def insert_data_to_elastic(bulk_data: List[dict], es_url: str) -> None:
    """Вставка данных в Elasticsearch.

    :param bulk_data:  Список подготовленных данных для вставки в Elasticsearch.
    :param es_url:  Ссылка для bulk вставки в Elasticsearch.
    """
    logger.info(f' -> Загрузка данных в elastic')
    es = Elasticsearch(es_url)
    response = bulk(es, bulk_data)
    logger.info(f' <- Данные загружены в elastic с response: {response}')
