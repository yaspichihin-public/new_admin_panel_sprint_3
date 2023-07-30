from itertools import groupby
from time import sleep
from typing import List, Any

from etl_app.config import settings
from etl_app.logger import logger
from etl_app.utils import (
    JsonFileStorage,
    State,
)
from etl_app.extract.utils import (
    get_movies_database,
    get_filmworks,
    get_filmworks_by_changed_persons,
    get_filmworks_by_changed_genre,
)
from etl_app.transfrom.transform_dataclasses import Movie
from etl_app.load.utils import (
    create_index_movie,
    get_prepared_data,
    insert_data_to_elastic,
)


def load_state(etl_state_filename: str) -> dict:
    """Загрузка состояния.

    :param etl_state_filename: Имя файла для загрузки состояния.
    :return: Возвращает словарь состояний.
    """
    logger.info(f' -> Этап получение состояния')
    storage = JsonFileStorage(etl_state_filename)
    state = State(storage)

    state_filmwork_modified = state.get_state('state_filmwork_modified')
    logger.info(f' - state_filmwork_modified: {state_filmwork_modified}')

    state_person_modified = state.get_state('state_person_modified')
    logger.info(f' - state_person_modified: {state_person_modified}')

    state_genre_modified = state.get_state('state_genre_modified')
    logger.info(f' - state_genre_modified: {state_genre_modified}')

    logger.info(f' <- Этап получение состояния')

    result = {
        'state_filmwork_modified': state_filmwork_modified,
        'state_person_modified': state_person_modified,
        'state_genre_modified': state_genre_modified,
    }
    return result


def extract(state: dict, extract_batch: int = 100) -> tuple[list[Any], dict]:
    """Загрузка данных.

    :param state: Словарь состояний для таблиц из которых извлекаем данные.
    :param extract_batch: Сколько записей собирать при изменении состояния по таблицам.
    :return: Возвращает кортеж из списка с данными из БД и словарь состояний.
             Данных в списке может быть более 100 строк т.к. изменение 1 жанра, может
             повлечь изменения более 100 фильмов, данным моментом по задаче пренебрегаем.
    """
    logger.info(f' -> Этап извлечения данных')

    # Получения объекта базы данных movies
    movies_db = get_movies_database()

    # Создание списка хранения извлеченных данных
    extracted_data = list()

    logger.info(f' - Кейс изменение записей в таблице film_work')
    filmwork_data, state_filmwork_modified = get_filmworks(
        movies_db,
        state_filmwork_modified=state.get('state_filmwork_modified'),
        batch=extract_batch
    )
    extracted_data += filmwork_data

    logger.info(f' - Кейс изменение записей в таблице person')
    person_data, state_person_modified = get_filmworks_by_changed_persons(
        movies_db,
        state_person_modified=state.get('state_person_modified'),
        batch=extract_batch
    )
    extracted_data += person_data

    logger.info(f' - Кейс изменение записей в таблице genre')
    genre_data, state_genre_modified = get_filmworks_by_changed_genre(
        movies_db,
        state_genre_modified=state.get('state_genre_modified'),
        batch=extract_batch
    )
    extracted_data += genre_data

    movies_db.close()

    state = {
        'state_filmwork_modified': state_filmwork_modified,
        'state_person_modified': state_person_modified,
        'state_genre_modified': state_genre_modified,
    }

    logger.info(f' <- Этап извлечения данных')
    return extracted_data, state


def transform(extracted_data: List[Any]) -> List[Movie]:
    """Трансформация данных.

    :param extracted_data: Список с данными из БД.
    :return: Список сгруппированных данных по фильмам в объектах Movie.
    """
    logger.info(f' -> Этап трансформации данных')

    # Список для результата трансформации
    transformed_data = list()

    # Предварительно сортируем данные для применения groupby
    data = sorted(extracted_data, key=lambda x: x.fw_id)

    # Группируем данные
    for key, group_items in groupby(data, key=lambda x: x.fw_id):
        # print(key)
        # ffaec4b6-477d-4247-add0-dbe2ad91b3dd

        # Зададим данные по умолчанию
        filmwork_id = ''
        filmwork_imdb_rating = 0.0
        filmwork_title = ''
        filmwork_description = ''
        filmwork_genre = list()
        filmwork_director = list()
        filmwork_actors_names = list()
        filmwork_writers_names = list()
        filmwork_actors = list()
        filmwork_writers = list()

        # Заполним данные из группированных строк
        for filmwork in group_items:

            # print(filmwork)
            # Record(
            #   fw_id='9d284e83-21f0-4073-aac0-4abee51193d8',
            #   title='Star Trek: Insurrection',
            #   description="While on a mission ...",
            #   rating=6.4,
            #   type='movie',
            #   created=datetime.datetime(2021, 6, 16, 20, 14, 9, 223239, tzinfo=datetime.timezone.utc),
            #   modified=datetime.datetime(2021, 6, 16, 20, 14, 9, 223256, tzinfo=datetime.timezone.utc),
            #   role='actor',
            #   id='972c86a5-16f4-432b-b9b3-54965291ddb0',
            #   full_name='Brent Spiner',
            #   name='Adventure'
            # )
            # ...

            if not filmwork_id:
                filmwork_id = filmwork.fw_id
                filmwork_imdb_rating = filmwork.rating
                filmwork_title = filmwork.title
                filmwork_description = filmwork.description

            if filmwork.name not in filmwork_genre:
                filmwork_genre.append(filmwork.name)

            if filmwork.role == 'director' and \
                    filmwork.full_name not in filmwork_director:
                filmwork_director.append(filmwork.full_name)

            elif filmwork.role == 'actor' and \
                    filmwork.id not in list(actor.get('id') for actor in filmwork_actors):
                filmwork_actors.append({'id': filmwork.id, 'name': filmwork.full_name})
                filmwork_actors_names.append(filmwork.full_name)

            elif filmwork.role == 'writer' and \
                    filmwork.id not in list(writer.get('id') for writer in filmwork_writers):
                filmwork_writers.append({'id': filmwork.id, 'name': filmwork.full_name})
                filmwork_writers_names.append(filmwork.full_name)

        # Упаковываем данные в датакласс для elastic
        filmwork_elastic = Movie(
            id=filmwork_id,
            imdb_rating=filmwork_imdb_rating,
            title=filmwork_title,
            description=filmwork_description,
            genre=filmwork_genre,
            director=filmwork_director,
            actors_names=filmwork_actors_names,
            writers_names=filmwork_writers_names,
            actors=filmwork_actors,
            writers=filmwork_writers,
        )

        transformed_data.append(filmwork_elastic)

    logger.info(f' <- Этап трансформации данных')
    return transformed_data


def load(transformed_data: List[Movie], load_batch=100) -> None:
    """Загрузка данных.

    :param transformed_data: Список сгруппированных данных по фильмам в объектах Movie.
    :param load_batch: Размер данных которые будем загружать в Elasticsearch за итерацию.
    """
    logger.info(f' -> Этап загрузки данных')

    # Создадим индекс movie если его не было
    create_index_movie(
        es_url_with_index=settings.es_url_with_index,
        es_index_file=settings.ELASTIC_INDEX_FILE,
        es_index_timeout=settings.ELASTIC_INDEX_TIMEOUT,
    )

    logger.info(f' - Загрузки данных')
    # Загрузка данных в Elasticsearch
    operations_count = len(transformed_data) // load_batch + 1
    for batch_number in range(operations_count):
        batch_data = transformed_data[load_batch * batch_number:load_batch*(batch_number+1)]
        bulk_data = get_prepared_data(batch_data)
        insert_data_to_elastic(bulk_data=bulk_data, es_url=settings.es_url)
    logger.info(f' - Загрузки данных завершена')

    logger.info(f' <- Этап загрузки данных')


def save_state(state_dict: dict) -> None:
    """Сохранение состояния.

    :param state_dict: Словарь состояний после итерации ETL.
    """
    logger.info(f' -> Этап сохранение состояния')
    try:
        storage = JsonFileStorage(settings.ETL_STATE_FILENAME)
        state = State(storage)
        state.set_state(state_dict)
        logger.info(f' - Успешное сохранение состояния')

    except Exception as err:
        logger.warning(f' - Ошибка сохранение состояния: {err}')

    logger.info(f' <- Этап сохранение состояния')


def main(timeout_sec: int = 5, extract_batch: int = 10, load_batch: int = 10) -> None:
    """Основной цикл ETL.

    :param timeout_sec: Пауза перед итерациями в секундах.
    :param extract_batch: Размер для выгрузки данных за раз.
    :param load_batch: Размер для загрузки данных за раз.
    """

    while True:

        # Получение состояния
        state = load_state(settings.ETL_STATE_FILENAME)

        # Извлекаем данные
        extracted_data, state = extract(state=state, extract_batch=extract_batch)

        # Преобразуем данные
        transformed_data = transform(extracted_data)

        # Загрузка данных
        load(transformed_data, load_batch=load_batch)

        # Сохранение состояния
        save_state(state)

        # Пауза перед следующим циклом
        logger.info(f' Пауза перед итерациями {timeout_sec} секунд')
        sleep(timeout_sec)


if __name__ == '__main__':
    main(
        timeout_sec=settings.ETL_TIMEOUT_SEC,
        extract_batch=settings.ETL_EXTRACT_BATCH,
        load_batch=settings.ETL_LOAD_BATCH,
    )
