import inspect
from time import sleep
from typing import List, Any

from logger import logger
from utils import JsonFileStorage, State
from extract.utils import (
    get_movies_database,
    get_filmworks,
    get_filmworks_by_changed_persons,
    get_filmworks_by_changed_genre,
)
from transfrom.transform_dataclasses import Movie
from load.utils import (
    create_index_movie,
    get_prepared_data,
    insert_data_to_elastic,
)


def extract(
        state_filmwork_modified=None,
        state_person_modified=None,
        state_genre_modified=None,
        extract_batch=100,
) -> tuple[list[Any], Any, Any, Any]:
    logger.info(f'{inspect.currentframe().f_code.co_name} -> Начало этапа извлечения данных')

    # Получения объекта базы данных movies
    movies_db = get_movies_database()

    # Создание списка хранения извлеченных данных
    extracted_data = list()

    logger.info(f'{inspect.currentframe().f_code.co_name} - Кейс изменение записей в таблице film_work')
    filmwork_data, state_filmwork_modified = get_filmworks(
        movies_db,
        state_filmwork_modified=state_filmwork_modified,
        batch=extract_batch
    )
    extracted_data += filmwork_data

    logger.info(f'{inspect.currentframe().f_code.co_name} - Кейс изменение записей в таблице person')
    person_data, state_person_modified = get_filmworks_by_changed_persons(
        movies_db,
        state_person_modified=state_person_modified,
        batch=extract_batch
    )
    extracted_data += person_data

    logger.info(f'{inspect.currentframe().f_code.co_name} - Кейс изменение записей в таблице genre')
    genre_data, state_genre_modified = get_filmworks_by_changed_genre(
        movies_db,
        state_genre_modified=state_genre_modified,
        batch=extract_batch
    )
    extracted_data += genre_data

    movies_db.close()

    logger.info(f'{inspect.currentframe().f_code.co_name} <- Конец этапа извлечения данных')
    return extracted_data, state_filmwork_modified, state_person_modified, state_genre_modified


def transform(extracted_data: List[Any]) -> List[Movie]:
    logger.info(f'{inspect.currentframe().f_code.co_name} -> Начало этапа трансформации данных')

    # Список для результата трансформации
    transformed_data = list()

    # Получить уникальные идентификаторы фильмов для группировки данных
    filmwork_uuids = set(filmwork.fw_id for filmwork in extracted_data)

    # Т.к. не понятно что будет дальше по курсу пример из теории не менял
    # Поэтому нет агрегации данных на уровне SQL в extract и тут O(N**2)
    for filmwork_uuid in filmwork_uuids:

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

        for filmwork in extracted_data:

            if filmwork.fw_id == filmwork_uuid:

                # Пример записи filmwork
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

    logger.info(f'{inspect.currentframe().f_code.co_name} <- Конец этапа трансформации данных')
    return transformed_data


def load(transformed_data: List[Movie], load_batch=100) -> None:
    logger.info(f'{inspect.currentframe().f_code.co_name} -> Начало этапа загрузки данных')

    # Создадим индекс movie если его не было
    create_index_movie()

    logger.info(f'{inspect.currentframe().f_code.co_name} - Загрузки данных')
    # Загрузка данных в Elasticsearch
    operations_count = len(transformed_data) // load_batch + 1
    for batch_number in range(operations_count):
        batch_data = transformed_data[load_batch * batch_number:load_batch*(batch_number+1)]
        batch_prepared_data = get_prepared_data(batch_data)
        insert_data_to_elastic(batch_prepared_data)
    logger.info(f'{inspect.currentframe().f_code.co_name} - Загрузки данных завершена')

    logger.info(f'{inspect.currentframe().f_code.co_name} <- Конец этапа загрузки данных')


def main(state_filename: str, timeout_sec=10, extract_batch=100, load_batch=1):

    while True:

        # Получение состояния
        logger.info(f'{inspect.currentframe().f_code.co_name} -> Получение состояния')
        storage = JsonFileStorage(state_filename)
        state = State(storage)

        state_filmwork_modified = state.get_state('state_filmwork_modified')
        logger.info(f'{inspect.currentframe().f_code.co_name} - state_filmwork_modified: {state_filmwork_modified}')

        state_person_modified = state.get_state('state_person_modified')
        logger.info(f'{inspect.currentframe().f_code.co_name} - state_person_modified: {state_person_modified}')

        state_genre_modified = state.get_state('state_genre_modified')
        logger.info(f'{inspect.currentframe().f_code.co_name} - state_genre_modified: {state_genre_modified}')
        logger.info(f'{inspect.currentframe().f_code.co_name} <- Состояние получено')

        # Извлекаем данные
        extracted_data, state_filmwork_modified, state_person_modified, state_genre_modified = extract(
            state_filmwork_modified,
            state_person_modified,
            state_genre_modified,
            extract_batch=extract_batch,
        )

        # Преобразуем данные
        transformed_data = transform(extracted_data)

        # Загрузка данных
        load(
            transformed_data,
            load_batch=load_batch,
        )

        # Сохранение состояния
        logger.info(f'{inspect.currentframe().f_code.co_name} -> Сохранение состояния')
        storage = JsonFileStorage(state_filename)
        state = State(storage)

        state.set_state(
            {
                'state_filmwork_modified': state_filmwork_modified,
                'state_person_modified': state_person_modified,
                'state_genre_modified': state_genre_modified
            }
        )
        logger.info(f'{inspect.currentframe().f_code.co_name} <- Состояние сохранено')

        sleep(timeout_sec)


if __name__ == '__main__':
    main('state_storage.json', timeout_sec=3, extract_batch=100, load_batch=100)
