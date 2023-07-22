"""Скрипт миграции данных из SQLite в PostgreSQL."""
import logging
import os
import sqlite3
from contextlib import contextmanager
from math import ceil

import psycopg2
from dotenv import load_dotenv
from psycopg2.errors import Error as PgError
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import DictCursor

logging.basicConfig(
    level=os.environ.get('DEBUG_LEVEL'),
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='load_data.log',
    encoding='utf-8',
)
logger = logging.getLogger('etl')


class MultipleInsertError(Exception):
    """Ошибка множественной вставки."""

    pass


# Словарь приведение имен полей и данных
# в нужный формат из SQLite в PostgreSQL
tables_transformation = {
    'film_work': {
        'column_names': {
            'id': 'id',
            'title': 'title',
            'description': 'description',
            'creation_date': 'creation_date',
            'rating': 'rating',
            'type': 'type',
            'created_at': 'created',
            'updated_at': 'modified',
        },
        'data_transformation': {
            'description': '',
        },
    },
    'person': {
        'column_names': {
            'id': 'id',
            'full_name': 'full_name',
            'created_at': 'created',
            'updated_at': 'modified',
        },
        'data_transformation': {
        },
    },
    'genre': {
        'column_names': {
            'id': 'id',
            'name': 'name',
            'description': 'description',
            'created_at': 'created',
            'updated_at': 'modified',
        },
        'data_transformation': {
            'description': '',
        },
    },
    'genre_film_work': {
        'column_names': {
            'id': 'id',
            'genre_id': 'genre_id',
            'film_work_id': 'film_work_id',
            'created_at': 'created',
        },
        'data_transformation': {
        },
    },
    'person_film_work': {
        'column_names': {
            'id': 'id',
            'person_id': 'person_id',
            'film_work_id': 'film_work_id',
            'role': 'role',
            'created_at': 'created',
        },
        'data_transformation': {
        },
    },
}


def main(
    sqlite_conn: sqlite3.Connection,
    sqlite_curs: sqlite3.Cursor,
    pg_conn: pg_connection,
    pg_curs: DictCursor,
    batch_size: int = 32,
) -> None:
    """Главная функция миграции данных из SQLite в PostgreSQL.

    Args:
        sqlite_conn (sqlite3.Connection): Подключение к БД SQLite.
        sqlite_curs (sqlite3.Connection): Курсор к БД SQLite.
        pg_conn (pg_connection): Подключение к БД PostgreSQL.
        pg_curs (DictCursor): Курсор к БД PostgreSQL.
        batch_size (int): Размер данных для множественной вставки.
    """
    sqlite_table_names = get_sqlite_tables(sqlite_curs)

    # Пройдем поочередно по таблицам из tables_field_transformation
    # Если таблица есть в sqlite_table_names,
    # то получим данные, а затем загрузим их в PostgreSQL
    # ! Порядок таблиц tables_field_transformation важен,
    # ! из-за ForeignKey.
    for table_name in tables_transformation:
        if table_name in sqlite_table_names:
            rows_count = get_rows_count(sqlite_curs, table_name)
            batches_count = get_batches_count(rows_count, batch_size)
            for batch_number in range(batches_count):
                data_batch = load_data_from_sqlite(
                    sqlite_curs,
                    table_name,
                    batch_size,
                    batch_number,
                )
                data_batch = field_transformation(table_name, data_batch)
                try:
                    multiple_insert(
                        table_name,
                        data_batch,
                        pg_conn,
                        pg_curs,
                    )
                except MultipleInsertError:
                    single_insert(
                        table_name,
                        data_batch,
                        pg_conn,
                        pg_curs,
                    )


def get_sqlite_tables(
    sqlite_curs: sqlite3.Cursor,
) -> list[str]:
    """Получение имен таблиц в SQLite.

    Args:
        sqlite_curs (sqlite3.Cursor): Курсор к БД SQLite.
    Returns:
        table_names (list[str]): Список с именами таблиц в SQLite.
    """
    sqlite_curs.execute("select tbl_name from sqlite_master where type = 'table'")
    table_names = sqlite_curs.fetchall()
    table_names = [table_name['tbl_name'] for table_name in table_names]
    return table_names


def get_rows_count(
    sqlite_curs: sqlite3.Cursor,
    table_name: str,
) -> int:
    """Получение кол-во строк в таблице.

    Args:
        sqlite_curs (sqlite3.Cursor): Курсор к БД SQLite.
        table_name (str): Имя таблицы.
    Returns:
        rows_count (int): Список с именами таблиц в SQLite.
    """
    query = f"select count(*) as qty from {table_name};"
    rows_count = dict(sqlite_curs.execute(query).fetchone()).get('qty')
    return rows_count


def get_batches_count(
    rows_count: int,
    batch_size: int,
) -> int:
    """Получим кол-во пакетных операций, которое необходимо выполнить.

    Args:
        rows_count (int): Кол-во строк в таблице.
        batch_size (int): Размер данных для множественной вставки.
    Returns:
        batches_count (int): Необходимое кол-во пакетных операций для таблицы.
    """
    batches_count = ceil(rows_count / batch_size)
    return batches_count


def load_data_from_sqlite(
    sqlite_curs: sqlite3.Cursor,
    table_name: str,
    batch_size: int,
    batch_number: int,
) -> list[sqlite3.Row]:
    """Получение порции данных из SQLite с учетом размера batch_size.

    Args:
        sqlite_curs (sqlite3.Cursor): Курсор к БД SQLite.
        table_name (str): Имя таблицы.
        batch_size (int): Размер данных для множественной вставки.
        batch_number (int): Номер пакетной операции для offset.
    Returns:
        data (list[sqlite3.Row]): Список строк размером batch_size из SQLite.
    """
    offset = batch_number * batch_size
    query = f'select * from {table_name} limit {batch_size} offset {offset};'
    data = sqlite_curs.execute(query).fetchall()
    return data


def field_transformation(
    table_name: str,
    data_batch: list[sqlite3.Row],
) -> list[dict]:
    """Преобразовыание данных к виду PostgreSQL вставки.

    Args:
        table_name (str): Имя таблицы
        data_batch (list[sqlite3.Row]): Список строк из SQLite.
    Returns:
        data (list[dict]): Список словарей подготовленных данных.
    """
    # Словарь для приведения имен колонок и данных
    transformation = tables_transformation[table_name]

    # Пройдемся по каждой строке из пачки
    for row_idx in range(len(data_batch)):

        old_row = dict(data_batch[row_idx])
        new_row = {}

        for key, value in old_row.items():

            # column_name transformation
            if key in transformation['column_names']:
                key = transformation['column_names'].get(key)

                # data transformation
                if key in transformation['data_transformation']:

                    # Приводим NULL значения из SQLite в '' для PostgreSQL
                    if value is None:
                        value = transformation['data_transformation'].get(key)

                # Добавим трансформированные данные в новую строку
                new_row[key] = value

        # Заменим в списке старую строку на новую по индексу
        data_batch[row_idx] = new_row

    return data_batch


def get_multiple_insert_query_to_postgres(
    table_name: str,
    data_batch: list[dict],
    pg_curs: DictCursor,
) -> str:
    """Подготовка запроса SQL для множественной вставки данных.

    Args:
        table_name (str): Имя таблицы.
        data_batch (list[dict]): Список словарей подготовленных данных.
        pg_curs (DictCursor): Курсор к БД PostgreSQL.
    Returns:
        sql_query (str): Подготовленный SQL запрос для множественной вставки.
    """
    column_names = tuple(data_batch[0].keys())
    # ('id', 'full_name', 'created', 'modified')

    col_count = ', '.join(['%s'] * len(column_names))
    # %s, %s, %s, %s, %s

    data = [tuple(row.values()) for row in data_batch]
    # [('7ec9b732-0f35...', ... '2021-06-16 20:14:09'), ('e3ff-cef-... 'director',..)...]

    values = ','.join(pg_curs.mogrify(f"({col_count})", item).decode() for item in data)
    # ('7ec9b732-0f35...', ... '2021-06-16 20:14:09'), ('e3ff-cef-... 'director',..)...

    sql_query = f"""
    insert into content.{table_name} ({", ".join(column_names)})
    values {values}
    on conflict (id) do nothing
    """

    return sql_query


def get_single_insert_query_to_postgres(
    table_name: str,
    data: dict,
    pg_curs: DictCursor,
) -> str:
    """Подготовка запроса SQL для одиночной вставки данных.

    Args:
        table_name (str): Имя таблицы.
        data (data): Словарь данных для вставки.
        pg_curs (DictCursor): Курсор к БД PostgreSQL.
    Returns:
        sql_query (str): Подготовленный SQL запрос для одиночной вставки.
    """
    column_names = tuple(data.keys())
    # ('id', 'full_name', 'created', 'modified')

    col_count = ', '.join(['%s'] * len(column_names))
    # %s, %s, %s, %s, %s

    data = tuple(data.values())
    values = pg_curs.mogrify(f"({col_count})", data).decode('utf-8')
    # ('7ec9b732-0f35...', ... '2021-06-16 20:14:09'), ('e3ff-cef-... 'Klein''s',..)...

    sql_query = f"""
    insert into content.{table_name} ({", ".join(column_names)})
    values {values}
    on conflict (id) do nothing
    """

    return sql_query


def multiple_insert(
    table_name: str,
    data_batch: list[dict],
    pg_conn: pg_connection,
    pg_curs: DictCursor,
) -> None:
    """Множественная вставка данных.

    Args:
        table_name (str): Имя таблицы.
        data_batch (list[dict]): Список словарей подготовленных данных.
        pg_conn (pg_connection): : Подключение к БД PostgreSQL.
        pg_curs (DictCursor): Курсор к БД PostgreSQL.
    Raises:
        MultipleInsertError: Ошибка при множественной вставке.
    """
    multiple_insert_query = get_multiple_insert_query_to_postgres(
        table_name,
        data_batch,
        pg_curs,
    )
    try:
        pg_curs.execute(multiple_insert_query)
        pg_conn.commit()
    except PgError:
        pg_conn.rollback()
        raise MultipleInsertError('Ошибка при множественной вставке.')


def single_insert(
    table_name: str,
    data_batch: list[dict],
    pg_conn: pg_connection,
    pg_curs: DictCursor,
) -> None:
    """Одиночная вставка данных.

    Если попытка вставки данных будет неудачной,
    то внести информацию об этом в log файл.

    Args:
        table_name (str): Имя таблицы.
        data_batch (list[dict]): Список словарей подготовленных данных.
        pg_conn (pg_connection): : Подключение к БД PostgreSQL.
        pg_curs (DictCursor): Курсор к БД PostgreSQL.
    """
    for data in data_batch:
        single_insert_query = get_single_insert_query_to_postgres(
            table_name,
            data,
            pg_curs,
        )
        try:
            pg_curs.execute(single_insert_query)
            pg_conn.commit()
        except PgError as err:
            pg_conn.rollback()
            message = (
                f'{err.diag.sqlstate}-'
                f'{err.diag.table_name}-'
                f'{err.diag.message_detail}'
            )
            logger.warning(message)


"""
Комментарий ревьюера:
В идеальном варианте должно быть три класса на каждый шаг ETL (extract, transform, save),
которые имеют минимальную связность (соблюдаем принцип единой ответственности из SOLID)

Мой ответ:
Так я так и старался делать:
* load_data_from_sqlite
* field_transformation
* multiple_insert или single_insert

+ обработка батчами

Если есть пример как можно лучше, буду рад посмотреть
"""


if __name__ == '__main__':

    # Загружаем данные из .env
    load_dotenv()

    # Данные для подключения к SQLite
    sqlite_database_path = os.environ.get('SQLITE_DB_NAME')

    # Данные для подключения к PostgreSQL
    pg_database_config = {
        'dbname': os.environ.get('POSTGRES_DB_NAME'),
        'user': os.environ.get('POSTGRES_DB_USER'),
        'password':  os.environ.get('POSTGRES_DB_PASS'),
        'host': os.environ.get('POSTGRES_DB_HOST'),
        'port': os.environ.get('POSTGRES_DB_PORT'),
    }

    # Подключение к Базам данных и запуск загрузки данных из SQLite в Postgres

    @contextmanager
    def sqlite_conn_context(db_path: str):
        db_conn = sqlite3.connect(db_path)
        # По-умолчанию SQLite возвращает строки в виде кортежа значений.
        # Эта строка указывает, что данные должны быть в формате «ключ-значение»
        db_conn.row_factory = sqlite3.Row
        db_curs = db_conn.cursor()
        yield db_conn, db_curs
        db_curs.close()
        db_conn.close()

    @contextmanager
    def pg_conn_context(db_config: dict):
        db_conn = psycopg2.connect(**db_config)
        db_curs = db_conn.cursor(cursor_factory=DictCursor)
        yield db_conn, db_curs
        db_curs.close()
        db_conn.close()

    with (
        sqlite_conn_context(sqlite_database_path) as (sqlite_conn, sqlite_curs),
        pg_conn_context(pg_database_config) as (pg_conn, pg_curs),
    ):
        main(sqlite_conn, sqlite_curs, pg_conn, pg_curs, batch_size=256)
