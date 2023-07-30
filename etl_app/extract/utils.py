import psycopg2
from psycopg2.extras import NamedTupleCursor
from backoff import on_exception, expo

from etl_app.config import settings
from etl_app.extract.queries import (
    get_filmworks_query,
    get_persons_query,
    get_genres_query,
    get_filmworks_query_by_person_uuid,
    get_filmworks_query_by_genre_uuid,
    get_filmworks_additional_query_by_filmwork_uuid,
)


class PostgresDB:

    def __init__(
            self,
            user: str,
            password: str,
            host: str,
            port: str,
            database: str
    ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self._connection = None
        self._cursor = None
        self._init()

    @on_exception(expo, Exception)
    def connect(self):
        if not self._connection:
            self._connection = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
            )
            return self._connection
        return self._connection

    @on_exception(expo, Exception)
    def cursor(self):
        if not self._cursor or self._cursor.closed:
            if not self._connection:
                self.connect()
            self._cursor = self._connection.cursor(cursor_factory=NamedTupleCursor)
            return self._cursor
        return self._cursor

    @on_exception(expo, Exception)
    def execute(self, sql: str):
        try:
            self._cursor.execute(sql)
            return self._cursor.fetchall()
        except Exception as err:
            self._connection.rollback()

    @on_exception(expo, Exception)
    def close(self):
        if self._connection:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            self._connection.close()
            self._connection = None

    def _init(self):
        self.connect()
        self.cursor()


def get_movies_database():
    # Подключаемся к базе данных
    movies_db = PostgresDB(
        user=settings.POSTGRES_DB_USER,
        password=settings.POSTGRES_DB_PASS,
        host=settings.POSTGRES_DB_HOST,
        port=settings.POSTGRES_DB_PORT,
        database=settings.POSTGRES_DB_NAME,
    )
    return movies_db


def get_filmworks(
        movies_db: PostgresDB,
        state_filmwork_modified=None,
        batch=100,
):
    # Определим данные для результата
    filmworks_additional_data = list()

    # Получаем строку идентификаторов фильмов
    filmworks_query = get_filmworks_query(
        state_modified=state_filmwork_modified,
        batch=batch
    )
    filmworks_data = movies_db.execute(filmworks_query)

    if filmworks_data:
        state_filmwork_modified = max(
            modified
            for filmwork_uuid, modified
            in (data for data in filmworks_data)
        )
        filmworks_uuid_list = list(
            filmwork_uuid
            for filmwork_uuid, modified
            in (data for data in filmworks_data)
        )
        filmworks_str = "', '".join(filmworks_uuid_list)

        # Получим недостающую информацию для elasticsearch
        filmworks_additional_query = get_filmworks_additional_query_by_filmwork_uuid(filmworks_str)
        filmworks_additional_data = movies_db.execute(filmworks_additional_query)

    return filmworks_additional_data, state_filmwork_modified


def get_filmworks_by_changed_persons(
        movies_db: PostgresDB,
        state_person_modified=None,
        batch=100,
):
    # Определим данные для результата
    filmworks_additional_data = list()

    # Получаем строку идентификаторов персон
    persons_query = get_persons_query(
        state_modified=state_person_modified,
        batch=batch
    )
    persons_data = movies_db.execute(persons_query)

    if persons_data:
        state_person_modified = max(
            modified
            for person_uuid, modified
            in (data for data in persons_data)
        )
        persons_uuid_list = list(
            person_uuid
            for person_uuid, modified
            in (data for data in persons_data)
        )
        persons_str = "', '".join(persons_uuid_list)

        # Получим строку фильмов в которых присутствовали персоны
        filmworks_query = get_filmworks_query_by_person_uuid(persons_str)
        filmworks_data = movies_db.execute(filmworks_query)
        filmworks_uuid_list = list(
            filmwork_uuid
            for filmwork_uuid, modified
            in (data for data in filmworks_data)
        )
        filmworks_str = "', '".join(filmworks_uuid_list)

        # Получим недостающую информацию для elasticsearch
        filmworks_additional_query = get_filmworks_additional_query_by_filmwork_uuid(filmworks_str)
        filmworks_additional_data = movies_db.execute(filmworks_additional_query)

    return filmworks_additional_data, state_person_modified


def get_filmworks_by_changed_genre(
        movies_db: PostgresDB,
        state_genre_modified=None,
        batch=100,
):
    # Определим данные для результата
    filmworks_additional_data = list()

    # Получаем строку идентификаторов жанров
    genres_query = get_genres_query(
        state_modified=state_genre_modified,
        batch=batch
    )
    genres_data = movies_db.execute(genres_query)

    if genres_data:
        state_genre_modified = max(
            modified
            for genre_uuid, modified
            in (data for data in genres_data)
        )
        genres_uuid_list = list(
            genre_uuid
            for genre_uuid, modified
            in (data for data in genres_data)
        )
        genres_str = "', '".join(genres_uuid_list)

        # Получим строку фильмов в которых присутствовали жанры
        filmworks_query = get_filmworks_query_by_genre_uuid(genres_str)
        filmworks_data = movies_db.execute(filmworks_query)
        filmworks_uuid_list = list(
            filmwork_uuid
            for filmwork_uuid, modified
            in (data for data in filmworks_data)
        )
        filmworks_str = "', '".join(filmworks_uuid_list)

        # Получим недостающую информацию для elasticsearch
        filmworks_additional_query = get_filmworks_additional_query_by_filmwork_uuid(filmworks_str)
        filmworks_additional_data = movies_db.execute(filmworks_additional_query)

    return filmworks_additional_data, state_genre_modified
