import psycopg2
from backoff import on_exception, expo
from psycopg2.extras import NamedTupleCursor


class PostgresDB:
    """Класс работы с БД.
    Можно его будет при желании переписать на функции через try-finally и два with.
    Но пока и так сойдет.
    """

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
