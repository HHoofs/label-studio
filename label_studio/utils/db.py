import contextlib

import psycopg2
from flask import g
from psycopg2.extras import DictCursor


class DBConnection:
    """
    Database connection object for creating a quick database connection
    """
    def __init__(self, host, dbname, user, password, schema, port):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.schema = schema
        self.port = port
        self._connection = None

    @contextlib.contextmanager
    def cursor(self, **kwargs):
        """
        :return:.
        """
        cursor = self.connection.cursor(**kwargs)
        cursor.execute("SET search_path TO %s", (self.schema,))

        yield cursor

        self.connection.commit()

    @property
    def connection(self):
        if self._connection is None:
            self._connection = psycopg2.connect(host=self.host, dbname=self.dbname, user=self.user,
                                                password=self.password, port=self.port,
                                                cursor_factory=DictCursor)
        return self._connection


def connect(host, dbname, user, password, port=5432, schema='development', **kwargs):
    """
    Creates a database connection

    :return: Database connection
    :rtype DBConnection
    """
    return DBConnection(host=host, dbname=dbname, user=user, password=password,
                        schema=schema, port=port)


def get_db(config) -> None:
    """
    checks if database is in global, if not set it
    """
    if 'db' not in g:
        g.db = connect(**config)
