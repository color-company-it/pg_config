import logging.config

import psycopg2

from pg_config.configuration import _handle_configuration

_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default_formatter": {
            "format": "[PG_CONFIG %(levelname)s:%(asctime)s] %(message)s"
        },
    },
    "handlers": {
        "stream_handler": {
            "class": "logging.StreamHandler",
            "formatter": "default_formatter",
        },
    },
    "loggers": {
        "pg_config": {
            "handlers": ["stream_handler"],
            "level": "INFO",
            "propagate": True,
        }
    },
}

logging.config.dictConfig(_LOGGING_CONFIG)
_logger = logging.getLogger("pg_config")


class PgConfig:
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        port: str,
        config: [str, dict],
    ):
        self._connection: psycopg2.connect = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )

        self._configuration = _handle_configuration(configuration=config)

    def _get_tables(self) -> list:
        cur = self._connection.cursor()
        query = "SELECT distinct table_name FROM information_schema.tables;"
        _logger.info(query)
        cur.execute(query)
        response = cur.fetchall()
        cur.close()
        return [table[0] for table in response]

    def _get_columns(self, table_name: str):
        cur = self._connection.cursor()
        query = f"SELECT distinct column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
        _logger.info(query)
        cur.execute(query)
        response = cur.fetchall()
        cur.close()
        columns = {}
        for col in response:
            columns[col[0]] = col[1]
        return columns


