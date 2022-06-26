import logging.config
from difflib import SequenceMatcher

import psycopg2

from pg_config.configuration import _handle_configuration, _sort_dict
from pg_config.mappers import phrases

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
        config: [str, dict] = None,
    ):
        self._connection: psycopg2.connect = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )

        self._configuration = _handle_configuration(configuration=config)

    def get_tables(self) -> list:
        cur = self._connection.cursor()
        query = "SELECT distinct table_name FROM information_schema.tables;"
        _logger.info(query)
        cur.execute(query)
        response = cur.fetchall()
        cur.close()
        return [table[0] for table in response]

    def does_table_exist(self, table_name: str) -> bool:
        cur = self._connection.cursor()
        query = f"SELECT distinct table_name FROM information_schema.tables WHERE table_name = '{table_name}';"
        _logger.info(query)
        cur.execute(query)
        response = cur.fetchall()[0][0]
        if response:
            return True
        return False

    def get_columns(self, table_name: str) -> dict:
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

    def create_table(self, table_name: str, table_schema: dict) -> bool:
        cur = self._connection.cursor()
        query_structure = [f"create table if not exists {table_name}("]
        count, total_cols = 1, len(table_schema)
        for col_name, col_constraint in table_schema.items():
            if count == total_cols:
                query_structure.append(f"{col_name} {col_constraint});")
            else:
                query_structure.append(f"{col_name} {col_constraint}, ")
            count += 1
        query = "".join(query_structure)
        _logger.info(query)
        cur.execute(query)
        self._connection.commit()
        cur.close()
        return self.does_table_exist(table_name=table_name)

    def put_data(self, table_name: str, data: dict) -> None:
        cur = self._connection.cursor()
        column_names = data.get("columns")
        column_data = data.get("data")
        for line_item in column_data:
            query_structure = [
                f"insert into {table_name} ({','.join(column_names)}) values ("
            ]
            count, length = 1, len(line_item)
            for item in line_item:
                if count == length:
                    if item in phrases:
                        query_structure.append(f"{item});")
                    else:
                        query_structure.append(f"'{item}');")
                else:
                    if item in phrases:
                        query_structure.append(f"{item},")
                    else:
                        query_structure.append(f"'{item}',")
                count += 1
            query = "".join(query_structure)
            _logger.info(query)
            cur.execute(query)
            self._connection.commit()
        cur.close()

    def check_schema_changes(
        self, table_name: str, table_schema: dict
    ) -> dict[str, bool]:
        db_schema = self.get_columns(table_name=table_name)
        db_schema = _sort_dict(data=db_schema)
        table_schema = _sort_dict(data=table_schema)

        db_names, db_types = list(db_schema.keys()), list(db_schema.values())
        table_names, table_types = list(table_schema.keys()), list(
            table_schema.values()
        )

        # test if col names are the same
        name_differences = {}
        for db_name, table_name in zip(db_names, table_names):
            if table_name != db_name:
                name_differences[table_name] = False
            else:
                name_differences[table_name] = True

        return name_differences
