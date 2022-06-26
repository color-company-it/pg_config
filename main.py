from pg_config import PgConfig
from pg_config.readers import load_yaml


def main():
    pg = PgConfig(
        dbname="rainbow_database",
        user="unicorn_user",
        password="magical_password",
        host="localhost",
        port="5433",
    )

    test = pg.check_schema_changes(
        table_name="table_1",
        table_schema={
            "id": "integer primary key not null",
            "name": "varchar(20)",
            "created_at": "timestamp without time zone default current_date",
            "age": "integer",
            "costs": "real",
            "valid": "boolean",
        },
    )

    print(test)


if __name__ == "__main__":
    main()
