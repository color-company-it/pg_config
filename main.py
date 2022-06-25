from pg_config import PgConfig


def main():
    pg = PgConfig(
        dbname="rainbow_database",
        user="unicorn_user",
        password="magical_password",
        host="localhost",
        port="5433",
        config={},
    )


    data_types = set()
    tables = pg._get_tables()
    print(tables)

    for table in tables:
        columns = pg._get_columns(table_name=table)
        data_types.update(columns.values())

    print(data_types)


if __name__ == "__main__":
    main()
