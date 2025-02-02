import duckdb


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_pums_table(db_path: str) -> None:
    conn = get_conn(db_path)
    conn.sql("DROP SEQUENCE IF EXISTS pums_sequence;")
    conn.sql("CREATE SEQUENCE pums_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "pumstable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('pums_sequence'),
            year INTEGER,
            agep INTEGER,
            sch INTEGER,
            schl INTEGER,
            hincp INTEGER,
            pwgtp INTEGER
        );
        """
    )


def init_gni_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.sql("DROP SEQUENCE IF EXISTS gni_sequence;")
    conn.sql("CREATE SEQUENCE gni_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "gnitable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('gni_sequence'),
            year INTEGER,
            constant FLOAT,
            capita FLOAT,
            life_exp FLOAT
        );
        """
    )
