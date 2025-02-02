import duckdb


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_pums_table(db_path: str) -> None:
    conn = get_conn(db_path)
    conn.sql("DROP SEQUENCE IF EXISTS id_sequence;")
    conn.sql("CREATE SEQUENCE id_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "pumstable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('id_sequence'),
            year INTEGER,
            agep INTEGER,
            sch INTEGER,
            schl INTEGER,
            hincp INTEGER,
            pwgtp INTEGER
        );
        """
    )
