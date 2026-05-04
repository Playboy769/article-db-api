from contextlib import contextmanager
from config import SQLITE_DB_PATH, TURSO_URL, TURSO_TOKEN


def _dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_conn():
    if TURSO_URL:
        import libsql_experimental as libsql
        conn = libsql.connect(database=TURSO_URL, auth_token=TURSO_TOKEN)
    else:
        import sqlite3
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
    conn.row_factory = _dict_factory
    return conn


@contextmanager
def conn_ctx():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_schema(sql_path: str = "init_db.sql"):
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    conn = get_conn()
    try:
        conn.executescript(sql)
        conn.commit()
    except Exception as e:
        print(f"Schema init warning: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    init_schema()
    print(f"Schema initialized")
