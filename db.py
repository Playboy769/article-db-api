import sqlite3
from contextlib import contextmanager

from config import SQLITE_DB_PATH


def _dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = _dict_factory
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
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
    with conn_ctx() as conn:
        conn.executescript(sql)


if __name__ == "__main__":
    init_schema()
    print(f"Schema initialized at {SQLITE_DB_PATH}")
