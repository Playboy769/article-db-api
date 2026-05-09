import sqlite3
from contextlib import contextmanager
from config import SQLITE_DB_PATH, TURSO_URL, TURSO_TOKEN


class _Cursor:
    """Wraps any DB-API 2.0 cursor and returns dicts from fetch calls."""
    def __init__(self, cur):
        self._c = cur

    @property
    def lastrowid(self):
        return self._c.lastrowid

    @property
    def description(self):
        return self._c.description

    def fetchone(self):
        row = self._c.fetchone()
        if row is None or not self._c.description:
            return None
        if isinstance(row, dict):
            return row
        return {col[0]: row[i] for i, col in enumerate(self._c.description)}

    def fetchall(self):
        rows = self._c.fetchall()
        if not rows or not self._c.description:
            return []
        if isinstance(rows[0], dict):
            return rows
        return [{col[0]: row[i] for i, col in enumerate(self._c.description)} for row in rows]


class _Conn:
    """Wraps a raw DB-API 2.0 connection; execute() returns _Cursor."""
    def __init__(self, raw):
        self._r = raw

    def execute(self, sql, params=()):
        return _Cursor(self._r.execute(sql, params))

    def executescript(self, sql):
        self._r.executescript(sql)

    def commit(self):
        self._r.commit()

    def rollback(self):
        try:
            self._r.rollback()
        except Exception:
            pass

    def close(self):
        self._r.close()


def _open_raw():
    if TURSO_URL:
        import libsql_experimental as libsql
        return libsql.connect(database=TURSO_URL, auth_token=TURSO_TOKEN)
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def get_conn():
    return _Conn(_open_raw())


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
    raw = _open_raw()
    try:
        raw.executescript(sql)
        try:
            raw.commit()
        except Exception:
            pass
    except Exception as e:
        print(f"Schema init warning: {e}")
    finally:
        raw.close()


if __name__ == "__main__":
    init_schema()
    print("Schema initialized")
