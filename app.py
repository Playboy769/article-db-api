from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from db import conn_ctx, init_schema


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_schema()
    # apply column migrations for existing databases
    with conn_ctx() as conn:
        for stmt in [
            "ALTER TABLE articles ADD COLUMN starred INTEGER DEFAULT 0",
            # links table (for databases created before links feature)
            """CREATE TABLE IF NOT EXISTS links (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                title      TEXT NOT NULL,
                url        TEXT NOT NULL,
                desc       TEXT,
                folder     TEXT DEFAULT '',
                icon       TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )""",
            "CREATE INDEX IF NOT EXISTS links_folder_idx  ON links(folder)",
            "CREATE INDEX IF NOT EXISTS links_created_idx ON links(created_at)",
        ]:
            try:
                conn.execute(stmt)
            except Exception:
                pass
    yield


app = FastAPI(title="Article DB", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- models ----------
class ArticleIn(BaseModel):
    title: str
    content: str = ""
    author: Optional[str] = None
    source: Optional[str] = None
    language: str = "zh"
    category_id: Optional[int] = None
    tags: list[str] = Field(default_factory=list)
    summary: Optional[str] = None
    date: Optional[str] = None
    images: str = "[]"
    starred: int = 0


class ArticleUpdate(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    source: Optional[str] = None
    language: Optional[str] = None
    category_id: Optional[int] = None
    tags: Optional[list[str]] = None
    summary: Optional[str] = None
    date: Optional[str] = None
    images: Optional[str] = None
    starred: Optional[int] = None


class TagIn(BaseModel):
    name: str


class CategoryIn(BaseModel):
    name: str
    parent_id: Optional[int] = None


class ReportIn(BaseModel):
    company: str
    ticker: Optional[str] = None
    sector: Optional[str] = None
    rating: Optional[str] = None
    target: Optional[str] = None
    date: Optional[str] = None
    analyst: Optional[str] = None
    source: Optional[str] = None
    content: str = ""
    images: str = "[]"


class ReportUpdate(BaseModel):
    company: Optional[str] = None
    ticker: Optional[str] = None
    sector: Optional[str] = None
    rating: Optional[str] = None
    target: Optional[str] = None
    date: Optional[str] = None
    analyst: Optional[str] = None
    source: Optional[str] = None
    content: Optional[str] = None
    images: Optional[str] = None


class LinkIn(BaseModel):
    title: str
    url: str
    desc: Optional[str] = None
    folder: str = ""
    icon: Optional[str] = None


class LinkUpdate(BaseModel):
    title: Optional[str] = None
    url: Optional[str] = None
    desc: Optional[str] = None
    folder: Optional[str] = None
    icon: Optional[str] = None


# ---------- helpers ----------
def _upsert_tags(conn, names: list[str]) -> list[int]:
    ids = []
    for raw in names:
        n = raw.strip()
        if not n:
            continue
        conn.execute(
            "INSERT INTO tags (name) VALUES (?) ON CONFLICT(name) DO NOTHING", (n,)
        )
        row = conn.execute("SELECT id FROM tags WHERE name = ?", (n,)).fetchone()
        if row:
            ids.append(row["id"])
    return ids


def _set_article_tags(conn, article_id: int, tag_ids: list[int]):
    conn.execute("DELETE FROM article_tags WHERE article_id = ?", (article_id,))
    for tid in tag_ids:
        conn.execute(
            "INSERT OR IGNORE INTO article_tags (article_id, tag_id) VALUES (?, ?)",
            (article_id, tid),
        )


def _fetch_article(conn, article_id: int) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT a.*, c.name AS category_name
        FROM articles a
        LEFT JOIN categories c ON c.id = a.category_id
        WHERE a.id = ?
        """,
        (article_id,),
    ).fetchone()
    if not row:
        return None
    tags = [
        r["name"]
        for r in conn.execute(
            """SELECT t.name FROM tags t
               JOIN article_tags at ON at.tag_id = t.id
               WHERE at.article_id = ? ORDER BY t.name""",
            (article_id,),
        ).fetchall()
    ]
    row["tags"] = tags
    return row


def _attach_tags(conn, rows: list[dict]) -> list[dict]:
    if not rows:
        return rows
    ids = [r["id"] for r in rows]
    placeholders = ",".join("?" * len(ids))
    tag_rows = conn.execute(
        f"""SELECT at.article_id, t.name
            FROM article_tags at JOIN tags t ON t.id = at.tag_id
            WHERE at.article_id IN ({placeholders})""",
        ids,
    ).fetchall()
    by_id: dict[int, list[str]] = {i: [] for i in ids}
    for tr in tag_rows:
        by_id[tr["article_id"]].append(tr["name"])
    for r in rows:
        r["tags"] = by_id.get(r["id"], [])
    return rows


# ---------- categories ----------
@app.post("/categories")
def create_category(c: CategoryIn):
    with conn_ctx() as conn:
        cur = conn.execute(
            "INSERT INTO categories (name, parent_id) VALUES (?, ?)",
            (c.name, c.parent_id),
        )
        return conn.execute(
            "SELECT * FROM categories WHERE id = ?", (cur.lastrowid,)
        ).fetchone()


@app.get("/categories")
def list_categories():
    with conn_ctx() as conn:
        rows = conn.execute(
            "SELECT * FROM categories ORDER BY parent_id IS NOT NULL, name"
        ).fetchall()
    by_id = {r["id"]: {**r, "children": []} for r in rows}
    roots = []
    for r in rows:
        node = by_id[r["id"]]
        if r["parent_id"] and r["parent_id"] in by_id:
            by_id[r["parent_id"]]["children"].append(node)
        else:
            roots.append(node)
    return roots


@app.patch("/categories/{cid}")
def update_category(cid: int, c: CategoryIn):
    with conn_ctx() as conn:
        if not conn.execute("SELECT id FROM categories WHERE id = ?", (cid,)).fetchone():
            raise HTTPException(404, "not found")
        conn.execute("UPDATE categories SET name = ? WHERE id = ?", (c.name, cid))
        return conn.execute("SELECT * FROM categories WHERE id = ?", (cid,)).fetchone()


@app.delete("/categories/{cid}")
def delete_category(cid: int):
    with conn_ctx() as conn:
        conn.execute("DELETE FROM categories WHERE id = ?", (cid,))
    return {"ok": True}


# ---------- articles CRUD ----------
@app.post("/articles")
def create_article(a: ArticleIn):
    with conn_ctx() as conn:
        cur = conn.execute(
            """INSERT INTO articles
               (title, content, author, source, language, category_id, summary, date, images, starred)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (a.title, a.content, a.author, a.source, a.language,
             a.category_id, a.summary, a.date, a.images, a.starred),
        )
        aid = cur.lastrowid
        if a.tags:
            tag_ids = _upsert_tags(conn, a.tags)
            _set_article_tags(conn, aid, tag_ids)
        return _fetch_article(conn, aid)


@app.get("/articles/{aid}")
def get_article(aid: int):
    with conn_ctx() as conn:
        row = _fetch_article(conn, aid)
    if not row:
        raise HTTPException(404, "not found")
    return row


@app.patch("/articles/{aid}")
def update_article(aid: int, u: ArticleUpdate):
    with conn_ctx() as conn:
        if not conn.execute("SELECT id FROM articles WHERE id = ?", (aid,)).fetchone():
            raise HTTPException(404, "not found")

        fields, values = [], []
        for k in ("title", "content", "author", "source", "language",
                  "category_id", "summary", "date", "images", "starred"):
            v = getattr(u, k)
            if v is not None:
                fields.append(f"{k} = ?")
                values.append(v)
        if fields:
            values.append(aid)
            conn.execute(f"UPDATE articles SET {', '.join(fields)} WHERE id = ?", values)

        if u.tags is not None:
            tag_ids = _upsert_tags(conn, u.tags)
            _set_article_tags(conn, aid, tag_ids)

        return _fetch_article(conn, aid)


@app.delete("/articles/{aid}")
def delete_article(aid: int):
    with conn_ctx() as conn:
        conn.execute("DELETE FROM articles WHERE id = ?", (aid,))
    return {"ok": True}


# ---------- articles list ----------
@app.get("/articles")
def list_articles(
    author: Optional[str] = None,
    category_id: Optional[int] = None,
    tag: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    limit: int = Query(20, le=500),
    offset: int = 0,
):
    where, params = [], []
    if author:
        where.append("a.author LIKE ?"); params.append(f"%{author}%")
    if category_id is not None:
        where.append("a.category_id = ?"); params.append(category_id)
    if date_from:
        where.append("a.created_at >= ?"); params.append(date_from.isoformat(sep=" "))
    if date_to:
        where.append("a.created_at <= ?"); params.append(date_to.isoformat(sep=" "))
    if tag:
        where.append(
            "EXISTS (SELECT 1 FROM article_tags at JOIN tags t ON t.id = at.tag_id "
            "WHERE at.article_id = a.id AND t.name = ?)"
        )
        params.append(tag)

    sql = """
        SELECT a.id, a.title, a.content, a.author, a.source, a.language, a.summary,
               a.date, a.images, a.category_id,
               c.name AS category_name, a.created_at, a.updated_at
        FROM articles a
        LEFT JOIN categories c ON c.id = a.category_id
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY a.created_at DESC LIMIT ? OFFSET ?"
    params += [limit, offset]

    with conn_ctx() as conn:
        rows = conn.execute(sql, params).fetchall()
        return _attach_tags(conn, rows)


# ---------- search ----------
def _fts_quote(q: str) -> str:
    return '"' + q.replace('"', '""') + '"'


@app.get("/search")
def search(q: str, limit: int = Query(20, le=100)):
    with conn_ctx() as conn:
        try:
            rows = conn.execute(
                """
                SELECT a.id, a.title, a.content, a.author, a.source, a.language, a.summary,
                       a.date, a.images, a.category_id,
                       c.name AS category_name, a.created_at,
                       bm25(articles_fts) AS score
                FROM articles_fts
                JOIN articles a ON a.id = articles_fts.rowid
                LEFT JOIN categories c ON c.id = a.category_id
                WHERE articles_fts MATCH ?
                ORDER BY score
                LIMIT ?
                """,
                (_fts_quote(q), limit),
            ).fetchall()
        except Exception:
            rows = []

        if not rows:
            like = f"%{q}%"
            rows = conn.execute(
                """
                SELECT a.id, a.title, a.content, a.author, a.source, a.language, a.summary,
                       a.date, a.images, a.category_id,
                       c.name AS category_name, a.created_at,
                       NULL AS score
                FROM articles a
                LEFT JOIN categories c ON c.id = a.category_id
                WHERE a.title LIKE ? OR a.content LIKE ? OR a.author LIKE ?
                ORDER BY a.created_at DESC
                LIMIT ?
                """,
                (like, like, like, limit),
            ).fetchall()

        return _attach_tags(conn, rows)


# ---------- reports CRUD ----------
@app.post("/reports")
def create_report(r: ReportIn):
    with conn_ctx() as conn:
        cur = conn.execute(
            """INSERT INTO reports
               (company, ticker, sector, rating, target, date, analyst, source, content, images)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (r.company, r.ticker, r.sector, r.rating, r.target,
             r.date, r.analyst, r.source, r.content, r.images),
        )
        return conn.execute(
            "SELECT * FROM reports WHERE id = ?", (cur.lastrowid,)
        ).fetchone()


@app.get("/reports")
def list_reports(
    sector: Optional[str] = None,
    limit: int = Query(200, le=500),
    offset: int = 0,
):
    where, params = [], []
    if sector:
        where.append("sector = ?"); params.append(sector)
    sql = "SELECT * FROM reports"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params += [limit, offset]
    with conn_ctx() as conn:
        return conn.execute(sql, params).fetchall()


@app.get("/reports/{rid}")
def get_report(rid: int):
    with conn_ctx() as conn:
        row = conn.execute("SELECT * FROM reports WHERE id = ?", (rid,)).fetchone()
    if not row:
        raise HTTPException(404, "not found")
    return row


@app.patch("/reports/{rid}")
def update_report(rid: int, u: ReportUpdate):
    with conn_ctx() as conn:
        if not conn.execute("SELECT id FROM reports WHERE id = ?", (rid,)).fetchone():
            raise HTTPException(404, "not found")
        fields, values = [], []
        for k in ("company", "ticker", "sector", "rating", "target",
                  "date", "analyst", "source", "content", "images"):
            v = getattr(u, k)
            if v is not None:
                fields.append(f"{k} = ?")
                values.append(v)
        if fields:
            values.append(rid)
            conn.execute(f"UPDATE reports SET {', '.join(fields)} WHERE id = ?", values)
        return conn.execute("SELECT * FROM reports WHERE id = ?", (rid,)).fetchone()


@app.delete("/reports/{rid}")
def delete_report(rid: int):
    with conn_ctx() as conn:
        conn.execute("DELETE FROM reports WHERE id = ?", (rid,))
    return {"ok": True}


# ---------- tags ----------
@app.get("/tags")
def list_tags():
    with conn_ctx() as conn:
        rows = conn.execute(
            """SELECT t.id, t.name, COUNT(at.article_id) AS count
               FROM tags t
               LEFT JOIN article_tags at ON at.tag_id = t.id
               GROUP BY t.id ORDER BY t.name"""
        ).fetchall()
    return rows


@app.patch("/tags/{tid}")
def update_tag(tid: int, t: TagIn):
    with conn_ctx() as conn:
        if not conn.execute("SELECT id FROM tags WHERE id = ?", (tid,)).fetchone():
            raise HTTPException(404, "not found")
        conn.execute("UPDATE tags SET name = ? WHERE id = ?", (t.name, tid))
        return conn.execute("SELECT * FROM tags WHERE id = ?", (tid,)).fetchone()


@app.delete("/tags/{tid}")
def delete_tag(tid: int):
    with conn_ctx() as conn:
        conn.execute("DELETE FROM tags WHERE id = ?", (tid,))
    return {"ok": True}


@app.get("/healthz")
def health():
    return {"ok": True}


# ---------- links CRUD ----------
@app.post("/links")
def create_link(lk: LinkIn):
    with conn_ctx() as conn:
        cur = conn.execute(
            "INSERT INTO links (title, url, desc, folder, icon) VALUES (?, ?, ?, ?, ?)",
            (lk.title, lk.url, lk.desc, lk.folder, lk.icon),
        )
        return conn.execute("SELECT * FROM links WHERE id = ?", (cur.lastrowid,)).fetchone()


@app.get("/links")
def list_links(
    folder: Optional[str] = None,
    limit: int = Query(500, le=1000),
    offset: int = 0,
):
    where, params = [], []
    if folder is not None:
        where.append("folder = ?"); params.append(folder)
    sql = "SELECT * FROM links"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params += [limit, offset]
    with conn_ctx() as conn:
        return conn.execute(sql, params).fetchall()


@app.patch("/links/{lid}")
def update_link(lid: int, lk: LinkUpdate):
    with conn_ctx() as conn:
        if not conn.execute("SELECT id FROM links WHERE id = ?", (lid,)).fetchone():
            raise HTTPException(404, "not found")
        fields, values = [], []
        for k in ("title", "url", "desc", "folder", "icon"):
            v = getattr(lk, k)
            if v is not None:
                fields.append(f"{k} = ?")
                values.append(v)
        if fields:
            values.append(lid)
            conn.execute(f"UPDATE links SET {', '.join(fields)} WHERE id = ?", values)
        return conn.execute("SELECT * FROM links WHERE id = ?", (lid,)).fetchone()


@app.delete("/links/{lid}")
def delete_link_api(lid: int):
    with conn_ctx() as conn:
        conn.execute("DELETE FROM links WHERE id = ?", (lid,))
    return {"ok": True}
