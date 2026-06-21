from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from db import conn_ctx, serialize_vec
from embeddings import embed
from config import ENABLE_AI, ENABLE_SEMANTIC
import llm

app = FastAPI(title="Article DB")
templates = Jinja2Templates(directory="templates")


def _require_ai():
    if not ENABLE_AI:
        raise HTTPException(
            403,
            "AI features disabled. Set ENABLE_AI=true in .env and provide ANTHROPIC_API_KEY to enable.",
        )


def _require_semantic():
    if not ENABLE_SEMANTIC:
        raise HTTPException(
            503,
            "Semantic search is disabled (sentence-transformers/PyTorch not installed). "
            "Use /search/keyword or /search/fuzzy instead.",
        )


# ---------- models ----------
class ArticleIn(BaseModel):
    title: str
    content: str
    author: Optional[str] = None
    language: str = "zh"
    category_id: Optional[int] = None
    tags: list[str] = Field(default_factory=list)
    summary: Optional[str] = None


class ArticleUpdate(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    language: Optional[str] = None
    category_id: Optional[int] = None
    tags: Optional[list[str]] = None
    summary: Optional[str] = None


class CategoryIn(BaseModel):
    name: str
    parent_id: Optional[int] = None


# ---------- helpers ----------
def _upsert_tags(conn, names: list[str]) -> list[int]:
    ids = []
    for raw in names:
        n = raw.strip()
        if not n:
            continue
        conn.execute(
            "INSERT INTO tags (name) VALUES (?) ON CONFLICT(name) DO NOTHING",
            (n,),
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


def _upsert_embedding(conn, article_id: int, vec: list[float]):
    conn.execute("DELETE FROM article_vec WHERE article_id = ?", (article_id,))
    conn.execute(
        "INSERT INTO article_vec (article_id, embedding) VALUES (?, ?)",
        (article_id, serialize_vec(vec)),
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
            "SELECT t.name FROM tags t JOIN article_tags at ON at.tag_id=t.id WHERE at.article_id = ? ORDER BY t.name",
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
        f"""
        SELECT at.article_id, t.name
        FROM article_tags at JOIN tags t ON t.id = at.tag_id
        WHERE at.article_id IN ({placeholders})
        """,
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
        return conn.execute("SELECT * FROM categories WHERE id = ?", (cur.lastrowid,)).fetchone()


@app.get("/categories")
def list_categories():
    with conn_ctx() as conn:
        rows = conn.execute("SELECT * FROM categories ORDER BY parent_id IS NOT NULL, name").fetchall()
    by_id = {r["id"]: {**r, "children": []} for r in rows}
    roots = []
    for r in rows:
        node = by_id[r["id"]]
        if r["parent_id"] and r["parent_id"] in by_id:
            by_id[r["parent_id"]]["children"].append(node)
        else:
            roots.append(node)
    return roots


@app.delete("/categories/{cid}")
def delete_category(cid: int):
    with conn_ctx() as conn:
        conn.execute("DELETE FROM categories WHERE id = ?", (cid,))
    return {"ok": True}


# ---------- articles CRUD ----------
@app.post("/articles")
def create_article(a: ArticleIn):
    vec = embed(f"{a.title}\n\n{a.content}") if ENABLE_SEMANTIC else None
    with conn_ctx() as conn:
        cur = conn.execute(
            """
            INSERT INTO articles (title, content, author, language, category_id, summary)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (a.title, a.content, a.author, a.language, a.category_id, a.summary),
        )
        aid = cur.lastrowid
        if vec is not None:
            _upsert_embedding(conn, aid, vec)
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
        cur_row = conn.execute(
            "SELECT title, content FROM articles WHERE id = ?", (aid,)
        ).fetchone()
        if not cur_row:
            raise HTTPException(404, "not found")

        fields, values = [], []
        for k in ("title", "content", "author", "language", "category_id", "summary"):
            v = getattr(u, k)
            if v is not None:
                fields.append(f"{k} = ?")
                values.append(v)
        if fields:
            values.append(aid)
            conn.execute(f"UPDATE articles SET {', '.join(fields)} WHERE id = ?", values)

        if ENABLE_SEMANTIC and (u.title is not None or u.content is not None):
            new_title = u.title if u.title is not None else cur_row["title"]
            new_content = u.content if u.content is not None else cur_row["content"]
            vec = embed(f"{new_title}\n\n{new_content}")
            if vec is not None:
                _upsert_embedding(conn, aid, vec)

        if u.tags is not None:
            tag_ids = _upsert_tags(conn, u.tags)
            _set_article_tags(conn, aid, tag_ids)

        return _fetch_article(conn, aid)


@app.delete("/articles/{aid}")
def delete_article(aid: int):
    with conn_ctx() as conn:
        conn.execute("DELETE FROM article_vec WHERE article_id = ?", (aid,))
        conn.execute("DELETE FROM articles WHERE id = ?", (aid,))
    return {"ok": True}


# ---------- list + advanced filters ----------
@app.get("/articles")
def list_articles(
    author: Optional[str] = None,
    language: Optional[str] = None,
    category_id: Optional[int] = None,
    tag: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    min_words: Optional[int] = None,
    max_words: Optional[int] = None,
    limit: int = Query(20, le=200),
    offset: int = 0,
):
    where, params = [], []
    if author:
        where.append("a.author LIKE ?"); params.append(f"%{author}%")
    if language:
        where.append("a.language = ?"); params.append(language)
    if category_id is not None:
        where.append("a.category_id = ?"); params.append(category_id)
    if date_from:
        where.append("a.created_at >= ?"); params.append(date_from.isoformat(sep=" "))
    if date_to:
        where.append("a.created_at <= ?"); params.append(date_to.isoformat(sep=" "))
    if min_words is not None:
        where.append("a.word_count >= ?"); params.append(min_words)
    if max_words is not None:
        where.append("a.word_count <= ?"); params.append(max_words)
    if tag:
        where.append(
            "EXISTS (SELECT 1 FROM article_tags at JOIN tags t ON t.id = at.tag_id "
            "WHERE at.article_id = a.id AND t.name = ?)"
        )
        params.append(tag)

    sql = """
        SELECT a.id, a.title, a.author, a.language, a.word_count, a.summary,
               a.category_id, c.name AS category_name, a.created_at, a.updated_at
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
    # Wrap in double quotes so FTS5 treats it as a phrase; escape internal quotes.
    return '"' + q.replace('"', '""') + '"'


@app.get("/search/keyword")
def keyword_search(q: str, limit: int = 20):
    fts_q = _fts_quote(q)
    with conn_ctx() as conn:
        rows = conn.execute(
            """
            SELECT a.id, a.title, a.author, a.language, a.summary, a.word_count, a.created_at,
                   bm25(articles_fts) AS rank
            FROM articles_fts
            JOIN articles a ON a.id = articles_fts.rowid
            WHERE articles_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (fts_q, limit),
        ).fetchall()
        for r in rows:
            r["rank"] = float(r["rank"]) if r["rank"] is not None else None
        return _attach_tags(conn, rows)


@app.get("/search/fuzzy")
def fuzzy_search(q: str, limit: int = 20):
    like = f"%{q}%"
    with conn_ctx() as conn:
        rows = conn.execute(
            """
            SELECT a.id, a.title, a.author, a.language, a.summary, a.word_count, a.created_at,
                   (CASE WHEN a.title LIKE ? THEN 2 ELSE 0 END) +
                   (CASE WHEN a.content LIKE ? THEN 1 ELSE 0 END) AS sim
            FROM articles a
            WHERE a.title LIKE ? OR a.content LIKE ?
            ORDER BY sim DESC, a.created_at DESC
            LIMIT ?
            """,
            (like, like, like, like, limit),
        ).fetchall()
        return _attach_tags(conn, rows)


@app.get("/search/semantic")
def semantic_search(q: str, limit: int = 20):
    _require_semantic()
    vec = embed(q)
    with conn_ctx() as conn:
        rows = conn.execute(
            """
            SELECT a.id, a.title, a.author, a.language, a.summary, a.word_count, a.created_at,
                   v.distance,
                   (1.0 - v.distance) AS similarity
            FROM (
                SELECT article_id, distance
                FROM article_vec
                WHERE embedding MATCH ? AND k = ?
                ORDER BY distance
            ) v
            JOIN articles a ON a.id = v.article_id
            ORDER BY v.distance
            """,
            (serialize_vec(vec), limit),
        ).fetchall()
        return _attach_tags(conn, rows)


@app.get("/articles/{aid}/related")
def related(aid: int, limit: int = 5):
    _require_semantic()
    with conn_ctx() as conn:
        row = conn.execute(
            "SELECT embedding FROM article_vec WHERE article_id = ?", (aid,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "article not found or has no embedding")
        rows = conn.execute(
            """
            SELECT a.id, a.title, a.author, a.language, a.summary, a.word_count, a.created_at,
                   v.distance,
                   (1.0 - v.distance) AS similarity
            FROM (
                SELECT article_id, distance
                FROM article_vec
                WHERE embedding MATCH ? AND k = ?
                ORDER BY distance
            ) v
            JOIN articles a ON a.id = v.article_id
            WHERE a.id != ?
            ORDER BY v.distance
            LIMIT ?
            """,
            (row["embedding"], limit + 1, aid, limit),
        ).fetchall()
        return _attach_tags(conn, rows)


# ---------- AI helpers ----------
@app.post("/articles/{aid}/summarize")
def ai_summarize(aid: int, save: bool = True, max_chars: int = 200):
    _require_ai()
    with conn_ctx() as conn:
        r = conn.execute("SELECT title, content FROM articles WHERE id = ?", (aid,)).fetchone()
        if not r:
            raise HTTPException(404, "not found")
        s = llm.summarize(r["title"], r["content"], max_chars=max_chars)
        if save:
            conn.execute("UPDATE articles SET summary = ? WHERE id = ?", (s, aid))
        return {"summary": s}


@app.post("/articles/{aid}/auto-tag")
def ai_auto_tag(aid: int, save: bool = True, max_tags: int = 6):
    _require_ai()
    with conn_ctx() as conn:
        r = conn.execute("SELECT title, content FROM articles WHERE id = ?", (aid,)).fetchone()
        if not r:
            raise HTTPException(404, "not found")
        tags = llm.auto_tags(r["title"], r["content"], max_tags=max_tags)
        if save and tags:
            tag_ids = _upsert_tags(conn, tags)
            _set_article_tags(conn, aid, tag_ids)
        return {"tags": tags}


@app.post("/articles/{aid}/translate")
def ai_translate(aid: int, target_language: str, save: bool = True):
    _require_ai()
    with conn_ctx() as conn:
        if save:
            cached = conn.execute(
                "SELECT translated_title, translated_content FROM article_translations "
                "WHERE article_id = ? AND target_language = ?",
                (aid, target_language),
            ).fetchone()
            if cached:
                return {
                    "title": cached["translated_title"],
                    "content": cached["translated_content"],
                    "cached": True,
                }
        r = conn.execute("SELECT title, content FROM articles WHERE id = ?", (aid,)).fetchone()
        if not r:
            raise HTTPException(404, "not found")
        out = llm.translate(r["title"], r["content"], target_language)
        if save:
            conn.execute(
                """
                INSERT INTO article_translations (article_id, target_language, translated_title, translated_content)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(article_id, target_language)
                DO UPDATE SET translated_title=excluded.translated_title,
                              translated_content=excluded.translated_content,
                              created_at=datetime('now')
                """,
                (aid, target_language, out["title"], out["content"]),
            )
        return {**out, "cached": False}


# ---------- UI ----------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "enable_ai": ENABLE_AI, "enable_semantic": ENABLE_SEMANTIC},
    )


@app.get("/healthz")
def health():
    return {"ok": True}
