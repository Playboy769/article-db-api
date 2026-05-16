# deploy marker: CLD (causal loop diagram) AI generation — 2026-05-17
import html as html_mod
import json as json_lib
import os
import re
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")

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
            "ALTER TABLE articles ADD COLUMN highlights TEXT DEFAULT '[]'",
            "ALTER TABLE articles ADD COLUMN notes TEXT DEFAULT '[]'",
            "ALTER TABLE articles ADD COLUMN cld TEXT DEFAULT ''",
            "ALTER TABLE reports ADD COLUMN starred INTEGER DEFAULT 0",
            "ALTER TABLE reports ADD COLUMN highlights TEXT DEFAULT '[]'",
            "ALTER TABLE reports ADD COLUMN notes TEXT DEFAULT '[]'",
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
    highlights: str = "[]"
    notes: str = "[]"
    cld: str = ""


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
    highlights: Optional[str] = None
    notes: Optional[str] = None
    cld: Optional[str] = None


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
    starred: int = 0
    highlights: str = "[]"
    notes: str = "[]"


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
    starred: Optional[int] = None
    highlights: Optional[str] = None
    notes: Optional[str] = None


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
               (title, content, author, source, language, category_id, summary, date, images, starred, highlights, notes, cld)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (a.title, a.content, a.author, a.source, a.language,
             a.category_id, a.summary, a.date, a.images, a.starred, a.highlights, a.notes, a.cld),
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
                  "category_id", "summary", "date", "images", "starred",
                  "highlights", "notes", "cld"):
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


# ---------- 因果循環圖 (CLD) AI 生成 ----------
_CLD_PROMPT = """你是系統動力學（System Dynamics）專家。請分析以下文章，萃取出一張「因果循環圖」(Causal Loop Diagram)。

規則：
- 找出文章核心的關鍵變數／概念，6 到 12 個，作為節點（node）。節點名稱要簡短（2-8 字）。
- 找出變數之間的因果關係，作為有方向的連結（edge）。
- 每個連結標註極性 polarity：「+」表示同向（原因增加→結果也增加），「-」表示反向（原因增加→結果減少）。
- 盡量讓連結形成回饋迴路。若辨識出迴路，放進 loops：type「R」為增強迴路、「B」為調節迴路，label 給一個簡短的迴路名稱。
- 全部使用繁體中文。

只回傳 JSON，不要任何其他文字，格式必須是：
{
  "nodes": [{"id": "n1", "label": "概念名稱"}],
  "edges": [{"from": "n1", "to": "n2", "polarity": "+"}],
  "loops": [{"type": "R", "label": "迴路名稱", "nodes": ["n1", "n2", "n3"]}]
}

文章標題：%(title)s

文章內容：
%(content)s
"""


def _call_gemini(prompt: str) -> dict:
    if not GEMINI_API_KEY:
        raise HTTPException(500, "伺服器未設定 GEMINI_API_KEY，請在 Railway 環境變數加入")
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.4,
        },
    }
    data = json_lib.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:900]
        raise HTTPException(502, f"Gemini API 錯誤 {e.code}：{detail}")
    except Exception as e:
        raise HTTPException(502, f"無法連線 Gemini API：{e}")
    try:
        parsed = json_lib.loads(raw)
        text = parsed["candidates"][0]["content"]["parts"][0]["text"]
        return json_lib.loads(text)
    except Exception as e:
        raise HTTPException(502, f"Gemini 回應格式異常：{e}")


@app.post("/articles/{aid}/generate-cld")
def generate_cld(aid: int):
    with conn_ctx() as conn:
        row = conn.execute(
            "SELECT title, content FROM articles WHERE id = ?", (aid,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "not found")
        title = row["title"] or ""
        content = (row["content"] or "")[:12000]
        if len(content.strip()) < 30:
            raise HTTPException(400, "文章內容太短，無法生成因果圖")
        prompt = _CLD_PROMPT % {"title": title, "content": content}
        cld = _call_gemini(prompt)
        # 基本結構驗證
        if not isinstance(cld, dict) or "nodes" not in cld or "edges" not in cld:
            raise HTTPException(502, "Gemini 回傳的因果圖結構不完整")
        cld.setdefault("loops", [])
        cld_json = json_lib.dumps(cld, ensure_ascii=False)
        conn.execute("UPDATE articles SET cld = ? WHERE id = ?", (cld_json, aid))
        return cld


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
               a.date, a.images, a.category_id, a.starred,
               a.highlights, a.notes, a.cld,
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
               (company, ticker, sector, rating, target, date, analyst, source, content, images,
                starred, highlights, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (r.company, r.ticker, r.sector, r.rating, r.target,
             r.date, r.analyst, r.source, r.content, r.images,
             r.starred, r.highlights, r.notes),
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
                  "date", "analyst", "source", "content", "images",
                  "starred", "highlights", "notes"):
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


# ---------- URL fetch (proxy) ----------
@app.get("/fetch-url")
def fetch_url_endpoint(url: str):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible)"})
        with urllib.request.urlopen(req, timeout=12) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        raise HTTPException(400, f"無法抓取：{e}")
    title_m = re.search(r"<title[^>]*>(.*?)</title>", raw, re.I | re.S)
    title = html_mod.unescape(title_m.group(1).strip()) if title_m else ""
    clean = re.sub(
        r"<(script|style|nav|header|footer|aside|form|button)[^>]*>.*?</\1>",
        "", raw, flags=re.I | re.S
    )
    clean = re.sub(r"<[^>]+>", " ", clean)
    clean = html_mod.unescape(re.sub(r"[ \t]+", " ", clean).strip())
    clean = "\n".join(line.strip() for line in clean.splitlines() if line.strip())
    return {"title": title, "content": clean[:8000]}
