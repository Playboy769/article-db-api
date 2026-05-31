# deploy marker: PDF files on volume filesystem — 2026-05-22
import base64
import html as html_mod
import json as json_lib
import os
from pathlib import Path
import re
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import io
import httpx
import trafilatura
from markdownify import markdownify as _md
from PIL import Image

from config import SQLITE_DB_PATH

# PDF binary 存到與 SQLite 同層的 pdfs/ 資料夾（在 Railway Volume 上）
_DB_DIR = Path(SQLITE_DB_PATH).resolve().parent
PDF_DIR = _DB_DIR / "pdfs"
PDF_DIR.mkdir(parents=True, exist_ok=True)


def _pdf_path(pid: int) -> Path:
    return PDF_DIR / f"{pid}.pdf"

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
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
            # PDF library
            """CREATE TABLE IF NOT EXISTS pdfs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT NOT NULL,
                author      TEXT,
                date        TEXT,
                source      TEXT,
                data        TEXT NOT NULL,
                highlights  TEXT DEFAULT '[]',
                notes       TEXT DEFAULT '[]',
                starred     INTEGER DEFAULT 0,
                category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
                created_at  TEXT DEFAULT (datetime('now')),
                updated_at  TEXT DEFAULT (datetime('now'))
            )""",
            "CREATE INDEX IF NOT EXISTS pdfs_created_idx ON pdfs(created_at)",
            "CREATE INDEX IF NOT EXISTS pdfs_category_idx ON pdfs(category_id)",
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
        # 一次性遷移：把舊版存在 pdfs.data 欄位的 base64 搬到 Volume 上的檔案
        try:
            rows = conn.execute(
                "SELECT id, data FROM pdfs WHERE data IS NOT NULL AND data != ''"
            ).fetchall()
            for r in rows:
                try:
                    raw = base64.b64decode(r["data"])
                    _pdf_path(r["id"]).write_bytes(raw)
                    conn.execute("UPDATE pdfs SET data = '' WHERE id = ?", (r["id"],))
                except Exception:
                    pass
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

_HERE = Path(__file__).parent

@app.get("/")
def frontend():
    return FileResponse(_HERE / "index.html")


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


class PDFIn(BaseModel):
    title: str
    author: Optional[str] = None
    date: Optional[str] = None
    source: Optional[str] = None
    data: str = ""             # base64 of PDF bytes
    highlights: str = "[]"
    notes: str = "[]"
    starred: int = 0
    category_id: Optional[int] = None


class PDFUpdate(BaseModel):
    title: Optional[str] = None
    author: Optional[str] = None
    date: Optional[str] = None
    source: Optional[str] = None
    highlights: Optional[str] = None
    notes: Optional[str] = None
    starred: Optional[int] = None
    category_id: Optional[int] = None
    # data is not patchable — re-upload via a new POST


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
_CLD_PROMPT = """你是系統動力學（System Dynamics）專家。請分析以下文章，萃取出一張**清晰、易讀**的「因果循環圖」(Causal Loop Diagram)。

最重要的目標：圖要**簡單明瞭**，不要雜亂。寧可少而精，不要多而亂。

節點規則：
- 萃取 5 到 8 個**最核心**的關鍵變數作為節點。不要超過 8 個。
- 節點名稱要非常簡短，2 到 6 個字，用名詞（例如「學習動機」「財富」「焦慮感」）。
- 每個節點都必須至少連到 2 條連結（有進有出），不要有孤立或只連一條的節點。

連結規則：
- 連結要**充足且有意義**：每個節點平均連到 2-3 個其他節點，讓圖形成清楚的網狀因果結構。
- 每條連結標註極性 polarity：「+」表示同向（原因增加→結果也增加），「-」表示反向（原因增加→結果減少）。
- **務必讓連結形成封閉的回饋迴路**——這是因果循環圖的重點。每個節點都應該屬於至少一個迴路。
- 不要有「死路」：不要有只進不出、或只出不進的節點。

迴路規則：
- 辨識出 2 到 4 個主要回饋迴路放進 loops。
- type「R」為增強迴路（同向放大）、「B」為調節迴路（反向收斂）。
- label 給一個簡短好懂的迴路名稱（4-10 字）。
- loops 裡的 nodes 要按迴路繞行的順序列出。

全部使用繁體中文。只回傳 JSON，不要任何其他文字、不要 markdown 標記，格式必須是：
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


# ---------- PDFs CRUD ----------
# PDF 二進位內容存到 Volume 上的 pdfs/{id}.pdf；DB 的 data 欄位保留為空字串
@app.post("/pdfs")
def create_pdf(p: PDFIn):
    if not p.data:
        raise HTTPException(400, "缺少 PDF 內容")
    try:
        raw = base64.b64decode(p.data)
    except Exception:
        raise HTTPException(400, "PDF base64 格式錯誤")

    with conn_ctx() as conn:
        cur = conn.execute(
            """INSERT INTO pdfs
               (title, author, date, source, data, highlights, notes, starred, category_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (p.title, p.author, p.date, p.source, "",
             p.highlights, p.notes, p.starred, p.category_id),
        )
        pid = cur.lastrowid

    # 寫到 Volume 上的檔案
    try:
        _pdf_path(pid).write_bytes(raw)
    except Exception as e:
        # 寫檔失敗 → 回滾資料庫紀錄，避免 orphan row
        with conn_ctx() as conn:
            conn.execute("DELETE FROM pdfs WHERE id = ?", (pid,))
        raise HTTPException(500, f"無法寫入 PDF 檔案：{e}")

    with conn_ctx() as conn:
        row = conn.execute(
            """SELECT id, title, author, date, source, highlights, notes, starred,
                      category_id, created_at, updated_at FROM pdfs WHERE id = ?""",
            (pid,),
        ).fetchone()
    return row


@app.get("/pdfs")
def list_pdfs(limit: int = Query(200, le=500), offset: int = 0):
    with conn_ctx() as conn:
        rows = conn.execute(
            """SELECT p.id, p.title, p.author, p.date, p.source,
                      p.highlights, p.notes, p.starred, p.category_id,
                      c.name AS category_name, p.created_at, p.updated_at
               FROM pdfs p LEFT JOIN categories c ON c.id = p.category_id
               ORDER BY p.created_at DESC LIMIT ? OFFSET ?""",
            (limit, offset),
        ).fetchall()
    return rows


@app.get("/pdfs/{pid}/data")
def get_pdf_data(pid: int):
    # 優先讀檔案（新版儲存方式）
    fp = _pdf_path(pid)
    if fp.exists():
        try:
            b64 = base64.b64encode(fp.read_bytes()).decode("ascii")
            return {"data": b64}
        except Exception as e:
            raise HTTPException(500, f"讀取 PDF 失敗：{e}")
    # 後援：舊資料還在 DB 的 data 欄位
    with conn_ctx() as conn:
        row = conn.execute("SELECT data FROM pdfs WHERE id = ?", (pid,)).fetchone()
    if not row:
        raise HTTPException(404, "not found")
    return {"data": row["data"] or ""}


@app.get("/pdfs/{pid}")
def get_pdf(pid: int):
    with conn_ctx() as conn:
        row = conn.execute(
            """SELECT p.id, p.title, p.author, p.date, p.source,
                      p.highlights, p.notes, p.starred, p.category_id,
                      c.name AS category_name, p.created_at, p.updated_at
               FROM pdfs p LEFT JOIN categories c ON c.id = p.category_id
               WHERE p.id = ?""",
            (pid,),
        ).fetchone()
    if not row:
        raise HTTPException(404, "not found")
    return row


@app.patch("/pdfs/{pid}")
def update_pdf(pid: int, u: PDFUpdate):
    with conn_ctx() as conn:
        if not conn.execute("SELECT id FROM pdfs WHERE id = ?", (pid,)).fetchone():
            raise HTTPException(404, "not found")
        fields, values = [], []
        for k in ("title", "author", "date", "source",
                  "highlights", "notes", "starred", "category_id"):
            v = getattr(u, k)
            if v is not None:
                fields.append(f"{k} = ?")
                values.append(v)
        if fields:
            values.append(pid)
            conn.execute(f"UPDATE pdfs SET {', '.join(fields)} WHERE id = ?", values)
        return conn.execute(
            """SELECT id, title, author, date, source, highlights, notes, starred,
                      category_id, created_at, updated_at FROM pdfs WHERE id = ?""",
            (pid,),
        ).fetchone()


@app.delete("/pdfs/{pid}")
def delete_pdf(pid: int):
    fp = _pdf_path(pid)
    try:
        if fp.exists():
            fp.unlink()
    except Exception:
        pass
    with conn_ctx() as conn:
        conn.execute("DELETE FROM pdfs WHERE id = ?", (pid,))
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
_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

_MAX_IMAGES   = 6     # 每篇文章最多嵌入幾張圖
_IMG_MAX_DIM  = 960   # 最長邊像素上限
_IMG_QUALITY  = 78    # JPEG 壓縮品質
_IMG_SKIP_RE  = re.compile(
    r'(icon|logo|avatar|pixel|tracking|badge|button|spinner|ad[_-]|\.gif$)',
    re.I
)

def _encode_image(url: str, client: httpx.Client) -> str | None:
    """下載並壓縮圖片，回傳 base64 data URL；失敗回傳 None。"""
    if _IMG_SKIP_RE.search(url):
        return None
    try:
        r = client.get(url, timeout=6, follow_redirects=True)
        if r.status_code != 200:
            return None
        ctype = r.headers.get("content-type", "").split(";")[0].strip()
        if not ctype.startswith("image/") or ctype == "image/gif":
            return None
        img = Image.open(io.BytesIO(r.content))
        if img.width < 80 or img.height < 80:   # 跳過縮圖/icon
            return None
        # 縮放
        if img.width > _IMG_MAX_DIM or img.height > _IMG_MAX_DIM:
            img.thumbnail((_IMG_MAX_DIM, _IMG_MAX_DIM), Image.LANCZOS)
        # 轉 RGB（JPEG 不支援透明）
        if img.mode not in ("RGB",):
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[3] if img.mode == "RGBA" else None)
            img = bg
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=_IMG_QUALITY, optimize=True)
        return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()
    except Exception:
        return None


_STRIP_TAGS = ["script","style","nav","header","footer","aside",
               "noscript","button","form","iframe","svg"]

def _html_to_md(html: str) -> str:
    """把文章 HTML 轉成保留圖片位置的 Markdown（僅用於 body_html 已是純文章內容的情況）。"""
    # 1. 把 <picture>…</picture> 化簡成單純 <img src="…">
    def _simplify_picture(m: re.Match) -> str:
        src = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', m.group(0), re.I)
        alt = re.search(r'<img[^>]+alt=["\']([^"\']*)["\']', m.group(0), re.I)
        if src:
            a = html_mod.unescape(alt.group(1)) if alt else ""
            return f'<img src="{html_mod.unescape(src.group(1))}" alt="{a}">'
        return ""
    html = re.sub(r'<picture[^>]*>.*?</picture>', _simplify_picture, html, flags=re.I|re.S)
    # 2. 把 <a href="…"><img …></a> 解包成 <img …>，避免 markdownify 產生 [![](url)](link)
    html = re.sub(r'<a[^>]*>\s*(<img[^>]+>)\s*</a>', r'\1', html, flags=re.I)
    # 3. markdownify 轉換
    md_text = _md(html, heading_style="ATX", strip=_STRIP_TAGS)
    return re.sub(r'\n{3,}', '\n\n', md_text).strip()


def _inline_images(md: str, client: httpx.Client) -> str:
    """把 Markdown 裡的圖片 URL 替換成 base64 data URL。"""
    pattern = re.compile(r'!\[([^\]]*)\]\((https?://[^)\s]+)\)')
    count = [0]

    def replace(m: re.Match) -> str:
        if count[0] >= _MAX_IMAGES:
            return ""          # 超過上限：移除圖片標記
        data_url = _encode_image(m.group(2), client)
        if data_url:
            count[0] += 1
            return f"![{m.group(1)}]({data_url})"
        return ""              # 下載失敗：移除圖片標記

    return pattern.sub(replace, md)


def _og(raw: str, prop: str) -> str:
    m = re.search(
        rf'<meta[^>]+(?:property|name)=["\']og:{prop}["\'][^>]+content=["\'](.*?)["\']',
        raw, re.I | re.S
    ) or re.search(
        rf'<meta[^>]+content=["\'](.*?)["\'][^>]+(?:property|name)=["\']og:{prop}["\']',
        raw, re.I | re.S
    )
    return html_mod.unescape(m.group(1).strip()) if m else ""


def _json_ld(raw: str) -> dict:
    """Extract title/author/date from JSON-LD structured data."""
    result = {}
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        raw, re.I | re.S
    ):
        try:
            d = json_lib.loads(m.group(1))
            if isinstance(d, list):
                d = d[0]
            if d.get("@type") in ("Article", "NewsArticle", "BlogPosting"):
                if d.get("headline") and not result.get("title"):
                    result["title"] = d["headline"]
                if d.get("author") and not result.get("author"):
                    auth = d["author"]
                    if isinstance(auth, list):
                        auth = auth[0]
                    result["author"] = auth.get("name", "")
                if d.get("datePublished") and not result.get("date"):
                    result["date"] = d["datePublished"][:10]
        except Exception:
            pass
    return result


def _fetch_substack(url: str) -> dict | None:
    """Use Substack's internal API to get full article JSON."""
    m = re.match(r'https?://open\.substack\.com/pub/([^/?]+)/p/([^/?]+)', url)
    if not m:
        m = re.match(r'https?://([^./?]+)\.substack\.com/p/([^/?]+)', url)
    if not m:
        return None

    pub, slug = m.group(1), m.group(2)
    api_url = f"https://{pub}.substack.com/api/v1/posts/by-slug/{slug}"
    try:
        with httpx.Client(headers=_FETCH_HEADERS, follow_redirects=True, timeout=15) as client:
            r = client.get(api_url)
            if r.status_code != 200:
                return None
            d = r.json()
    except Exception:
        return None

    title = d.get("title", "")
    bylines = d.get("publishedBylines") or []
    author = bylines[0].get("name", "") if bylines else ""
    date = (d.get("post_date") or "")[:10]

    # body_html contains the full article HTML
    body_html = d.get("body_html", "")
    content = ""
    if body_html:
        with httpx.Client(headers=_FETCH_HEADERS, follow_redirects=True, timeout=20) as img_client:
            content = _html_to_md(body_html)
            content = _inline_images(content, img_client)
    if not content:
        content = d.get("truncated_body_text", "")

    return {"title": title, "author": author, "date": date, "content": content}


@app.get("/fetch-url")
def fetch_url_endpoint(url: str):
    # ── Substack ──────────────────────────────────────────────────
    substack = _fetch_substack(url)
    if substack:
        return substack

@app.get("/debug-fetch")
def debug_fetch(url: str):
    """Debug endpoint: 回傳 Substack body_html 片段和抓到的圖片 URL，不做 base64 編碼。"""
    m = re.match(r'https?://open\.substack\.com/pub/([^/?]+)/p/([^/?]+)', url)
    if not m:
        m = re.match(r'https?://([^./?]+)\.substack\.com/p/([^/?]+)', url)
    if not m:
        return {"error": "not a substack url"}
    pub, slug = m.group(1), m.group(2)
    api_url = f"https://{pub}.substack.com/api/v1/posts/by-slug/{slug}"
    with httpx.Client(headers=_FETCH_HEADERS, follow_redirects=True, timeout=15) as client:
        r = client.get(api_url)
        d = r.json()
    body_html = d.get("body_html", "")
    # 找出所有 <img src="..."> 和 <source srcset="...">
    img_srcs = re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', body_html, re.I)
    picture_count = len(re.findall(r'<picture', body_html, re.I))
    a_img_count = len(re.findall(r'<a[^>]*>\s*<img', body_html, re.I))
    # 跑 _html_to_md 看結果
    md_preview = _html_to_md(body_html)
    img_in_md = re.findall(r'!\[([^\]]*)\]\((https?://[^)\s]+)\)', md_preview)
    return {
        "picture_tags": picture_count,
        "a_img_direct": a_img_count,
        "img_srcs_in_html": img_srcs[:5],
        "img_refs_in_md": img_in_md[:5],
        "md_preview_500chars": md_preview[:500],
    }

    # ── General fetch ─────────────────────────────────────────────
    try:
        with httpx.Client(headers=_FETCH_HEADERS, follow_redirects=True, timeout=15) as client:
            resp = client.get(url)
            resp.raise_for_status()
            raw = resp.text
    except Exception as e:
        raise HTTPException(400, f"無法抓取：{e}")

    # title: JSON-LD → og:title → <title>
    ld = _json_ld(raw)
    title_m = re.search(r"<title[^>]*>(.*?)</title>", raw, re.I | re.S)
    title = (
        ld.get("title")
        or _og(raw, "title")
        or (html_mod.unescape(title_m.group(1).strip()) if title_m else "")
    )

    # author: JSON-LD → og:article:author → meta author
    author = ld.get("author") or _og(raw, "article:author") or _og(raw, "author")
    if not author:
        am = re.search(r'<meta[^>]+name=["\']author["\'][^>]+content=["\'](.*?)["\']', raw, re.I)
        author = html_mod.unescape(am.group(1).strip()) if am else ""

    # date: JSON-LD → og
    date = ld.get("date") or _og(raw, "article:published_time") or _og(raw, "published_time")
    if date:
        date = date[:10]

    # content: trafilatura 萃取主體文字（markdown 格式）
    content = trafilatura.extract(
        raw, url=url, include_comments=False, include_tables=True,
        no_fallback=False, favor_recall=True,
        output_format="markdown",
    )
    if not content:
        clean = re.sub(
            r"<(script|style|nav|header|footer|aside|form|button|noscript)[^>]*>.*?</\1>",
            "", raw, flags=re.I | re.S
        )
        clean = re.sub(r"<[^>]+>", " ", clean)
        clean = html_mod.unescape(re.sub(r"[ \t]+", " ", clean).strip())
        content = "\n".join(ln.strip() for ln in clean.splitlines() if ln.strip())

    return {"title": title, "content": content, "author": author, "date": date}
