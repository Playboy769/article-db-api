import html as html_mod
import json as json_lib
import os
import time
from pathlib import Path
import re
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
from urllib.parse import quote, urlencode, urlparse

import httpx
import trafilatura
from markdownify import markdownify as _md

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
)
from pydantic import BaseModel, Field

import auth as A
from config import AUTH_SIGNING_SECRET, AUTHORIZED_EMAIL, CENTRAL_AUTH_ORIGIN
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
            "ALTER TABLE articles ADD COLUMN loved INTEGER DEFAULT 0",
            "ALTER TABLE articles ADD COLUMN learn TEXT DEFAULT ''",
        ]:
            try:
                conn.execute(stmt)
            except Exception:
                pass
        # 一次性修復：合併重複的分類（同名同層）。SQLite 的 UNIQUE(name, parent_id)
        # 對 parent_id 為 NULL 的頂層資料夾無效，過去可能產生重複，於此合併。
        try:
            cats = conn.execute(
                "SELECT id, name, parent_id FROM categories ORDER BY id"
            ).fetchall()
            seen = {}
            for cat in cats:
                pid = cat["parent_id"] if cat["parent_id"] is not None else -1
                key = (cat["name"], pid)
                if key in seen:
                    keeper, dup = seen[key], cat["id"]
                    conn.execute("UPDATE articles SET category_id = ? WHERE category_id = ?", (keeper, dup))
                    conn.execute("DELETE FROM categories WHERE id = ?", (dup,))
                else:
                    seen[key] = cat["id"]
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

# ── Auth (cross-domain login system — see auth.py for the shared token format) ──
# OutsideFramework is the only service that talks to Google; this service just verifies
# tokens it mints and keeps its own short-lived local session once verified once. Gating
# this closes a real pre-existing exposure: every endpoint here (including the ones that
# call the Gemini API) was previously reachable by anyone with no auth at all.
AUTH_SECRET = AUTH_SIGNING_SECRET
SESSION_COOKIE = "adb_sid"
CSRF_COOKIE = "csrf_token"
SESSION_TTL_SEC = 12 * 60 * 60
_auth_fail_limiter = A.RateLimiter(window_seconds=600, max_hits=30)

if not AUTH_SECRET:
    print("WARNING: AUTH_SIGNING_SECRET is not set — every request will be treated as unauthenticated.")


# Cookie flags and this service's own origin come from configuration, never from request
# headers: Host and X-Forwarded-Proto are both attacker-settable, and deriving the Secure
# flag from X-Forwarded-Proto let anyone strip it off an issued cookie by sending
# "X-Forwarded-Proto: http".
IS_PROD = os.getenv("ENV") == "production" or bool(os.getenv("RAILWAY_PROJECT_ID")) or bool(
    os.getenv("RAILWAY_ENVIRONMENT_NAME")
)
SELF_ORIGIN = os.getenv("PUBLIC_ORIGIN", "https://articlebase.up.railway.app")
_ALLOWED_HOSTS = {urlparse(SELF_ORIGIN).netloc.lower(), "articlebase.up.railway.app"}
_LOCAL_HOST_RE = re.compile(r"^(localhost|127\.0\.0\.1)(:\d+)?$")
# Marks that we already sent this browser to the central service for a handoff token.
# See the loop breaker in auth_gate below.
HANDOFF_TRY_COOKIE = "adb_hs"


def _self_origin(request: Request) -> str:
    host = (request.headers.get("host") or "").lower()
    if not IS_PROD and _LOCAL_HOST_RE.match(host):
        return f"http://{host}"
    if host in _ALLOWED_HOSTS:
        return f"https://{host}"
    return SELF_ORIGIN


def _session_email(request: Request):
    # verify_for derives the key from the audience, so a token minted for any other service
    # cannot produce a valid signature here. This previously checked neither aud nor typ,
    # and the session cookie minted below carried no aud at all -- making it a universal key
    # accepted by every sibling service that also skipped the check.
    payload = A.verify_for(
        request.cookies.get(SESSION_COOKIE), AUTH_SECRET,
        aud=_self_origin(request), typ=A.TYP_SESSION,
    )
    if not payload or not payload.get("email"):
        return None
    if payload["email"].lower() != AUTHORIZED_EMAIL:
        return None
    return payload["email"]


def _csrf_ok(request: Request) -> bool:
    cookie_tok = request.cookies.get(CSRF_COOKIE)
    header_tok = request.headers.get("x-csrf-token")
    return bool(cookie_tok) and bool(header_tok) and cookie_tok == header_tok


def _clean_path_and_query(request: Request) -> str:
    qs = {k: v for k, v in request.query_params.items() if k != "auth"}
    return request.url.path + ("?" + urlencode(qs) if qs else "")


@app.middleware("http")
async def auth_gate(request: Request, call_next):
    # Default-deny, allowlist the public exception (not the other way around) so a new
    # route added later is gated by default. OPTIONS preflight is exempt too — the
    # browser never attaches cookies to it and always follows up with the real
    # (gated) request, so there's nothing to protect by redirecting it.
    if request.method == "OPTIONS" or request.url.path == "/healthz":
        return await call_next(request)

    email = _session_email(request)
    if not email:
        self_origin = _self_origin(request)
        token = request.query_params.get("auth")
        # aud and typ are both enforced, and the key is derived from aud -- the hand-written
        # `payload.get("aud") == self_origin` comparison this replaces was correct, but it
        # was the only thing standing between a token for one service and a session on
        # another, and the session path above had no such comparison at all.
        payload = (
            A.verify_for(token, AUTH_SECRET, aud=self_origin, typ=A.TYP_HANDOFF) if token else None
        )
        token_ok = (
            payload
            and payload.get("email")
            and payload["email"].lower() == AUTHORIZED_EMAIL
        )
        if token_ok:
            # The local session now names its own audience and purpose. It used to carry
            # neither, which made this cookie a universal key across all four services.
            session_token = A.sign_for(
                {
                    "email": payload["email"].lower(),
                    "aud": self_origin,
                    "typ": A.TYP_SESSION,
                    "exp": time.time() + SESSION_TTL_SEC,
                },
                AUTH_SECRET,
            )
            csrf_token = A.random_token(18)
            resp = RedirectResponse(url=_clean_path_and_query(request), status_code=302)
            resp.headers.append(
                "set-cookie",
                A.cookie_header(SESSION_COOKIE, session_token, max_age=SESSION_TTL_SEC, http_only=True, secure=IS_PROD),
            )
            resp.headers.append(
                "set-cookie",
                A.cookie_header(CSRF_COOKIE, csrf_token, max_age=SESSION_TTL_SEC, http_only=False, secure=IS_PROD),
            )
            resp.headers.append("set-cookie", A.clear_cookie_header(HANDOFF_TRY_COOKIE))
            return resp

        if not _auth_fail_limiter.check(A.client_ip(request)):
            return PlainTextResponse("Too many attempts. Try again later.", status_code=429)

        # Loop breaker. Bouncing back to /auth/handoff is right when there was no token or
        # an expired one -- the central service just mints a fresh one. But if a token that
        # WAS present still fails, minting another identical one fails identically and the
        # browser ping-pongs forever. That is exactly what a signing-scheme rollout across
        # four independently-deployed services produces mid-window, so fail loudly once.
        if token and request.cookies.get(HANDOFF_TRY_COOKIE):
            resp = HTMLResponse(
                "<h1>登入交接失敗</h1><p>中央登入服務簽發的憑證無法在此服務驗證。"
                f'若剛完成部署，請稍候一分鐘再重試。</p><p><a href="{CENTRAL_AUTH_ORIGIN}">回首頁</a></p>',
                status_code=503,
            )
            resp.headers.append("set-cookie", A.clear_cookie_header(HANDOFF_TRY_COOKIE))
            return resp

        return_to = self_origin + _clean_path_and_query(request)
        resp = RedirectResponse(
            url=f"{CENTRAL_AUTH_ORIGIN}/auth/handoff?return_to={quote(return_to, safe='')}", status_code=302
        )
        resp.headers.append(
            "set-cookie",
            A.cookie_header(HANDOFF_TRY_COOKIE, "1", max_age=120, http_only=True, secure=IS_PROD),
        )
        return resp

    # Double-submit CSRF check applies to every mutating request once authenticated —
    # enforced here once instead of per-route so a future POST/PATCH/DELETE endpoint is
    # covered automatically instead of needing a remembered opt-in.
    if request.method in ("POST", "PUT", "PATCH", "DELETE") and not _csrf_ok(request):
        return JSONResponse({"error": "csrf"}, status_code=403)

    return await call_next(request)


_HERE = Path(__file__).parent

@app.get("/")
def frontend():
    # 強制每次都跟伺服器重新驗證，避免瀏覽器（尤其是跨來源 iframe 嵌入時，
    # 例如 OutsideFramework 的 Works 頁）快取住舊版 index.html 導致更新看不到。
    return FileResponse(
        _HERE / "index.html",
        headers={"Cache-Control": "no-cache, must-revalidate"},
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
    loved: Optional[int] = None
    highlights: Optional[str] = None
    notes: Optional[str] = None
    cld: Optional[str] = None
    learn: Optional[str] = None


class TagIn(BaseModel):
    name: str


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
        # 冪等：同名同層已存在就回傳既有的，避免產生重複分類（資料夾）
        existing = conn.execute(
            "SELECT * FROM categories WHERE name = ? AND IFNULL(parent_id, -1) = IFNULL(?, -1)",
            (c.name, c.parent_id),
        ).fetchone()
        if existing:
            return existing
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
                  "loved", "highlights", "notes", "cld", "learn"):
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


def _call_gemini(prompt: str, json_mode: bool = True):
    if not GEMINI_API_KEY:
        raise HTTPException(500, "伺服器未設定 GEMINI_API_KEY，請在 Railway 環境變數加入")
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )
    gen_config = {"temperature": 0.4}
    if json_mode:
        gen_config["responseMimeType"] = "application/json"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": gen_config,
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
        return json_lib.loads(text) if json_mode else text.strip()
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


# ---------- 學習拆解（布魯姆 / SOLO）AI 生成 ----------
_LEARN_PROMPT = """你是教學設計專家，精通布魯姆認知分類法（Bloom's Taxonomy）與 SOLO 分類法。
請閱讀以下文章，把內容「由基底向上」拆解成兩套認知層級，產出可用於主動學習的知識點。

要求：
- 全程使用繁體中文。
- 每個層級萃取「一個」最具代表性的知識點：concept 是簡短的關鍵概念或任務（4～12 字），detail 是對應的說明、定義或例子（20～45 字）。
- concept 與 detail 必須來自文章實際內容，不要杜撰；越基底越偏記憶事實，越上層越偏分析、評估與創造。
- desc 用一句話說明「在這篇文章中，這個層級要做什麼思考」。
- 各層的 concept 不要重複，detail 要能明確對應到自己的 concept。

布魯姆 6 層（由低到高）：記憶 Remember、理解 Understand、應用 Apply、分析 Analyse、評估 Evaluate、創造 Create。
SOLO 4 層（由低到高）：單點結構 Uni-structural、多點結構 Multi-structural、關聯 Relational、拓展抽象 Extended Abstract。

只輸出 JSON，結構如下（不要任何多餘文字、不要 markdown 圍欄）：
{
  "bloom": [
    {"level":"記憶","en":"Remember","desc":"...","concept":"...","detail":"..."},
    {"level":"理解","en":"Understand","desc":"...","concept":"...","detail":"..."},
    {"level":"應用","en":"Apply","desc":"...","concept":"...","detail":"..."},
    {"level":"分析","en":"Analyse","desc":"...","concept":"...","detail":"..."},
    {"level":"評估","en":"Evaluate","desc":"...","concept":"...","detail":"..."},
    {"level":"創造","en":"Create","desc":"...","concept":"...","detail":"..."}
  ],
  "solo": [
    {"level":"單點結構","en":"Uni-structural","desc":"...","concept":"...","detail":"..."},
    {"level":"多點結構","en":"Multi-structural","desc":"...","concept":"...","detail":"..."},
    {"level":"關聯","en":"Relational","desc":"...","concept":"...","detail":"..."},
    {"level":"拓展抽象","en":"Extended Abstract","desc":"...","concept":"...","detail":"..."}
  ]
}

文章標題：%(title)s

文章內容：
%(content)s
"""


@app.post("/articles/{aid}/generate-learn")
def generate_learn(aid: int):
    with conn_ctx() as conn:
        row = conn.execute(
            "SELECT title, content FROM articles WHERE id = ?", (aid,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "not found")
        title = row["title"] or ""
        content = (row["content"] or "")[:12000]
        if len(content.strip()) < 30:
            raise HTTPException(400, "文章內容太短，無法生成學習拆解")
        prompt = _LEARN_PROMPT % {"title": title, "content": content}
        data = _call_gemini(prompt)
        if not isinstance(data, dict) or "bloom" not in data or "solo" not in data:
            raise HTTPException(502, "Gemini 回傳的學習拆解結構不完整")
        learn_json = json_lib.dumps(data, ensure_ascii=False)
        conn.execute("UPDATE articles SET learn = ? WHERE id = ?", (learn_json, aid))
        return data


# ---------- AI 摘要生成 ----------
_SUMMARY_PROMPT = """你是專業的文章編輯。請閱讀以下文章，寫出一段精煉的繁體中文摘要。

要求：
- 用 3 到 5 句話、約 120 至 200 字，涵蓋文章的核心論點與重要結論。
- 客觀中立、直接陳述內容，不要使用「這篇文章」「作者認為」等贅詞。
- 只回傳純文字摘要，不要 JSON、不要 markdown 標記、不要標題。

文章標題：%(title)s

文章內容：
%(content)s
"""


@app.post("/articles/{aid}/generate-summary")
def generate_summary(aid: int):
    with conn_ctx() as conn:
        row = conn.execute(
            "SELECT title, content FROM articles WHERE id = ?", (aid,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "not found")
        title = row["title"] or ""
        content = (row["content"] or "")[:12000]
        if len(content.strip()) < 30:
            raise HTTPException(400, "文章內容太短，無法生成摘要")
        prompt = _SUMMARY_PROMPT % {"title": title, "content": content}
        summary = (_call_gemini(prompt, json_mode=False) or "").strip()
        if not summary:
            raise HTTPException(502, "Gemini 未回傳摘要內容")
        conn.execute("UPDATE articles SET summary = ? WHERE id = ?", (summary, aid))
        return {"summary": summary}


# ---------- AI 文章問答 ----------
class AskIn(BaseModel):
    question: str


_ASK_PROMPT = """你是嚴謹的研究助理。請「只根據」以下文章內容，用繁體中文回答使用者的問題。

要求：
- 只依文章內容作答；若文章未提供足夠資訊，直接說「文章未提及」，不要臆測或補充外部知識。
- 回答精簡、切中要點，必要時可條列。
- 只回傳純文字答案，不要 JSON、不要 markdown 標題。

文章標題：%(title)s

文章內容：
%(content)s

使用者問題：%(question)s
"""


@app.post("/articles/{aid}/ask")
def ask_article(aid: int, body: AskIn):
    question = (body.question or "").strip()
    if not question:
        raise HTTPException(400, "請輸入問題")
    if len(question) > 500:
        raise HTTPException(400, "問題過長，請精簡至 500 字內")
    with conn_ctx() as conn:
        row = conn.execute(
            "SELECT title, content FROM articles WHERE id = ?", (aid,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "not found")
        title = row["title"] or ""
        content = (row["content"] or "")[:12000]
        if len(content.strip()) < 20:
            raise HTTPException(400, "文章內容太短，無法問答")
        prompt = _ASK_PROMPT % {
            "title": title, "content": content, "question": question
        }
        answer = (_call_gemini(prompt, json_mode=False) or "").strip()
        if not answer:
            raise HTTPException(502, "Gemini 未回傳答案")
        return {"answer": answer}


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


# ---------- 儀表板統計 ----------
@app.get("/stats")
def stats():
    with conn_ctx() as conn:
        def scalar(sql: str, params=()) -> int:
            r = conn.execute(sql, params).fetchone()
            if not r:
                return 0
            return list(r.values())[0] or 0

        month_start = "date('now','start of month')"
        result = {
            "articles": {
                "total": scalar("SELECT COUNT(*) FROM articles"),
                "this_month": scalar(
                    f"SELECT COUNT(*) FROM articles WHERE created_at >= {month_start}"
                ),
                "starred": scalar("SELECT COUNT(*) FROM articles WHERE starred = 1"),
                "with_cld": scalar(
                    "SELECT COUNT(*) FROM articles WHERE cld IS NOT NULL AND cld != ''"
                ),
            },
            "tags": {"total": scalar("SELECT COUNT(*) FROM tags")},
        }
        # 各分類文章數
        result["categories"] = conn.execute(
            """SELECT c.id, c.name, COUNT(a.id) AS count
               FROM categories c
               LEFT JOIN articles a ON a.category_id = c.id
               GROUP BY c.id ORDER BY count DESC, c.name"""
        ).fetchall()
        # 最近更新的文章
        result["recent_articles"] = conn.execute(
            """SELECT id, title, author, updated_at FROM articles
               ORDER BY updated_at DESC LIMIT 8"""
        ).fetchall()
        return result


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


_STRIP_TAGS = ["script","style","nav","header","footer","aside",
               "noscript","button","form","iframe","svg","img","figure","picture"]

def _html_to_md(html: str) -> str:
    """把文章 HTML 轉成 Markdown 純文字（不含圖片）。"""
    md_text = _md(html, heading_style="ATX", strip=_STRIP_TAGS)
    return re.sub(r'\n{3,}', '\n\n', md_text).strip()


def _extract_main_md(raw: str) -> str:
    """備援：擷取最長的 <article>/<main> 區塊轉 Markdown，
    用來補足 trafilatura 偶爾「只抓到部分主文」的情況。"""
    best = ""
    for tag in ("article", "main"):
        for m in re.finditer(rf'<{tag}\b[^>]*>(.*?)</{tag}>', raw, re.I | re.S):
            md_text = _html_to_md(m.group(1))
            if len(md_text) > len(best):
                best = md_text
    return best


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
            if d.get("@type") in ("Article", "NewsArticle", "BlogPosting", "SocialMediaPosting"):
                if d.get("headline") and not result.get("title"):
                    result["title"] = d["headline"]
                if d.get("author") and not result.get("author"):
                    auth = d["author"]
                    if isinstance(auth, list):
                        auth = auth[0]
                    result["author"] = auth.get("name", "")
                date_val = d.get("datePublished") or d.get("dateCreated")
                if date_val and not result.get("date"):
                    result["date"] = date_val[:10]
                body_val = d.get("articleBody") or d.get("text")
                if body_val and not result.get("body"):
                    result["body"] = body_val
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
    content = _html_to_md(body_html) if body_html else ""
    # body_html 為空或過短（如付費文章只給預覽）時，回傳 None 改走一般抓取，
    # 不要用 truncated_body_text（那只是截斷預覽，會導致字數太少）
    if len(content.strip()) < 200:
        return None

    return {"title": title, "author": author, "date": date, "content": content}


def _fetch_substack_note(url: str) -> dict | None:
    """Substack「Notes」貼文（短動態，非文章）沒有對應的 by-slug API 可用，
    網頁本身又是重度 SPA 殼層，一般抓取流程（trafilatura + og/meta）在這種頁面上
    會把標題誤判成發文者名稱、作者誤判成頁面通用的 <meta name="author" content="Substack">。
    改用頁面內嵌的 JSON-LD（@type: SocialMediaPosting）直接取得乾淨的作者與全文內容。"""
    if not re.match(r'https?://(?:www\.)?substack\.com/@[^/?]+/note/', url):
        return None

    try:
        with httpx.Client(headers=_FETCH_HEADERS, follow_redirects=True, timeout=15) as client:
            r = client.get(url)
            r.raise_for_status()
            raw = r.text
    except Exception:
        return None

    posting = None
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        raw, re.I | re.S
    ):
        try:
            d = json_lib.loads(m.group(1))
            if isinstance(d, list):
                d = d[0]
            if d.get("@type") == "SocialMediaPosting" and d.get("text"):
                posting = d
                break
        except Exception:
            continue
    if not posting:
        return None

    content = posting["text"].strip()
    if not content:
        return None

    auth = posting.get("author") or {}
    if isinstance(auth, list):
        auth = auth[0] if auth else {}
    author = auth.get("name", "")
    date = (posting.get("dateCreated") or "")[:10]

    # Notes 沒有標題欄位，用內文第一行當標題（供列表顯示用）
    first_line = next((ln.strip() for ln in content.splitlines() if ln.strip()), "")
    title = first_line[:60] + ("…" if len(first_line) > 60 else "")

    return {"title": title, "author": author, "date": date, "content": content}


@app.get("/fetch-url")
def fetch_url_endpoint(url: str):
    # ── Substack ──────────────────────────────────────────────────
    substack = _fetch_substack(url) or _fetch_substack_note(url)
    if substack:
        return substack

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
        include_formatting=True, no_fallback=False, favor_recall=True,
        output_format="markdown",
    ) or ""

    # 備援：trafilatura 有時只抓到部分主文。比對 <article>/<main> 區塊，
    # 若明顯更長（絕對與相對都更長）就改用它，避免文章被截斷。
    alt = _extract_main_md(raw)
    if len(alt) > len(content) + 400 and len(alt) > len(content) * 1.25:
        content = alt

    # 備援：JSON-LD 內嵌的 articleBody。不少站台（含部分軟性付費牆）
    # 即使畫面上把內文遮住，全文仍以 articleBody 形式存在頁面裡。
    ld_body = (ld.get("body") or "").strip()
    if len(ld_body) > len(content):
        content = ld_body

    if not content:
        clean = re.sub(
            r"<(script|style|nav|header|footer|aside|form|button|noscript)[^>]*>.*?</\1>",
            "", raw, flags=re.I | re.S
        )
        clean = re.sub(r"<[^>]+>", " ", clean)
        clean = html_mod.unescape(re.sub(r"[ \t]+", " ", clean).strip())
        content = "\n".join(ln.strip() for ln in clean.splitlines() if ln.strip())

    return {"title": title, "content": content, "author": author, "date": date}
