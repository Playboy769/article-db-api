"""
一次性遷移腳本：把本機 articles.db 的所有資料上傳到 Railway。

使用：
    cd C:\\Users\\ryan9\\OneDrive\\桌面\\Claudecode\\article_db
    python migrate_to_railway.py

特性：
- 只用 Python 標準函式庫（sqlite3 + urllib）
- 進度顯示
- 失敗會印出詳細訊息但繼續跑下一筆
"""

import json
import sqlite3
import sys
import time
import urllib.request
import urllib.error

# ─────────────────────────────────────────────────────────────
RAILWAY_URL = "https://web-production-2210c.up.railway.app"
LOCAL_DB    = "articles.db"
TIMEOUT     = 60   # Railway cold start 可能要等 5-10 秒
# ─────────────────────────────────────────────────────────────


def api(method: str, path: str, body=None):
    url = RAILWAY_URL + path
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        raw = resp.read()
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))


def health_check():
    print("─" * 50)
    print(f"連線測試：{RAILWAY_URL}/healthz")
    try:
        api("GET", "/healthz")
        print("✓ Railway 已上線")
    except Exception as e:
        print(f"✕ 無法連線：{e}")
        print("請確認 Railway 服務正在運行")
        sys.exit(1)


def migrate_categories(conn):
    print("─" * 50)
    print("[1/3] 轉移資料夾（categories）")
    rows = conn.execute(
        "SELECT id, name, parent_id FROM categories ORDER BY parent_id IS NOT NULL, id"
    ).fetchall()
    if not rows:
        print("  （沒有資料夾）")
        return {}

    id_map = {}  # local_id → cloud_id
    for row in rows:
        local_id, name, parent_id = row
        body = {"name": name}
        if parent_id is not None:
            body["parent_id"] = id_map.get(parent_id)
        try:
            res = api("POST", "/categories", body)
            id_map[local_id] = res["id"]
            print(f"  ✓ 「{name}」(local {local_id} → cloud {res['id']})")
        except Exception as e:
            print(f"  ✕ 「{name}」失敗：{e}")
    print(f"  共轉移 {len(id_map)} / {len(rows)} 個資料夾")
    return id_map


def table_columns(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def migrate_articles(conn, cat_id_map):
    print("─" * 50)
    print("[2/3] 轉移文章（articles）")

    cols = table_columns(conn, "articles")
    # 只 SELECT 實際存在的欄位
    base = ["id", "title", "content"]
    optional = ["author", "source", "language", "summary",
                "date", "images", "starred", "category_id"]
    select_cols = base + [c for c in optional if c in cols]
    print(f"  本機 articles 欄位：{', '.join(sorted(cols))}")

    # 確認 tags / article_tags 表是否存在
    has_tags = bool(conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='article_tags'"
    ).fetchone()) and bool(conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='tags'"
    ).fetchone())

    rows = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM articles ORDER BY id"
    ).fetchall()
    if not rows:
        print("  （沒有文章）")
        return

    total = len(rows)
    success = 0
    failed = []

    for i, row in enumerate(rows, 1):
        d = dict(zip(select_cols, row))
        local_id = d["id"]

        # 抓 tags（若表不存在則跳過）
        tags = []
        if has_tags:
            tag_rows = conn.execute("""
                SELECT t.name FROM tags t
                JOIN article_tags at ON at.tag_id = t.id
                WHERE at.article_id = ?
                ORDER BY t.name
            """, (local_id,)).fetchall()
            tags = [r[0] for r in tag_rows]

        title = d.get("title") or "未命名"
        starred = d.get("starred") or 0
        body = {
            "title": title,
            "content": d.get("content") or "",
            "author": d.get("author"),
            "source": d.get("source"),
            "language": d.get("language") or "zh",
            "category_id": cat_id_map.get(d.get("category_id")),
            "summary": d.get("summary"),
            "date": d.get("date"),
            "images": d.get("images") or "[]",
            "starred": starred,
            "tags": tags,
        }

        try:
            api("POST", "/articles", body)
            success += 1
            mark = " ⭐" if starred else ""
            tag_info = f" [{','.join(tags)}]" if tags else ""
            print(f"  [{i:>3}/{total}] ✓ {title[:36]}{mark}{tag_info}")
        except Exception as e:
            failed.append((title, str(e)))
            print(f"  [{i:>3}/{total}] ✕ {title[:36]} — {e}")

    print(f"  完成：{success} / {total}")
    if failed:
        print(f"  失敗 {len(failed)} 篇：")
        for t, err in failed[:10]:
            print(f"    - {t}: {err}")


def migrate_reports(conn):
    print("─" * 50)
    print("[3/3] 轉移公司深度報告（reports）")

    # 確認 reports 表存在
    has_reports = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='reports'"
    ).fetchone()
    if not has_reports:
        print("  （本機沒有 reports 表）")
        return

    cols = table_columns(conn, "reports")
    base = ["company"]
    optional = ["ticker", "sector", "rating", "target", "date",
                "analyst", "source", "content", "images"]
    select_cols = base + [c for c in optional if c in cols]
    print(f"  本機 reports 欄位：{', '.join(sorted(cols))}")

    rows = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM reports ORDER BY id"
    ).fetchall() if "id" in cols else conn.execute(
        f"SELECT {', '.join(select_cols)} FROM reports"
    ).fetchall()
    if not rows:
        print("  （沒有公司報告）")
        return

    total = len(rows)
    success = 0
    failed = []

    for i, row in enumerate(rows, 1):
        d = dict(zip(select_cols, row))
        company = d.get("company") or "（未命名）"
        ticker = d.get("ticker")

        body = {
            "company": company,
            "ticker": ticker,
            "sector": d.get("sector"),
            "rating": d.get("rating"),
            "target": d.get("target"),
            "date": d.get("date"),
            "analyst": d.get("analyst"),
            "source": d.get("source"),
            "content": d.get("content") or "",
            "images": d.get("images") or "[]",
        }

        try:
            api("POST", "/reports", body)
            success += 1
            print(f"  [{i:>3}/{total}] ✓ {company} ({ticker or '—'})")
        except Exception as e:
            failed.append((company, str(e)))
            print(f"  [{i:>3}/{total}] ✕ {company} — {e}")

    print(f"  完成：{success} / {total}")
    if failed:
        print(f"  失敗 {len(failed)} 筆：")
        for c, err in failed[:10]:
            print(f"    - {c}: {err}")


def main():
    print("=" * 50)
    print("Articles DB → Railway 遷移工具")
    print("=" * 50)
    print(f"來源：{LOCAL_DB}")
    print(f"目標：{RAILWAY_URL}")
    print()

    health_check()

    conn = sqlite3.connect(LOCAL_DB)
    try:
        cat_id_map = migrate_categories(conn)
        migrate_articles(conn, cat_id_map)
        migrate_reports(conn)
    finally:
        conn.close()

    print("─" * 50)
    print("✓ 全部完成！打開前端網頁按 F5 重整即可看到資料")
    print("=" * 50)


if __name__ == "__main__":
    main()
