"""
從本機 CSV 備份還原文章到 Railway。

使用：
    cd C:\\Users\\ryan9\\OneDrive\\桌面\\Claudecode\\article_db
    .venv\\Scripts\\python.exe restore_from_csv.py

CSV 必須有以下欄位（編碼 UTF-8，可帶 BOM）：
    編號 / 資料夾 / 文章名稱 / 作者 / 日期 / 來源 / 文章內容

⚠️ 執行前請先確認 Railway 已掛 Volume 並設好 SQLITE_DB_PATH，
   否則資料會在下次 redeploy 時再次消失。
"""

import csv
import json
import sys
import urllib.request
import urllib.error

RAILWAY_URL = "https://web-production-2210c.up.railway.app"
CSV_PATH    = r"C:\Users\ryan9\OneDrive\桌面\文章資料庫.csv"
TIMEOUT     = 60


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
    print("─" * 56)
    print(f"連線測試：{RAILWAY_URL}/healthz")
    try:
        api("GET", "/healthz")
        print("✓ Railway 已上線")
    except Exception as e:
        print(f"✕ 無法連線：{e}")
        sys.exit(1)


def warn_if_not_empty():
    """提醒：避免重覆匯入"""
    try:
        existing = api("GET", "/articles?limit=1")
        if existing:
            print("─" * 56)
            print(f"⚠️  Railway 上已有 {len(existing)}+ 篇文章。繼續會建立重覆資料！")
            ans = input("確定要繼續嗎？(yes/no): ").strip().lower()
            if ans != "yes":
                print("取消")
                sys.exit(0)
    except Exception:
        pass


def main():
    print("=" * 56)
    print("CSV → Railway 還原工具")
    print("=" * 56)
    print(f"來源 CSV：{CSV_PATH}")
    print(f"目標    ：{RAILWAY_URL}")
    print()

    health_check()
    warn_if_not_empty()

    # ── 讀取 CSV ─────────────────────────────────────────────
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    print("─" * 56)
    print(f"共讀到 {len(rows)} 篇文章")

    # ── 第一階段：建立資料夾 ─────────────────────────────────
    print("─" * 56)
    print("[1/2] 建立資料夾（categories）")
    folder_names = []
    seen = set()
    for r in rows:
        name = (r.get("資料夾") or "").strip()
        if name and name not in seen:
            seen.add(name)
            folder_names.append(name)

    folder_id_map = {}
    for name in folder_names:
        try:
            res = api("POST", "/categories", {"name": name})
            folder_id_map[name] = res["id"]
            print(f"  ✓ 「{name}」→ id {res['id']}")
        except Exception as e:
            print(f"  ✕ 「{name}」失敗：{e}")
    print(f"  完成：{len(folder_id_map)} / {len(folder_names)} 個資料夾")

    # ── 第二階段：上傳文章 ────────────────────────────────────
    print("─" * 56)
    print("[2/2] 上傳文章")
    success = 0
    failed = []
    total = len(rows)
    for i, r in enumerate(rows, 1):
        title  = (r.get("文章名稱") or "未命名").strip()
        folder = (r.get("資料夾")   or "").strip()
        body = {
            "title":       title,
            "content":     r.get("文章內容") or "",
            "author":      (r.get("作者")   or "").strip() or None,
            "source":      (r.get("來源")   or "").strip() or None,
            "language":    "zh",
            "category_id": folder_id_map.get(folder),
            "summary":     None,
            "date":        (r.get("日期")   or "").strip() or None,
            "images":      "[]",
            "starred":     0,
            "tags":        [],
        }
        try:
            api("POST", "/articles", body)
            success += 1
            print(f"  [{i:>3}/{total}] ✓ {title[:40]}")
        except Exception as e:
            failed.append((title, str(e)))
            print(f"  [{i:>3}/{total}] ✕ {title[:40]} — {e}")

    # ── 結果 ─────────────────────────────────────────────────
    print("─" * 56)
    print(f"完成：{success} / {total} 篇文章")
    if failed:
        print(f"失敗 {len(failed)} 篇：")
        for t, err in failed:
            print(f"  - {t}: {err}")
    print("=" * 56)
    print("✓ 全部完成！打開前端網頁按 F5 重整即可看到資料")
    print("  https://Playboy769.github.io/article-db/")
    print("=" * 56)


if __name__ == "__main__":
    main()
