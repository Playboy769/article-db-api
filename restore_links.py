"""從 backup/links_*.json 還原 links 到 Railway"""
import json
import urllib.request
import glob
import os
import sys

RAILWAY_URL = "https://web-production-2210c.up.railway.app"
BACKUP_DIR  = os.path.join(os.path.dirname(__file__), "backup")


def api_post(path, body):
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        RAILWAY_URL + path, data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    files = sorted(glob.glob(os.path.join(BACKUP_DIR, "links_*.json")))
    if not files:
        print("找不到備份檔")
        sys.exit(1)
    latest = files[-1]
    print(f"從 {os.path.basename(latest)} 讀取")
    with open(latest, encoding="utf-8") as f:
        links = json.load(f)
    print(f"共 {len(links)} 筆 links")
    for i, l in enumerate(links, 1):
        body = {
            "title":  l["title"],
            "url":    l["url"],
            "desc":   l.get("desc") or "",
            "folder": l.get("folder") or "",
            "icon":   l.get("icon") or "",
        }
        try:
            res = api_post("/links", body)
            print(f"  [{i}/{len(links)}] ✓ {l['title'][:50]}")
        except Exception as e:
            print(f"  [{i}/{len(links)}] ✕ {l['title'][:50]} — {e}")
    print("✓ 完成")


if __name__ == "__main__":
    main()
