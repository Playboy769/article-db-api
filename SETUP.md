# 文章資料庫 — 安裝與啟動（SQLite 版）

不用裝 PostgreSQL、不用 Docker。Python 內建 SQLite，搭配 `sqlite-vec` 擴充做向量搜尋。

## 1. Python 環境
```
cd article_db
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

> 第一次跑會下載 sentence-transformers 多語模型（約 450MB），之後就不用了。

## 2. 設定環境變數
```
copy .env.example .env
```
編輯 `.env`，填入 `ANTHROPIC_API_KEY`。`SQLITE_DB_PATH` 預設為 `articles.db`（會建在 `article_db/` 目錄下）。

## 3. 初始化資料庫
```
python db.py
```
會建立 `articles.db` 檔案，內含所有資料表 + FTS5 + sqlite-vec 向量表。

## 4. 啟動
```
uvicorn app:app --reload
```
開瀏覽器 → http://127.0.0.1:8000

互動式 API 文件：http://127.0.0.1:8000/docs

## API 速查
| 功能 | 端點 |
|---|---|
| 新增文章 | `POST /articles` |
| 取得 / 更新 / 刪除 | `GET/PATCH/DELETE /articles/{id}` |
| 列表 + 進階篩選 | `GET /articles?author=&language=&tag=&min_words=&date_from=...` |
| 全文搜尋（FTS5） | `GET /search/keyword?q=...` |
| 模糊搜尋（LIKE） | `GET /search/fuzzy?q=...` |
| 語意搜尋（向量） | `GET /search/semantic?q=...` |
| 相關文章 | `GET /articles/{id}/related` |
| 自動摘要 | `POST /articles/{id}/summarize` |
| 自動標籤 | `POST /articles/{id}/auto-tag` |
| 翻譯（含快取） | `POST /articles/{id}/translate?target_language=en` |
| 分類樹 | `GET /categories`、`POST /categories` |

## 控制 Anthropic API 用量
- 摘要 / 標籤 / 翻譯都是「按下按鈕才呼叫」，不會自動觸發
- 翻譯結果存進 `article_translations`，同一篇 + 同一目標語言只呼叫一次
- 預設模型 `claude-haiku-4-5-20251001`（最便宜）
- 內文超過 8000 字會自動截斷再送 API
- Embedding 用本地模型，**完全免費**

## 備份
備份就是複製 `articles.db` 一個檔案。要搬到別台電腦就把整個 `article_db/` 資料夾複製過去。

## 常見問題

**Q: 啟動時報 `no such module: vec0`？**
A: `sqlite-vec` 沒裝好或 Python 的 sqlite3 不支援載入擴充。
   重裝：`pip install --force-reinstall sqlite-vec`

**Q: 啟動時報 `no such module: fts5` 或 trigram tokenizer 錯誤？**
A: Python 內建的 sqlite3 太舊（< 3.34）。Windows 預設 Python 通常很新，
   不太會遇到。如果遇到請升級 Python 至 3.11+。

**Q: 中文搜尋結果不準？**
A: FTS5 用 trigram tokenizer 處理中英文都還可以；準確度較依賴語意搜尋。
   建議遇到中文找不到時改用 `/search/semantic`。
