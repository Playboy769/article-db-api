# 文章資料庫 — 安裝與啟動（SQLite 版）

不用裝 PostgreSQL、不用 Docker，也不用 PyTorch。Python 內建 SQLite，搭配 `sqlite-vec` 擴充。

## 啟用狀況

| 功能 | 預設 | 開啟方式 |
|---|---|---|
| 全文搜尋（FTS5 + BM25） | ✅ 啟用 | — |
| 模糊搜尋（LIKE） | ✅ 啟用 | — |
| CRUD / 分類 / 標籤 / 進階篩選 | ✅ 啟用 | — |
| 語意搜尋 / 相關文章 | ❌ 停用 | `ENABLE_SEMANTIC=true` + 自行裝 sentence-transformers（受 Smart App Control 限制） |
| 自動摘要 / 自動標籤 / 翻譯 | ❌ 停用 | `ENABLE_AI=true` + 填入 `ANTHROPIC_API_KEY`（會花 API 費用） |

## 1. Python 環境
```
cd article_db
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 2. 設定環境變數
```
copy .env.example .env
```
不需要填任何值就能用基本功能。要啟用 AI 功能再去 `.env` 把 `ENABLE_AI` 改 `true` 並填 API key。

## 3. 初始化資料庫
```
python db.py
```
看到 `Schema initialized at articles.db` 就成功。

## 4. 啟動
```
python -m uvicorn app:app
```
> 用 `python -m uvicorn` 而不是直接 `uvicorn`，避免 Smart App Control 擋住 `uvicorn.exe`。
> 不加 `--reload`，避免 watchfiles 的原生模組被 Smart App Control 擋。

開瀏覽器 → http://127.0.0.1:8000

互動式 API 文件：http://127.0.0.1:8000/docs

## 之後每天用

```powershell
cd article_db
.venv\Scripts\activate
python -m uvicorn app:app
```

按 Ctrl+C 關掉 server。

## API 速查
| 功能 | 端點 |
|---|---|
| 新增文章 | `POST /articles` |
| 取得 / 更新 / 刪除 | `GET/PATCH/DELETE /articles/{id}` |
| 列表 + 進階篩選 | `GET /articles?author=&language=&tag=&min_words=&date_from=...` |
| 全文搜尋（FTS5） | `GET /search/keyword?q=...` |
| 模糊搜尋（LIKE） | `GET /search/fuzzy?q=...` |
| 語意搜尋 | `GET /search/semantic?q=...`（停用時回 503） |
| 相關文章 | `GET /articles/{id}/related`（停用時回 503） |
| 自動摘要 | `POST /articles/{id}/summarize`（停用時回 403） |
| 自動標籤 | `POST /articles/{id}/auto-tag`（停用時回 403） |
| 翻譯（含快取） | `POST /articles/{id}/translate?target_language=en` |
| 分類樹 | `GET /categories`、`POST /categories` |

## 備份
備份就是複製 `articles.db` 一個檔案。

## 常見問題

**Q: 啟動時報 `應用程式控制原則已封鎖此檔案`？**
A: Windows Smart App Control 擋住未簽署的原生 DLL。本專案預設已避開：
   - 不裝 PyTorch（語意搜尋停用）
   - 啟動用 `python -m uvicorn`（避開 uvicorn.exe）
   - 不加 `--reload`（避開 watchfiles 的 `_C.pyd`）
   如果還遇到，可在 `設定 → 隱私權與安全性 → Windows 安全性 → 應用程式與瀏覽器控制 → 智慧型應用程式控制設定` 關掉。
   注意：關掉後無法重新開啟（除非重灌 Windows）。

**Q: 想啟用語意搜尋怎麼辦？**
A: 需要：
   1. 關掉 Smart App Control（一次性、不可逆）
   2. `pip install sentence-transformers`
   3. 把 `embeddings.py` 換回原本用 SentenceTransformer 的版本（git history 或請 Claude 重寫）
   4. `.env` 設 `ENABLE_SEMANTIC=true`

**Q: 中文搜尋結果不準？**
A: FTS5 用 trigram tokenizer 處理中英文都還可以。如果搜尋字詞太短（1-2 字）可能不準，
   建議改用模糊搜尋（LIKE 子字串比對）。
