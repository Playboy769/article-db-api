PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS categories (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    parent_id  INTEGER REFERENCES categories(id) ON DELETE CASCADE,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(name, parent_id)
);

CREATE TABLE IF NOT EXISTS articles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT NOT NULL,
    content     TEXT NOT NULL DEFAULT '',
    author      TEXT,
    source      TEXT,
    language    TEXT DEFAULT 'zh',
    summary     TEXT,
    date        TEXT,
    images      TEXT DEFAULT '[]',
    starred     INTEGER DEFAULT 0,
    highlights  TEXT DEFAULT '[]',
    notes       TEXT DEFAULT '[]',
    category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS articles_author_idx   ON articles(author);
CREATE INDEX IF NOT EXISTS articles_category_idx ON articles(category_id);
CREATE INDEX IF NOT EXISTS articles_created_idx  ON articles(created_at);

CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
    title, content,
    content='articles', content_rowid='id',
    tokenize='trigram'
);

CREATE TRIGGER IF NOT EXISTS articles_fts_ai AFTER INSERT ON articles BEGIN
    INSERT INTO articles_fts(rowid, title, content) VALUES (new.id, new.title, new.content);
END;
CREATE TRIGGER IF NOT EXISTS articles_fts_ad AFTER DELETE ON articles BEGIN
    INSERT INTO articles_fts(articles_fts, rowid, title, content)
    VALUES ('delete', old.id, old.title, old.content);
END;
CREATE TRIGGER IF NOT EXISTS articles_fts_au AFTER UPDATE OF title, content ON articles BEGIN
    INSERT INTO articles_fts(articles_fts, rowid, title, content)
    VALUES ('delete', old.id, old.title, old.content);
    INSERT INTO articles_fts(rowid, title, content) VALUES (new.id, new.title, new.content);
END;

CREATE TRIGGER IF NOT EXISTS articles_touch
AFTER UPDATE OF title, content, author, source, language, category_id, summary, date, images ON articles
FOR EACH ROW BEGIN
    UPDATE articles SET updated_at = datetime('now') WHERE id = old.id;
END;

CREATE TABLE IF NOT EXISTS tags (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS article_tags (
    article_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
    tag_id     INTEGER REFERENCES tags(id)     ON DELETE CASCADE,
    PRIMARY KEY (article_id, tag_id)
);

-- ── 公司深度報告 ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS reports (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    company     TEXT NOT NULL,
    ticker      TEXT,
    sector      TEXT,
    rating      TEXT,
    target      TEXT,
    date        TEXT,
    analyst     TEXT,
    source      TEXT,
    content     TEXT DEFAULT '',
    images      TEXT DEFAULT '[]',
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS reports_company_idx ON reports(company);
CREATE INDEX IF NOT EXISTS reports_sector_idx  ON reports(sector);
CREATE INDEX IF NOT EXISTS reports_created_idx ON reports(created_at);

CREATE TRIGGER IF NOT EXISTS reports_touch
AFTER UPDATE OF company, ticker, sector, rating, target, date, analyst, source, content, images ON reports
FOR EACH ROW BEGIN
    UPDATE reports SET updated_at = datetime('now') WHERE id = old.id;
END;

-- ── 連結資料庫 ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS links (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    title      TEXT NOT NULL,
    url        TEXT NOT NULL,
    desc       TEXT,
    folder     TEXT DEFAULT '',
    icon       TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS links_folder_idx  ON links(folder);
CREATE INDEX IF NOT EXISTS links_created_idx ON links(created_at);
