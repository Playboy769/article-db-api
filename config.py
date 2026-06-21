import os
from dotenv import load_dotenv

load_dotenv()

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "articles.db")
TURSO_URL      = os.getenv("TURSO_DATABASE_URL", "")
TURSO_TOKEN    = os.getenv("TURSO_AUTH_TOKEN", "")
