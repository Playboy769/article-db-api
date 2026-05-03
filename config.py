import os
from dotenv import load_dotenv

load_dotenv()

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "articles.db")
