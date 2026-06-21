import os
from dotenv import load_dotenv

load_dotenv()


def _bool(v: str) -> bool:
    return str(v).lower() in ("1", "true", "yes", "on")


SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "articles.db")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "paraphrase-multilingual-MiniLM-L12-v2")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "384"))
ENABLE_AI = _bool(os.getenv("ENABLE_AI", "false"))
ENABLE_SEMANTIC = _bool(os.getenv("ENABLE_SEMANTIC", "false"))
