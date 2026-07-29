import os
from dotenv import load_dotenv

load_dotenv()

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "articles.db")
TURSO_URL      = os.getenv("TURSO_DATABASE_URL", "")
TURSO_TOKEN    = os.getenv("TURSO_AUTH_TOKEN", "")

# Cross-domain login system — see auth.py. AUTH_SIGNING_SECRET must be the exact same
# value across all four deployed services (OutsideFramework, globe-invest,
# structural_holes, article_db); AUTHORIZED_EMAIL is the single whitelisted account.
AUTH_SIGNING_SECRET = os.getenv("AUTH_SIGNING_SECRET", "")
AUTHORIZED_EMAIL    = os.getenv("AUTHORIZED_EMAIL", "").lower()
CENTRAL_AUTH_ORIGIN = os.getenv("CENTRAL_AUTH_ORIGIN", "https://ofw.up.railway.app")
