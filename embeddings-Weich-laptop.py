"""Embeddings stub.

PyTorch / sentence-transformers were removed because Windows Smart App Control
blocks the unsigned native DLLs. Semantic search and related-articles are disabled.

To re-enable later: install `sentence-transformers`, restore the original
implementation, and set ENABLE_SEMANTIC=true in .env.
"""
from config import ENABLE_SEMANTIC


def is_enabled() -> bool:
    return ENABLE_SEMANTIC


def embed(text: str) -> list[float] | None:
    if not ENABLE_SEMANTIC:
        return None
    raise RuntimeError(
        "ENABLE_SEMANTIC=true but no embedding backend is installed. "
        "Install sentence-transformers and restore embeddings.py."
    )


def embed_batch(texts: list[str]) -> list[list[float] | None]:
    return [embed(t) for t in texts]
