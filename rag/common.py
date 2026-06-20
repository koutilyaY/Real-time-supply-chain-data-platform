"""Shared RAG config + embedding/Qdrant helpers."""
import os

from fastembed import TextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("RAG_COLLECTION", "supply_chain_docs")
# ONNX-based embeddings (no torch). 384-dim, small & fast.
EMBED_MODEL = os.getenv("EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBED_DIM = 384

_model: TextEmbedding | None = None


def model() -> TextEmbedding:
    global _model
    if _model is None:
        _model = TextEmbedding(model_name=EMBED_MODEL)
    return _model


def client() -> QdrantClient:
    return QdrantClient(url=QDRANT_URL)


def ensure_collection(c: QdrantClient) -> None:
    names = {col.name for col in c.get_collections().collections}
    if COLLECTION not in names:
        c.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
        )


def embed(texts: list[str]) -> list[list[float]]:
    # fastembed returns a generator of np.ndarray; Qdrant uses cosine distance
    # so explicit normalization isn't required.
    return [vec.tolist() for vec in model().embed(texts)]
