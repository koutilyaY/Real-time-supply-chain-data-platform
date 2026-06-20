"""
Embed supply-chain documents (SOPs, contracts, shipping notices) into Qdrant.

Chunks each .md/.txt file under rag/sample_docs (or a path passed as argv[1]),
embeds with sentence-transformers, and upserts into the Qdrant collection.

Usage:
  QDRANT_URL=http://localhost:6333 python rag/ingest/ingest_docs.py [docs_dir]
"""
import glob
import os
import sys

from qdrant_client.models import PointStruct

# Make the rag package root importable regardless of cwd.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import client, embed, ensure_collection, COLLECTION  # noqa: E402

CHUNK_CHARS = 800
OVERLAP = 120


def chunk(text: str) -> list[str]:
    out, i = [], 0
    while i < len(text):
        out.append(text[i : i + CHUNK_CHARS])
        i += CHUNK_CHARS - OVERLAP
    return [c.strip() for c in out if c.strip()]


def main() -> int:
    docs_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "sample_docs")
    files = sorted(glob.glob(os.path.join(docs_dir, "**", "*.md"), recursive=True)) + sorted(
        glob.glob(os.path.join(docs_dir, "**", "*.txt"), recursive=True)
    )
    if not files:
        print(f"No documents found under {docs_dir}")
        return 1

    c = client()
    ensure_collection(c)
    points, pid = [], 0
    for path in files:
        with open(path, encoding="utf-8") as fh:
            chunks = chunk(fh.read())
        vectors = embed(chunks)
        for ch, vec in zip(chunks, vectors):
            points.append(
                PointStruct(id=pid, vector=vec, payload={"source": os.path.basename(path), "text": ch})
            )
            pid += 1
    c.upsert(collection_name=COLLECTION, points=points)
    print(f"Ingested {pid} chunks from {len(files)} files into '{COLLECTION}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
