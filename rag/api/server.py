"""
RAG question-answering API.

Retrieves the top-k document chunks from Qdrant for the user's question, then
asks a local Ollama LLM to answer using ONLY that retrieved context (context
engineering: explicit system instruction + grounded context + the question).
If Ollama is unreachable, it returns the retrieved context so the endpoint is
still useful for debugging retrieval.
"""
import os
import sys

import requests
from fastapi import FastAPI
from pydantic import BaseModel

# Make the rag package root importable regardless of cwd.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import client, embed, ensure_collection, COLLECTION  # noqa: E402

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")
TOP_K = int(os.getenv("RAG_TOP_K", "4"))

app = FastAPI(title="Supply-Chain RAG", version="0.1.0")

SYSTEM = (
    "You are a supply-chain operations assistant. Answer the question using ONLY "
    "the provided context. If the context is insufficient, say so plainly. Be concise."
)


class Ask(BaseModel):
    question: str


def retrieve(question: str) -> list[dict]:
    c = client()
    ensure_collection(c)
    hits = c.search(collection_name=COLLECTION, query_vector=embed([question])[0], limit=TOP_K)
    return [{"source": h.payload.get("source"), "text": h.payload.get("text"), "score": h.score} for h in hits]


def generate(question: str, context: str) -> str | None:
    prompt = f"{SYSTEM}\n\n# Context\n{context}\n\n# Question\n{question}\n\n# Answer\n"
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=60,
        )
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception:  # noqa: BLE001
        return None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ask")
def ask(body: Ask):
    docs = retrieve(body.question)
    context = "\n\n".join(f"[{d['source']}] {d['text']}" for d in docs)
    answer = generate(body.question, context)
    return {
        "question": body.question,
        "answer": answer,
        "note": None if answer else "Ollama unreachable; returning retrieved context only.",
        "sources": [{"source": d["source"], "score": d["score"]} for d in docs],
        "context": None if answer else context,
    }
