# RAG layer (local, fully open-source)

Embeds operational documents (SOPs, supplier policies, shipping notices) into
**Qdrant** and answers natural-language questions with a **local LLM via Ollama** —
no external API calls. Embeddings use **fastembed** (ONNX `all-MiniLM-L6-v2`,
384-dim) — no torch, so the image stays small.

## Run
```bash
# 1. start vector store + LLM + rag-api
docker compose --profile rag up -d --build

# 2. pull a local model (set OLLAMA_MODEL in .env to match)
docker compose exec ollama ollama pull llama3.2      # ~2GB, better quality
# or, for a tiny footprint:
docker compose exec ollama ollama pull qwen2.5:0.5b  # ~400MB

# 3. embed the sample docs (runs inside the rag-api container)
docker compose exec rag-api python ingest/ingest_docs.py

# 4. ask
curl -s localhost:8100/ask -H 'content-type: application/json' \
  -d '{"question":"What temperature must vaccines stay within and what happens on an excursion?"}' | jq
```

## Context engineering
`rag/api/server.py` builds the prompt as: **system instruction** (role + "use only
the context" guardrail) + **retrieved context** (top-k chunks with source tags) +
**question**. If Ollama is down, the endpoint returns the retrieved context so you
can still validate retrieval quality.

## Files
- `common.py` — embedding model (`all-MiniLM-L6-v2`, 384-dim) + Qdrant client.
- `ingest/ingest_docs.py` — chunk → embed → upsert.
- `api/server.py` — `/ask` retrieval + generation endpoint.
- `sample_docs/` — seed documents.
