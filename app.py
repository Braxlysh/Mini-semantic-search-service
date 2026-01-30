import os
from litestar import Litestar, get, post
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from parser import main, CNAME


URL = os.getenv("SOURCE_URL", "https://confluence.atlassian.com/doc/page-restrictions-139414.html")
MODEL_NAME = os.getenv("EMBEDDING_MODEL", "deepvk/USER-bge-m3")
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")

model = SentenceTransformer(MODEL_NAME)
client = QdrantClient(url=QDRANT_URL)


@get("/create", sync_to_thread=True)
def create(n_sentences: int = 2) -> dict:
    ok, msg = main(client, model, URL, n_sentences)
    return {"answer": ok, "reason": msg}


@post("/search", sync_to_thread=True)
def search(data: dict) -> dict:
    query = data.get("query")
    if not query:
        return {"query": "error", "results": []}
    results = []
    
    qvec = model.encode([data['query']], normalize_embeddings=True)[0].tolist()
    
    hits = client.query_points(
        collection_name=CNAME,
        query=qvec,
        limit=5,
        with_payload=True,
    )
    
    for h in hits.points:
        results.append(
            {"id":h.id,"score":round(h.score, 3), "chunk_text":h.payload.get("chunk_text", ""), "page_number":h.payload.get("page_number", 0)},
        )

    return {"query": query, "results": results}


app = Litestar(route_handlers=[create, search])