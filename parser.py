import os
import re
import requests
import numpy as np

from bs4 import BeautifulSoup

from qdrant_client import models
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from sentence_transformers import SentenceTransformer


CNAME = os.getenv("CNAME", "testovoe")


def get_page_text(url: str) -> str:
    response = requests.get(url, timeout=20)
    soup = BeautifulSoup(response.text, 'lxml')
    div = soup.find("div", class_="wiki-content")
    if div is None:
        raise ValueError(f"Блок wiki-content на странице {url.split('/')[-1]} отсутствует")
    return div.get_text(separator=" ", strip=True)


def text_split(text: str) -> str:
    text = re.split(r"(?<=[.])\s+", text)
    return [i.strip() for i in text]


def create_chunks(sentences: list[str], url: str, n: int) -> list:
    chunks = []
    chunk_id = 0
    pg_number = int(re.search(r"\d+", url)[0])
    for i in range(0, len(sentences), n):
        merged_chunk = " ".join(sentences[i:i + n])
        if not merged_chunk:
            continue

        chunks.append(
            {
                "id":chunk_id,
                "page":pg_number,
                "chunk": merged_chunk
            }
        )
        chunk_id += 1
    return chunks


def create_vector(chunks: list, model: SentenceTransformer) -> np.ndarray:
    texts = [c["chunk"] for c in chunks]
    vectors = model.encode(texts, normalize_embeddings=True, batch_size=32)
    return vectors


def main(
    client: QdrantClient,
    model: SentenceTransformer,
    url: str,
    n_sentences: int = 2
) -> tuple[bool, str]:
    try:
        text = get_page_text(url)
        sentences = text_split(text)
        chunks = create_chunks(sentences, url, n_sentences)
        embeddings = create_vector(chunks, model)

        CNAME = "testovoe"

        if not client.collection_exists(CNAME):
            client.create_collection(
                collection_name=CNAME,
                vectors_config=models.VectorParams(
                    size=len(embeddings[0]),
                    distance=models.Distance.COSINE,
                ),
            )

        points = []
        for idx, (chunk, vec) in enumerate(zip(chunks, embeddings)):
            point = PointStruct(
                    id=idx,
                    vector=vec.tolist(),
                    payload={
                        "chunk_text":chunk["chunk"],
                        "page_number":chunk["page"]
                    }
                )
            points.append(point)

        client.upsert(
            collection_name=CNAME,
            points=points
        )
    except Exception as e:
        return False, e

    return True, "ok"