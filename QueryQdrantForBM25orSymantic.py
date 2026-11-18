from pathlib import Path

from qdrant_client import QdrantClient, models

COLLECTION = "stackoverflow"
EMBEDDING_MODEL = "sentence-transformers/all-minilm-l6-v2"


def main():
    question = input("Enter your question: ").strip()
    if not question:
        print("Please enter a question.")
        return

    # Use the exact filenames from your working ingest script.
    downloads = Path.home() / "Downloads"

    client = QdrantClient(
        url=(downloads / "qdrant vector db endpoint.txt").read_text(encoding="utf-8").strip(),
        api_key=(downloads / "MrMtd-RAG_api_key.txt").read_text(encoding="utf-8").strip(),
        cloud_inference=True,
        timeout=120,
    )

    results = client.query_points(
        collection_name=COLLECTION,
        prefetch=[
            models.Prefetch(
                query=models.Document(text=question, model=EMBEDDING_MODEL),
                limit=20,
            ),
            models.Prefetch(
                query=models.TextQuery(text=question),
                using="text",
                limit=20,
            ),
        ],
        query=models.FusionQuery(fusion=models.Fusion.RRF),
        limit=5,
        with_payload=["question", "accepted_answer"],
    ).points

    if not results:
        print("No matches found.")
        return

    for rank, point in enumerate(results, start=1):
        payload = point.payload or {}
        print(f"\n--- Match {rank} ---")
        print(f"\nQuestion\n{payload.get('question', '')}")
        print(f"\nAccepted answer\n{payload.get('accepted_answer', '')}")


if __name__ == "__main__":
    main()

#run:
#python3 .\QueryQdrantForBM25orSymantic.py