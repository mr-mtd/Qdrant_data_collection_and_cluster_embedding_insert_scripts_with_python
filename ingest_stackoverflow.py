import argparse
import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from qdrant_client import QdrantClient, models

COLLECTION = "stackoverflow"
EMBEDDING_MODEL = "sentence-transformers/all-minilm-l6-v2"
BATCH_SIZE = 32


def make_point(record):
    question = record["question"]
    accepted_answer = record["accepted_answer"]
    text = f"{question}\n\n{accepted_answer}"

    question_id = record.get("question_id")
    point_id = (
        int(question_id)
        if question_id is not None
        else str(uuid5(NAMESPACE_URL, f"stackoverflow:{question}"))
    )

    return models.PointStruct(
        id=point_id,
        vector=models.Document(text=text, model=EMBEDDING_MODEL),
        payload={
            "question": question,
            "accepted_answer": accepted_answer,
            "text": text,
        },
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", type=Path, help="File containing a JSON array")
    args = parser.parse_args()

    records = json.loads(args.input_file.read_text(encoding="utf-8-sig"))
    if not isinstance(records, list):
        parser.error("The input file must contain a JSON array.")

    points = []
    for row_number, record in enumerate(records, start=1):
        try:
            points.append(make_point(record))
        except (KeyError, TypeError, ValueError) as error:
            parser.error(f"Invalid JSON record at position {row_number}: {error}")

    downloads = Path.home() / "Downloads"
    client = QdrantClient(
        url=(downloads / "qdrant vector db endpoint.txt").read_text(encoding="utf-8").strip(),
        api_key=(downloads / "MrMtd-RAG_api_key.txt").read_text(encoding="utf-8").strip(),
        cloud_inference=True,
        timeout=120,
    )

    for start in range(0, len(points), BATCH_SIZE):
        batch = points[start : start + BATCH_SIZE]
        client.upsert(collection_name=COLLECTION, points=batch, wait=True)
        print(f"Uploaded {start + len(batch)}/{len(points)} records", flush=True)


if __name__ == "__main__":
    main()

#to run:
# python -m pip install -U qdrant-client
# python .\ingest_stackoverflow.py "C:\Users\mrmtd\OneDrive\Documents\GitHub\Qdrant_data_collection_and_cluster_embedding_insert_scripts_with_python\kafka_questions_and_answers.txt"