#pip install qdrant-client

from pathlib import Path
from qdrant_client import QdrantClient
from qdrant_client import models
from qdrant_client.models import Distance, VectorParams, PointStruct, Document

uri = Path(r"C:\Users\mrmtd\Downloads\qdrant vector db endpoint.txt").read_text(encoding="utf-8").strip()
api_key = Path(r"C:\Users\mrmtd\Downloads\MrMtd-RAG_api_key.txt").read_text(encoding="utf-8").strip()

# connect to Qdrant Cloud
client = QdrantClient(
    url=uri,
    api_key=api_key,
    cloud_inference=True,
)

# create collection
# client.create_collection(
#     collection_name="stackoverflow",
#     vectors_config=VectorParams(size=384, distance=Distance.COSINE),
# )

### after running above I realize I need to support BM25 as well...
#using below to support
collection_name = "stackoverflow"

# if not client.collection_exists(collection_name):
#     client.create_collection(
#         collection_name=collection_name,
#         vectors_config=models.VectorParams(
#             size=384,
#             distance=models.Distance.COSINE,
#         ),
#     )

#support BM25 - need to run after my insert script as insert script only supports
#symantic search
client.create_payload_index(
    collection_name=collection_name,
    field_name="text",
    field_schema=models.TextIndexParams(
        type=models.TextIndexType.TEXT,
        scoring=True,
    ),
    wait=True,
)

print("BM25 full-text index created!")

#to work we need to use the following payload structure when inserting
# payload = {
#     "question": question,
#     "accepted_answer": accepted_answer,
#     "text": f"{question}\n\n{accepted_answer}",
# }


