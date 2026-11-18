#pip install qdrant-client

from pathlib import Path
from qdrant_client import QdrantClient
from qdrant_client import models
from qdrant_client.models import Distance, VectorParams, PointStruct, Document

#uri = Path(r"C:\Users\mrmtd\Downloads\qdrant vector db endpoint").read_text(encoding="utf-8").strip()
#api_key = Path(r"C:\Users\mrmtd\Downloads\MrMtd-RAG_api_key").read_text(encoding="utf-8").strip()
# connect to Qdrant Cloud
client = QdrantClient(
    #url=uri,
    url="https://fda5f3d6-5a98-462c-b2b3-610b87789b67.us-east-2-0.aws.cloud.qdrant.io",
    #api_key=api_key,
    api_key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIiwic3ViamVjdCI6ImFwaS1rZXk6OGIzMGFiOGYtYTM3NC00N2EwLWFiNzctYjRlZjBlZDZiN2EwIn0.BNcaWVLnHdPiFQBqHBGx4Pb2eCwuNaDUnn_chPv5OqM",
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

if not client.collection_exists(collection_name):
    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=384,
            distance=models.Distance.COSINE,
        ),
    )

#support BM25
client.create_payload_index(
    collection_name=collection_name,
    field_name="text",
    field_schema=models.TextIndexParams(
        type=models.TextIndexType.TEXT,
        scoring=True,
    ),
    wait=True,
)

#to work we need to use the following payload structure when inserting
# payload = {
#     "question": question,
#     "accepted_answer": accepted_answer,
#     "text": f"{question}\n\n{accepted_answer}",
# }