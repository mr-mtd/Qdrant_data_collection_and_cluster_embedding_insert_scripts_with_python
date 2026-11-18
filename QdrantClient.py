#pip install qdrant-client

from pathlib import Path
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Document

uri = Path(r"C:\Users\mrmtd\Downloads\qdrant vector db endpoint").read_text(encoding="utf-8").strip()
api_key = Path(r"C:\Users\mrmtd\Downloads\MrMtd-RAG_api_key").read_text(encoding="utf-8").strip()

# connect to Qdrant Cloud
client = QdrantClient(
    url=uri,
    api_key=api_key,
    cloud_inference=True,
)

# create collection
client.create_collection(
    collection_name="stackoverflow",
    vectors_config=VectorParams(size=384, distance=Distance.COSINE),
)