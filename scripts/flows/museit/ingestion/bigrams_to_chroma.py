import json
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

# === CONFIGURATION ===
json_file = "adj_noun_bigrams.json"
chroma_collection_name = "bigrams_embeddings"

# === LOAD PHRASES FROM JSON ===
with open(json_file, "r") as f:
    phrases = json.load(f)

# === LOAD EMBEDDING MODEL ===
model = SentenceTransformer('all-mpnet-base-v2')

# === GENERATE EMBEDDINGS ===
embeddings = model.encode(phrases, show_progress_bar=True)

# === CONNECT TO CHROMADB ===
client = chromadb.HttpClient(
    host="localhost",
    port=8000,
    ssl=False,
    settings=Settings()
)

# === CREATE OR GET COLLECTION ===
collection = client.get_or_create_collection(name=chroma_collection_name)

# === PREPARE METADATA AND IDS ===
ids = [f"id_{i}" for i in range(len(phrases))]
metadatas = [{"label": phrase} for phrase in phrases]

# === INGEST DATA ===
collection.add(
    ids=ids,
    documents=phrases,
    embeddings=embeddings,
    metadatas=metadatas
)

print(f"Ingested {len(phrases)} phrases into ChromaDB collection '{chroma_collection_name}'")
