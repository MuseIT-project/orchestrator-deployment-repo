import json
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings

# === Step 1: Load JSON Data ===
INPUT_FILE = "foundkeys_enriched_34b_hsi_keywords_cleaned.json"

with open(INPUT_FILE, 'r', encoding='utf-8') as f:
    data = json.load(f)

# === Step 2: Extract Relevant Fields ===
entries = []
for item in data:
    try:
        entry = {
            "id": item["contentId"],
            "title": item.get("title", ""),
            "artist": item.get("artistName", ""),
            "style": item.get("style", ""),
            "completitionYear": item.get("completitionYear", ""),
            "keywords_34b": item.get("keywords_34b", []),
            "ollama_description_34b": item["ollama_description_34b"],
        }
        entries.append(entry)
    except KeyError:
        # Skip entries missing required fields
        continue

# === Step 3: Compute Embeddings with MPNet ===
model = SentenceTransformer("all-mpnet-base-v2")
texts = [e["ollama_description_34b"] for e in entries]
print("Amount of texts to encode:", len(texts))
embeddings = model.encode(texts, convert_to_numpy=True)

# === Step 4: Connect to ChromaDB ===
client = chromadb.HttpClient(
    host="localhost",
    port=8000,
    ssl=False,
    settings=Settings()
)

# === Step 5: Create New Collection ===
collection_name = "hb_300_embeddings"
if collection_name in [col.name for col in client.list_collections()]:
    client.delete_collection(collection_name)
collection = client.create_collection(collection_name)

# === Step 6: Insert Data into ChromaDB ===
ids = [str(e["id"]) for e in entries]
metadatas = [
    {
        "title": e["title"],
        "style": e["style"],
        "artist": e["artist"],
        "completitionYear": e["completitionYear"],
        "keywords_34b": e["keywords_34b"],
        "description_34b": e["ollama_description_34b"],
    }
    for e in entries
]

collection.add(
    ids=ids,
    embeddings=embeddings.tolist(),
    metadatas=metadatas,
    documents=texts
)

print(f"Successfully inserted {len(ids)} records into collection '{collection_name}'.")
