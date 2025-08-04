from keybert import KeyBERT
import json

from sentence_transformers import SentenceTransformer

# Load a pre-trained embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")  # Small, fast model

# Define words to encode
time_words = ["noon", "afternoon", "evening", "nightfall", "midnight", "dawn", "morning", "mid-morning"]
location_words = ["urban", "rural", "portrait", "everyday", "still-life", "abstract", "landscape", "seascape"]
age_words = ["adult", "aged", "old", "newborn", "baby", "toddler", "young", "adolescent"]
person_words = ["people", "women", "plants", "herd", "illustration", "man"]

result_dict = {}

# Generate embeddings
for word_list in [time_words, location_words, age_words, person_words]:
    for word in word_list:
        embeddings = model.encode(word)
        result_dict[word] = embeddings

def extract_keywords(text, top_n=5, filter_words=None):
    if filter_words is None:
        filter_words = {"painting", "image", "portrait", "image", "portrayal", "canvas", "art"}
    
    kw_model = KeyBERT()
    keywords = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 1), stop_words='english', top_n=top_n)
    
    return [kw[0] for kw in keywords if kw[0] not in filter_words]

def calculate_cosine_similarity(embedding, embeddings_dict = result_dict):
    """
    Calculate the cosine similarity between two embeddings
    """
    from sklearn.metrics.pairwise import cosine_similarity

    # Calculate the cosine similarity between the embeddings
    cosine_similarities = {}
    for word, emb in embeddings_dict.items():
        similarity = cosine_similarity([embedding], [emb])[0][0]
        if similarity > 0.55:
            cosine_similarities[word] = similarity
    return cosine_similarities

if __name__ == "__main__":
    with open('foundkeys_origin.json', 'r') as json_file:
        json_data = json.load(json_file)
    for item in json_data[12:16]:
        text = []
        result_data = {}
        matches = {}
        print("Processing item:", item['title'])
        sample_text = item['ollama_description'].split(' ')
        for word in sample_text:
            result_data[word] = model.encode(word)
        for k,v in result_data.items():
            for word, embeddings in result_dict.items():
                cosine_similarities = calculate_cosine_similarity(v, result_dict)
                sorted_similarities = sorted(cosine_similarities.items(), key=lambda item: item[1], reverse=True)
                matches[k] = sorted_similarities[0] if sorted_similarities else None
        filtered_matches = {k: v for k, v in matches.items() if v is not None}
        from pprint import pprint
        pprint(filtered_matches)