import requests
import json
import io
from configuration.config import settings

def load_data():
    try:
        with open('foundkeys_enriched_34b.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    
    
def load_enriched_data():
    try:
        with open('foundkeys_enriched_34b_hsi_keywords.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
      
def enrich_with_ollama(text):
    prompt = f"""
    You are an AI that categorizes text based on four predefined lists. You must only choose one word from each list provided and nothing else. Any word not in the list is not allowed.

    Here are the lists:
    - Time List: ["Noon", "Dusk", "Evening", "Nightfall", "Midnight", "Dawn", "Morning", "Midmorning"]
    - Age List: ["Adult", "Aged", "Old", "Newborn", "Baby", "Toddler", "Young", "Adolescent"]
    - People List: ['people', 'men', 'man', 'women', 'woman', 'group', 'illustration', 'plants']
    - Type List: ["Urban", "Rural", "Portrait", "Everyday", "Still life", "Abstract", "Landscape", "Seascape"]

    Instructions:
    1. Read the text below.
    2. Select one word from each list that best fits the description of the text.
    3. If you think no good fits are available, you can choose "None" instead.
    4. Return only four words, one from each list.
    5. Format it as a dictionary, where you use the list names as keys and the selected words as values.

    Text:
    "{text}"

    Your response (four words only):
    """
    response = requests.post(
        url=settings.OLLAMA_API_URL + '/api/chat',
        json={
            'model': 'llama2:70b',
            'messages': [
                {
                    'role': 'user', 
                    'content': prompt,
                }
            ],
            'stream': False,
            'raw': True,
        },
        timeout=600
    )
    return response.json()['message']['content']

def generate_keywords(text):
    prompt = f"""
    You are an AI that generates keywords based on a given text. Your task is to extract relevant keywords from the text provided.
    Here are the instructions:
    1. Read the text below.
    2. Extract 5 relevant keywords that best describe the content of the text.
    3. Return the keywords in a list format.
    4. Do not include any additional text or explanations.
    Text:
    "{text}"
    Your response (list of keywords only):
    """
    response = requests.post(
        url=settings.OLLAMA_API_URL + '/api/chat',
        json={
            'model': 'llama2:70b',
            'messages': [
                {
                    'role': 'user', 
                    'content': prompt,
                }
            ],
            'stream': False,
            'raw': True,
        },
        timeout=600
    )
    return response.json()['message']['content']

def enrich_metadata():
    data = load_data()
    enriched = load_enriched_data()
    enrichedkeys = [item['contentId'] for item in enriched]
    for item in data:
        thirtyfourb = item['ollama_description_34b_2']
        if item['contentId'] in enrichedkeys:
            print(f"Skipping {item['contentId']}")
            continue
        else:
            print("Enriching", item['contentId'])
            item['hsi_34b'] = enrich_with_ollama(text=thirtyfourb)
            item['keywords_34b'] = generate_keywords(text=thirtyfourb)
            enriched.append(item)
        with open('foundkeys_enriched_34b_hsi_keywords.json', 'w') as f:
            json.dump(enriched, f, indent=4, ensure_ascii=False)

if __name__ == '__main__':
    enrich_metadata()