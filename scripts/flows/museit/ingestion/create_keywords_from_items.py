#!/usr/bin/env python3

from prefect import task, flow
import json
from prefect.logging import get_logger
from keybert import KeyBERT


@task
def process_description_into_keywords(description):
    model = KeyBERT('distilbert-base-nli-mean-tokens')
    keywords = model.extract_keywords(
        description,
        keyphrase_ngram_range=(2, 2),
        use_mmr=True,
        diversity=0.7,
        stop_words=['painting', 'image', 'depiction']
    )
    return keywords

@task
def load_enriched_json(filepath='foundkeys_origin.json'):
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data

@task
def save_enriched_json(data, filepath='foundkeys_origin.json'):
    with open(filepath, 'w') as file:
        json.dump(data, file)

@flow
def generate_keywords_per_item():
    logger = get_logger()
    enriched_data = load_enriched_json()
    for item in enriched_data:
        keywords = process_description_into_keywords(item['ollama_description'])
        item['keywords'] = keywords
    save_enriched_json(enriched_data)

if __name__ == '__main__':
    generate_keywords_per_item()
