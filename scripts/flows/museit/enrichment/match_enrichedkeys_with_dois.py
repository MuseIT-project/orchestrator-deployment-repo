#!/usr/bin/env python3

from prefect import task, flow
from prefect.logging import get_run_logger
from pyDataverse.api import SearchApi as search
import json

@task
def load_enriched_json(filepath='foundkeys_merged.json'):
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data

@task
def save_with_global_ids(data, filepath='foundkeys_with_global_ids.json'):
    with open(filepath, 'w') as file:
        json.dump(data, file, indent=4)

@task
def search_dataverse_on_item_title_retrieve_doi(item):
    # This is a fake function that would search the dataverse for a title and return a DOI
    logger = get_run_logger()
    api = search('https://dataverse.museit.eu', 'fc66fe9a-c1ec-46c0-a55f-8b2d2636853b')
    search_str = item['title']
    results = api.search(q_str=search_str, subtree='transformations', query_entities=['dataset'])
    return results.json()

@task
def filter_dataset_global_id_from_response(response):
    for item in response['data']['items']:
        if item['type'] == 'dataset':
            return item['global_id']

@flow
def search_for_titles():
    enriched_data = load_enriched_json()
    matched_data = []
    for item in enriched_data:
        response = search_dataverse_on_item_title_retrieve_doi(item)
        global_id = filter_dataset_global_id_from_response(response)
        item['global_id'] = global_id
        matched_data.append(item)
    save_with_global_ids(matched_data)

if __name__ == '__main__':
    search_for_titles()
