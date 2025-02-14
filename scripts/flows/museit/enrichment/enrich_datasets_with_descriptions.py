from prefect import task, flow
from prefect.logging import get_run_logger
import json
from pyDataverse.api import NativeApi
import requests
import datetime
import time

@task
def get_metadata_by_doi(doi, api):
    metadata = api.get_dataset(doi, is_pid=True)
    return metadata

@task
def load_foundkeys_enriched_indented():
    with open('foundkeys_enriched_indented.json', 'r') as f:
        foundkeys = json.load(f)
    return foundkeys

@task
def update_description(metadata, description):
    expected_description = {
        "dsDescriptionValue": {
                "typeName": "dsDescriptionValue",
                "multiple": False,
                "typeClass": "primitive",
                "value": description
        }}
    fields = metadata['data']['latestVersion']['metadataBlocks']['citation']['fields']
    for field in fields:
        if field['typeName'] == 'dsDescription':
            if len(field['value']) > 2:
                del field['value'][-1]
            else:
                field['value'].append(expected_description)
    metadata['data']['latestVersion']['metadataBlocks']['citation']['fields'] = fields
    return metadata

@task
def reformat_productiondate_yyyy_mm_dd(production_date):
    return datetime.datetime.strptime(production_date, '%Y').strftime('%Y-%m-%d')

@task
def update_productiondate(metadata, production_date):
    reformatted = reformat_productiondate_yyyy_mm_dd(production_date)
    expected_production_date = {
                "typeName": "productionDate",
                "multiple": False,
                "typeClass": "primitive",
                "value": reformatted
    }
    fields = metadata['data']['latestVersion']['metadataBlocks']['citation']['fields']
    for field in fields:
        if field['typeName'] == 'productionDate':
            fields.pop(fields.index(field))
    fields.append(expected_production_date)
    metadata['data']['latestVersion']['metadataBlocks']['citation']['fields'] = fields
    return metadata

@task
def split_keywords(keywords):
    keywords_list = []
    for keyword_item in keywords:
        if keyword_item[1] >= 0.3:
            keywords_list.append(keyword_item[0])
    return keywords_list
        

@task 
def update_keywords(dataset_metadata, keywords, style):
    keyword_values = []
    for keyword in keywords:
        keyword_values.append({
                "keywordValue": {
                        "typeName": "keywordValue",
                        "multiple": False,
                        "typeClass": "primitive",
                        "value": keyword
                        }
                }
        )
    keyword_values.append(
        {
            "keywordValue": {
                "typeName": "keywordValue",
                "multiple": False,
                "typeClass": "primitive",
                "value": style
            }
        }
    )
    keyword_field = {
        "typeName": "keyword",
        "multiple": True,
        "typeClass": "compound",
        "value": keyword_values
    }
    fields = dataset_metadata['data']['latestVersion']['metadataBlocks']['citation']['fields']
    for field in fields:
        if field['typeName'] == 'keyword':
            fields.pop(fields.index(field))    
    dataset_metadata['data']['latestVersion']['metadataBlocks']['citation']['fields'].append(keyword_field)
    return dataset_metadata

@task
def write_to_file(metadata_json):
    with open('metadata_example.json', 'w') as f:
        json.dump(metadata_json, f, indent=4)

@task
def update_metadata(metadata, doi):
    url = f"https://dataverse.museit.eu/api/datasets/:persistentId/versions/:draft?persistentId={doi}"
    headers = {
        "X-Dataverse-key": "fc66fe9a-c1ec-46c0-a55f-8b2d2636853b",
    }
    response = requests.put(url, headers=headers, json=metadata)
    return response

@task
def clean_metadata(metadata):
    latest_version = metadata['data']['latestVersion']
    del latest_version['createTime']
    del latest_version['lastUpdateTime']
    del latest_version['versionState']
    del latest_version['datasetPersistentId']
    del latest_version['license']['iconUri']
    del latest_version['storageIdentifier']
    del latest_version['id']
    del latest_version['datasetId']
    del latest_version['fileAccessRequest']
    del latest_version['files']
    return latest_version

@flow
def update_dataset(dataset):
    logger = get_run_logger()
    api = NativeApi(api_token="fc66fe9a-c1ec-46c0-a55f-8b2d2636853b", base_url="https://dataverse.museit.eu/")
    metadata = get_metadata_by_doi(dataset['global_id'], api)
    logger.info(metadata.json())
    #write_to_file(metadata.json())
    description = dataset['ollama_description']
    production_date = dataset['yearAsString']
    style = dataset['style']
    keywords = split_keywords(dataset['keywords'])
    metadata_with_description = update_description(metadata.json(), description)
    metadata_with_keywords = update_keywords(metadata_with_description, keywords, style=style)
    metadata_enriched = update_productiondate(metadata_with_keywords, production_date)
    logger.info(metadata_enriched)
    #latestversion = json.dumps(metadata_enriched['data']['latestVersion'])
    cleaned_metadata = clean_metadata(metadata_enriched)
    #logger.info(cleaned_metadata)
    #write_to_file(metadata_enriched)
    time.sleep(2)
    response = update_metadata(doi=dataset['global_id'], metadata=cleaned_metadata)
    logger.info(response.status_code)
    logger.info(response.json())

@flow
def enrich_datasets():
    foundkeys = load_foundkeys_enriched_indented()
    foundkeys_redacted = foundkeys
    for dataset in foundkeys_redacted:
        update_dataset(dataset)

if __name__ == '__main__':
    enrich_datasets()