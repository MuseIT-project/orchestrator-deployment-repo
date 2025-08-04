from prefect import flow, task
from normalize_years import normalize_year
import boto3
import requests
import json
import random
import base64
import time
import html
from configuration.config import settings


@task
def map_item_to_metadata(item, mapping, template):
    '''
    Maps an item to metadata using the dataverse mapper service
    '''
    asset = {}
    asset['title'] = html.unescape(item.get('Title'))
    asset['author'] = html.unescape(item.get('Artist'))
    description_fields = ['34b', 'Category', 'Style']
    asset['descriptionFields'] = []
    for field in description_fields:
        asset['descriptionFields'].append(item.get(field))
    asset['keywords'] = [keyword.strip() for keyword in item.get('keywords_80b', '').split(',') if keyword.strip()]
    asset['productionDate'] = str(normalize_year(item.get('Year')))
    response = requests.post(
        url=settings.DATAVERSE_MAPPER_URL + '/mapper',
        json={
            'metadata': asset,
            'template': template,
            'mapping': mapping,
        }
    )
    return response

@task
def ingest_metadata(refined_metadata):
    '''
    Ingests the metadata into the dataverse
    '''
    time.sleep(1)
    response = requests.post(
        url=settings.DATAVERSE_IMPORTER_URL + '/importer/',
        json={
            'metadata': refined_metadata,
            'dataverse_information': {
                'base_url': settings.DATAVERSE_BASE_URL,
                'dt_alias': settings.DATAVERSE_DT_ALIAS,
                'api_token': settings.DATAVERSE_API_TOKEN
            }
        }
    )
    return response

@task
def refine_metadata(mapped_metadata):
    '''
    Does refinement on the metadata
    '''
    response = requests.post(
        url=settings.METADATA_REFINEMENT_URL + '/museit',
        json={
            'metadata': mapped_metadata,
        }
    )
    return response

@flow
def transform_ingest_to_dateverse(item, mappingjson, templatejson):
    '''
    Transforms an item to dataverse metadata and ingests it
    '''
    metadata = map_item_to_metadata(item=item, mapping=mappingjson, template=templatejson)
    ingest = ingest_metadata(refined_metadata=metadata.json())
    return ingest

@flow
def ingest_to_dataverse():
    '''
    Main task to ingest into Dataverse.
    '''
    output_data = {}
    with open('wae_mapping.json', 'r') as mapping:
        mappingjson = json.load(mapping)
    with open('museittemplate.json', 'r') as template:
        templatejson = json.load(template)
    with open('wae_keywords.json', 'r') as json_file:
        json_data = json.load(json_file)
    for item in json_data: #Skip first 4064 items
        ingest = transform_ingest_to_dateverse(item=item, mappingjson=mappingjson, templatejson=templatejson)
        output_data[item['Title']] = ingest.json()['data']['persistentId']
    with open('wae_ingest_output.json', 'w') as outfile:
        json.dump(output_data, outfile, indent=4, ensure_ascii=False)

if __name__ == '__main__':
    ingest_to_dataverse()
