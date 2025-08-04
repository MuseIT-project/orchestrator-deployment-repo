from prefect import flow, task
import boto3
import requests
import json
from prefect.logging import get_run_logger
import random
import base64
import time
from configuration.config import settings


@task
def map_item_to_metadata(item, mapping, template):
    '''
    Maps an item to metadata using the dataverse mapper service
    '''
    logger = get_run_logger()
    asset = {}
    asset['title'] = item.get('title')
    asset['author'] = item.get('artistName')
    description_fields = ['ollama_description', 'keywords', 'tags', 'style']
    for field in description_fields:
        asset[field] = item.get(field)
    filtered_keywords = []
    for keyword in asset['keywords']:
        if keyword[1] > 0.3:
            filtered_keywords.append(keyword[0])
    asset['keywords'] = "+".join(filtered_keywords)
    asset['description'] = f"{asset['ollama_description']} + {asset['tags']} + {asset['style']}"
    asset['productionDate'] = item.get('yearAsString')
    asset['alternativeTitle'] = item.get('bucketlocation')
    response = requests.post(
        url=settings.DATAVERSE_MAPPER_URL + '/mapper',
        json={
            'metadata': asset,
            'template': template,
            'mapping': mapping,
        }
    )
    logger.info("Response: ", response.json())
    return response

@task
def extract_valuable_keywords(item):
    '''
    Splits the keywords from the item and returns the ones with a score higher than 0.3
    '''
    keywords = item['keywords']
    return [keyword[0] for keyword in keywords if keyword[1] > 0.3]

@task
def retrieve_original_file(item):
    '''
    Retrieves the original file from the item
    '''
    access_key = settings.ACCESS_KEY
    bucketname = '300originals'
    secret_key = settings.SECRET_KEY
    bucketlocation = item['bucketlocation']
    minio_client = boto3.client('s3', endpoint_url=settings.MINIO_ENDPOINT_URL, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    file = minio_client.get_object(Bucket=bucketname, Key=bucketlocation)
    filedata = file['Body'].read()
    return (bucketlocation, filedata)



@task
def retrieve_files_for_metadata(item):
    '''
    Retrieves the files for the metadata item
    '''
    access_key = settings.ACCESS_KEY
    bucketname = 'transformedassets'
    secret_key = settings.SECRET_KEY
    bucketlocation = item['bucketlocation'].split('.')[0]
    minio_client = boto3.client('s3', endpoint_url=settings.MINIO_ENDPOINT_URL, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    keys = []
    paginator = minio_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucketname):
        keys.extend(page.get('Contents', []))
    
    keys = [key['Key'] for key in keys]
    filtered_keys = [key for key in keys if bucketlocation in key]
    print("Filtered keys: ", filtered_keys)
    print(bucketlocation)
    files_data = []
    for key in filtered_keys:
        file = minio_client.get_object(Bucket=bucketname, Key=key)
        filedata = file['Body'].read()
        files_data.append((key, filedata))
    return files_data
    #the bagpipe is angry

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

@task
def add_file(ch_file, doi, filename):
    '''
    Imports a file into the dataverse
    '''
    files = {'file': (filename, ch_file)}
    data = {
        'json_data': json.dumps({
            'doi': doi,
            'dataverse_information': {
                'base_url': settings.DATAVERSE_BASE_URL,
                'dt_alias': settings.DATAVERSE_DT_ALIAS,
                'api_token': settings.DATAVERSE_API_TOKEN
            }
        })
    }
    time.sleep(1)
    response = requests.post(
        url=settings.DATAVERSE_FILE_UPLOAD_URL + '/file-upload/',
        files=files,
        data=data
    )
    return response

@flow
def transform_ingest_to_dateverse(item, mappingjson, templatejson):
    '''
    Transforms an item to dataverse metadata and ingests it
    '''
    logger = get_run_logger()
    metadata = map_item_to_metadata(item=item, mapping=mappingjson, template=templatejson)
    refined_metadata = refine_metadata(mapped_metadata=metadata.json())
    ch_files = retrieve_files_for_metadata(item)
    if ch_files == []:
        raise ValueError("No files found for metadata", item['title'])
    logger.info(item['title'])
    ingest = ingest_metadata(refined_metadata=refined_metadata.json())
    logger.info(ingest.json())
    original_file = retrieve_original_file(item)
    add_file(ch_file=original_file[1], doi=ingest.json()['data']['persistentId'], filename=original_file[0])
    for item in ch_files:
        filedata = item[1]
        filename = item[0]
        add_file(ch_file=filedata, doi=ingest.json()['data']['persistentId'], filename=filename)
    return ingest

@flow
def ingest_to_dataverse():
    '''
    Main task to ingest into Dataverse.
    '''
    logger = get_run_logger()
    output_data = {}
    with open('museitmapping.json', 'r') as mapping:
        mappingjson = json.load(mapping)
    with open('museittemplate.json', 'r') as template:
        templatejson = json.load(template)
    with open('foundkeys_origin.json', 'r') as json_file:
        json_data = json.load(json_file)
    for item in json_data[-21:]:
        ingest = transform_ingest_to_dateverse(item=item, mappingjson=mappingjson, templatejson=templatejson)
        output_data[item['title']] = ingest.json()['data']['persistentId']
    with open('output.json', 'w') as outfile:
        json.dump(output_data, outfile, indent=4, ensure_ascii=False)

if __name__ == '__main__':
    ingest_to_dataverse()
