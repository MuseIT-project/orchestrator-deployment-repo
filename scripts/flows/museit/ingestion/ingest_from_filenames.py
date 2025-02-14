from prefect import flow, task
import boto3
import requests
import json
from prefect.logging import get_run_logger
import random
import base64
import time


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
    response = requests.post(
        url='http://dataversemapper:8099/mapper/',
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
def retrieve_files_for_metadata(item):
    '''
    Retrieves the files for the metadata item
    '''
    access_key = 'zGnGNFec3DKTXiN790kZ'
    bucketname = 'transformedassets'
    secret_key = 'eBZts8xTc3wbU1UEe5E0fHufTiZtBqwMItFbC9oC'
    bucketlocation = item['bucketlocation'].split('.')[0]  
    minio_client = boto3.client('s3', endpoint_url='http://nginxminio:9000', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    keys = minio_client.list_objects(Bucket=bucketname)
    keys = [key['Key'] for key in keys['Contents']]
    filtered_keys = [key for key in keys if key.startswith(bucketlocation)]
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
        url='http://dataverse-importer:8090/importer/',
        json={
            #'doi': f'doi:10.5072/FK2/1{pid}',
            'metadata': refined_metadata,
            'dataverse_information': {
                'base_url': 'https://dataverse.museit.eu',
                'dt_alias': 'transformations',
                'api_token': 'fc66fe9a-c1ec-46c0-a55f-8b2d2636853b'
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
        url='http://metadata-refiner:7878/metadata-refinement/museit',
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
                'base_url': 'https://dataverse.museit.eu',
                'dt_alias': 'transformations',
                'api_token': 'fc66fe9a-c1ec-46c0-a55f-8b2d2636853b'
            }
        })
    }
    time.sleep(1)
    response = requests.post(
        url='http://dataverse-importer:8090/file-upload/',
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
    logger.info(refined_metadata.json())
    ingest = ingest_metadata(refined_metadata=refined_metadata.json())
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
    for item in json_data:
        ingest = transform_ingest_to_dateverse(item=item, mappingjson=mappingjson, templatejson=templatejson)
        output_data[item['title']] = ingest.json()['data']['persistentId']
    with open('output.json', 'w') as outfile:
        json.dump(output_data, outfile)

if __name__ == '__main__':
    ingest_to_dataverse()
