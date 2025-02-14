from prefect import flow, task
from openpyxl.reader.excel import load_workbook
import boto3
import requests
import json
from prefect.logging import get_run_logger
import random
import base64


@task
def map_item_to_metadata(item, mapping, template):
    logger = get_run_logger()
    asset = {}
    asset['title'] = item.get('title')
    asset['author'] = item.get('artistName')
    description = item.get('description')
    tags = item.get('tags')
    placeholder = "placeholder"
    asset['description'] = description if description is not None else (tags if tags is not None else placeholder)
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
def retrieve_file_for_metadata(item):
    access_key = 'zGnGNFec3DKTXiN790kZ'
    bucketname = 'steen'
    secret_key = 'eBZts8xTc3wbU1UEe5E0fHufTiZtBqwMItFbC9oC'
    filename = item['contentId']
    year = item.get('completitionYear')
    path = f"{year}/{filename}.jpg"
    print("Path: ", path)
    minio_client = boto3.client('s3', endpoint_url='http://nginxminio:9000', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    filedata = minio_client.get_object(Bucket=bucketname, Key=path)
    return filedata['Body'].read()
    #the bagpipe is angry

@task
def ingest_metadata(refined_metadata):
    pid = random.randint(1,10000)
    response = requests.post(
        url='http://dataverse-importer:8090/importer/',
        json={
            #'doi': f'doi:10.5072/FK2/1{pid}',
            'metadata': refined_metadata,
            'dataverse_information': {
                'base_url': 'http://dev_dataverse:8080',
                'dt_alias': 'root',
                'api_token': '5ad66e37-8e62-45de-82cd-8bed64895003'
            }
        }
    )
    return response

@task
def add_file(ch_file, doi, filename):
    files = {'file': (filename, ch_file)}
    data = {
        'json_data': json.dumps({
            'doi': doi,
            'dataverse_information': {
                'base_url': 'http://dev_dataverse:8080',
                'dt_alias': 'root',
                'api_token': '5ad66e37-8e62-45de-82cd-8bed64895003'
            }
        })
    }
    response = requests.post(
        url='http://dataverse-importer:8090/file-upload/',
        files=files,
        data=data
    )
    print(repr(response.request))
    return response

@flow
def ingest_to_dataverse():
    logger = get_run_logger()
    with open('museitmapping.json', 'r') as mapping:
        mappingjson = json.load(mapping)
    with open('museittemplate.json', 'r') as template:
        templatejson = json.load(template)
    with open('jan-steen.json', 'r') as json_file:
        json_data = json.load(json_file)
    for item in json_data:
        metadata = map_item_to_metadata(item=item, mapping=mappingjson, template=templatejson)
        ch_file = retrieve_file_for_metadata(item)
        ingest = ingest_metadata(refined_metadata=metadata.json())
        print(ingest.json())
        add_file(ch_file=ch_file, doi=ingest.json()['data']['persistentId'], filename=f"{item['contentId']}.jpg")
if __name__ == '__main__':
    ingest_to_dataverse()
