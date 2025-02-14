from prefect import flow, task
import boto3
import requests
import json
from prefect.logging import get_run_logger
import random
import base64
from PIL import Image
import numpy as np
import io

@task
def load_foundkeys():
    with open('foundkeys.json', 'r') as f:
        return json.load(f)

@task
def load_enriched_data():
    with open('foundkeys_enriched.json', 'r') as f:
        return json.load(f)

    
@task
def retrieve_file_for_metadata(item):
    logger = get_run_logger()
    access_key = 'zGnGNFec3DKTXiN790kZ'
    bucketname = 'selectedassets'
    secret_key = 'eBZts8xTc3wbU1UEe5E0fHufTiZtBqwMItFbC9oC'
    filename = f"{item['contentId']}.jpg"
    logger.info(f"Retrieving {item['title']}")
    minio_client = boto3.client('s3', endpoint_url='http://nginxminio:9000', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    try:
        filedata = minio_client.get_object(Bucket=bucketname, Key=filename)
        return filedata['Body'].read()
    except Exception as e:
        print(f"Error fetching {filename}: {e}")
        return
    
@task
def preprocess_image_to_244x244(image_data):
    image = Image.open(io.BytesIO(image_data))
    image = image.resize((244, 244))
    image = np.array(image)
    # Now return it as base64
    image = Image.fromarray(image)
    buffered = io.BytesIO()
    image.save(buffered, format="PNG")
    base64_image = base64.b64encode(buffered.getvalue()).decode("utf-8")
    return base64_image

@task
def enrich_with_ollama(image_data, title):
    logger = get_run_logger()
    # image = base64.b64encode(image_data).decode('utf-8')
    response = requests.post(
        url='http://164.90.178.121:11434/api/chat',
        json={
            'model': 'llava-llama3:latest',
            'messages': [
                {
                    'role': 'user', 
                    'content': 'Describe this painting.',
                    'images': [image_data]
                }
            ],
            'stream': False,
            'raw': True,
        },
        timeout=600
    )
    logger.info(str(response.json()['message']['content']))
    logger.info(str(title))
    return response.json()['message']['content']

@task
def save_foundkeys_data_intermittently(enriched_item):
    with open('foundkeys_enriched.json', 'r') as f:
        existing_data = json.load(f)
    existing_data.append(enriched_item)
    with open('foundkeys_enriched.json', 'w') as f:
        json.dump(existing_data, f)

@flow
def enrich_item(item):
    image_data = retrieve_file_for_metadata(item)
    if not image_data:
        item['ollama_description'] = 'Error fetching image data'
        return item
    optimized_image_data = preprocess_image_to_244x244(image_data)
    enriched_data = enrich_with_ollama(optimized_image_data, item['title'])
    item['ollama_description'] = enriched_data
    return item

@flow
def enrich_metadata():
    foundkeys = load_foundkeys()
    enriched = load_enriched_data()
    enrichedkeys = [item['contentId'] for item in enriched]
    for k, v in foundkeys.items():
        if k in enrichedkeys:
            continue
        else:
            enriched_item = enrich_item(v)
            enriched.append(enriched_item)
            with open('foundkeys_enriched.json', 'w') as f:
                json.dump(enriched, f)

def deploy_flow():
    enrich_metadata.deploy(
        name="enrich-metadata",
        work_pool_name="default",
    )

if __name__ == '__main__':
    enrich_metadata()