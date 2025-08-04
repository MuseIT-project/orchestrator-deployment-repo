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
from configuration.config import settings

@task
def load_foundkeys():
    with open('foundkeys_enriched_2.json', 'r') as f:
        return json.load(f)

@task
def load_enriched_data():
    try:
        with open('foundkeys_enriched_34b.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

@task
def retrieve_file_for_metadata(item):
    logger = get_run_logger()
    access_key = settings.MINIO_ACCESS_KEY
    bucketname = settings.MINIO_BUCKET_NAME
    secret_key = settings.MINIO_SECRET_KEY
    filename = f"{item['bucketlocation']}"
    logger.info(f"Retrieving {item['title']}")
    minio_client = boto3.client('s3', endpoint_url=settings.MINIO_ENDPOINT_URL, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    try:
        filedata = minio_client.get_object(Bucket=bucketname, Key=filename)
        return filedata['Body'].read()
    except Exception as e:
        print(f"Error fetching {filename}: {e}")
        return
    
@task
def preprocess_image_to_448x448(image_data):
    target_size = 672
    
    # Open image from bytes
    image = Image.open(io.BytesIO(image_data)).convert("RGB")
    
    # Resize while preserving aspect ratio
    image.thumbnail((target_size, target_size), Image.LANCZOS)
    
    # Create a blank image with a black background
    new_image = Image.new("RGB", (target_size, target_size), (0, 0, 0))
    
    # Center the resized image
    paste_x = (target_size - image.size[0]) // 2
    paste_y = (target_size - image.size[1]) // 2
    new_image.paste(image, (paste_x, paste_y))
    
    # Convert to NumPy array (optional step)
    image_array = np.array(new_image)
    
    # Convert back to image and encode as base64 PNG
    final_image = Image.fromarray(image_array)
    buffered = io.BytesIO()
    final_image.save(buffered, format="PNG")
    base64_image = base64.b64encode(buffered.getvalue()).decode("utf-8")
    
    return base64_image

@task
def enrich_with_ollama(image_data, title, style, artist):
    logger = get_run_logger()
    # image = base64.b64encode(image_data).decode('utf-8')
    artist_reversed = ' '.join(artist.strip().split(' ')[-1:] + artist.strip().split(' ')[:-1])
    response = requests.post(
        url=settings.OLLAMA_API_URL + '/api/chat',
        json={
            'model': 'llava:34b',
            'messages': [
                {
                    'role': 'user', 
                    'content': f'Describe what is in this image, which is an artwork titled "{title}", in the style of {style}, by {artist_reversed}.',
                    'images': [image_data]
                }
            ],
            'stream': False,
            'raw': True,
        },
        timeout=600
    )
    logger.info(str(title))
    logger.info(response.text)
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
    optimized_image_data = preprocess_image_to_448x448(image_data)
    enriched_data = enrich_with_ollama(optimized_image_data, item['title'], item['style'], item['artistName'])
    item['ollama_description_34b_2'] = enriched_data
    return item

@flow
def enrich_metadata():
    foundkeys = load_foundkeys()
    enriched = load_enriched_data()
    enrichedkeys = [item['contentId'] for item in enriched]
    for item in foundkeys:
        if item['contentId'] in enrichedkeys:
            print(f"Skipping {item['contentId']}")
            continue
        else:
            enriched_item = enrich_item(item)
            enriched.append(enriched_item)
        with open('foundkeys_enriched_34b.json', 'w') as f:
            json.dump(enriched, f, indent=4, ensure_ascii=False)

def deploy_flow():
    enrich_metadata.deploy(
        name="enrich-metadata",
        work_pool_name="default",
    )

if __name__ == '__main__':
    enrich_metadata()