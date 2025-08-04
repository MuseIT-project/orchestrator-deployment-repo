import json
from csv import DictReader
import requests
from PIL import Image
import base64
import numpy as np
import io
from configuration.config import settings

def get_image(filename):
    filepath = f'images/{filename}'
    with open(filepath, 'rb') as f:
        return f.read()

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

def enrich_with_ollama(image_data, title, style, artist):
    # image = base64.b64encode(image_data).decode('utf-8')
    response = requests.post(
        url=settings.OLLAMA_API_URL + '/api/chat',
        json={
            'model': 'llava:34b',
            'messages': [
                {
                    'role': 'user', 
                    'content': f'Describe what is in this image, which is an artwork titled "{title}", in the style of {style}, created by {artist}',
                    'images': [image_data]
                }
            ],
            'stream': False,
            'raw': True,
        },
        timeout=600
    )
    return response.json()['message']['content']

with open ('WikiArt-Emotions-All.tsv', 'r') as f:
    reader = DictReader(f)
    data = [row for row in reader]

result = []
length = len(data)
current_row = 0

for row in data:
    if row.get('34b'):
        print(f"Skipping {row['ID']} as it is already enriched")
        current_row += 1
        continue
    current_row += 1
    image_data = base64.b64decode(row['image'])
    title = row['Title']
    artist = row['Artist']
    style = row['Category']
    image_name = f"{row['ID']}.jpg"
    try:
        image_data = preprocess_image_to_448x448(image_data=get_image(image_name))
    except Exception as e:
        print(f"Error processing {image_name}: {e}")
        continue
    print(f"Processing {title} by {artist}, {current_row} out of {length} rows")
    try:
        enriched_item = enrich_with_ollama(image_data=image_data, title=title, style=style, artist=artist)
    except Exception as e:
        print(f"Error fetching {image_name}: {e}")
        continue
    row['34b'] = enriched_item
    result.append(row)
    with open('wae_enriched.json', 'w') as f:
        json.dump(result, f)