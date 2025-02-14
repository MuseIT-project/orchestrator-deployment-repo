import requests
import base64
import boto3

def retrieve_file_for_metadata(item):
    access_key = 'zGnGNFec3DKTXiN790kZ'
    bucketname = 'selectedassets'
    secret_key = 'eBZts8xTc3wbU1UEe5E0fHufTiZtBqwMItFbC9oC'
    filename = f"{item['contentId']}.jpg"
    minio_client = boto3.client('s3', endpoint_url='http://localhost:9000', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    try:
        filedata = minio_client.get_object(Bucket=bucketname, Key=filename)
        return filedata['Body'].read()
    except Exception as e:
        print(f"Error fetching {filename}: {e}")
        return

def enrich_with_ollama(image_data, title):
    image = base64.b64encode(image_data).decode('utf-8')
    response = requests.post(
        url='http://localhost:11434/api/chat',
        json={
            'model': 'llava-llama3:latest',
            'messages': [
                {
                    'role': 'user', 
                    'content': 'Describe this painting.',
                    'images': [image]
                }
            ],
            'stream': False,
            'raw': True,

        }
    )
    print(title)
    print(response.json()['message']['content'])
    return response.json()

def main():
    item = {
        'contentId': '235555',
        "title": "Elizabeth Griffiths Smith Hopper, The Artist's Mother"
    }
    image_data = retrieve_file_for_metadata(item)
    if not image_data:
        item['ollama_description'] = 'Error fetching image data'
        return item
    response = enrich_with_ollama(image_data, item['title'])

if __name__ == "__main__":
    main()



