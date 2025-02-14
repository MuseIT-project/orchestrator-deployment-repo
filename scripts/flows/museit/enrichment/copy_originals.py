import json
import boto3

#with open('foundkeys_origin.json', 'r') as f:
#    foundkeys = json.load(f)

class GenericClient:

    def __init__(self, endpoint_url, access_key, secret_key):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.client = boto3.client('s3', endpoint_url=self.endpoint_url, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)

class LocalMinioClient(GenericClient):

    def __init__(self, endpoint_url: str = "http://localhost:9000", access_key: str = "zGnGNFec3DKTXiN790kZ", secret_key: str = "eBZts8xTc3wbU1UEe5E0fHufTiZtBqwMItFbC9oC"):
        super().__init__(endpoint_url, access_key, secret_key)

    def get_object(self, bucketname, key):
        return self.client.get_object(Bucket=bucketname, Key=key)
    
    def copy_object(self, bucketname, key, copy_source):
        return self.client.copy_object(Bucket=bucketname, Key=key, CopySource=copy_source)
    
    def list_objects(self, bucketname):
        objects = []
        continuation_token = None

        while True:
            if continuation_token:
                response = self.client.list_objects_v2(Bucket=bucketname, ContinuationToken=continuation_token)
            else:
                response = self.client.list_objects_v2(Bucket=bucketname)

            objects.extend(response.get('Contents', []))

            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
            else:
                break

        return objects
    
    
client = LocalMinioClient()

objects = client.list_objects('transformedassets')
keys = [obj['Key'] for obj in objects]

search_term = "190179"
matching_keys = [key for key in keys if search_term in key]
print(matching_keys)
print("Length: ", len(matching_keys))
    