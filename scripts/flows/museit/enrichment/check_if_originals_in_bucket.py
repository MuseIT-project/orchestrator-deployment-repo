import json
import boto3

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

    def put_object(self, bucketname, key, data):
        return self.client.put_object(Bucket=bucketname, Key=key, Body=data)

    def list_objects(self, bucketname):
        objects = []
        continuation_token = None

        while True:
            if continuation_token:
                response = self.client.list_objects_v2(Bucket=bucketname, ContinuationToken=continuation_token)
            else:
                response = self.client.list_objects_v2(Bucket=bucketname)

            objects.extend(response.get('Contents', []))

            if response.get('IsTruncated'):  # Check if there are more objects to retrieve
                continuation_token = response.get('NextContinuationToken')
            else:
                break

        return objects

    def delete_object(self, bucketname, key):
        return self.client.delete_object(Bucket=bucketname, Key=key)

def check_if_originals_in_bucket(originals, bucketname='300assets'):

    minio = LocalMinioClient()

    present = []
    missing = []

    list_objects = minio.list_objects(bucketname)
    keys = [obj['Key'] for obj in list_objects]

    for original in originals:
        print("Checking for ", original)
        if original in keys:
            present.append(original)
            keys.remove(original)
        else:
            missing.append(original)
            #keys.remove(original)

    return present, missing, keys

with open ("foundkeys_origin.json", "r") as f:
    foundkeys_origin = json.load(f)

originals = [item['bucketlocation'] for item in foundkeys_origin]

present, missing, keys = check_if_originals_in_bucket(originals)
#print("Present: ", present)
print("Missing: ", missing)
print("Count missing: ", len(missing))
print("Remaining keys: ", keys)

