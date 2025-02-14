import boto3
from pyDataverse.api import NativeApi, SearchApi
import requests


class GenericClient:

    def __init__(self, endpoint_url, access_key, secret_key):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.client = boto3.client('s3', endpoint_url=self.endpoint_url, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)


class LocalMinioClient(GenericClient):

    def __init__(self, endpoint_url: str = "http://nginxminio:9000", access_key: str = "zGnGNFec3DKTXiN790kZ", secret_key: str = "eBZts8xTc3wbU1UEe5E0fHufTiZtBqwMItFbC9oC"):
        super().__init__(endpoint_url, access_key, secret_key)

    def get_object(self, bucketname, key):
        return self.client.get_object(Bucket=bucketname, Key=key)

    def put_object(self, bucketname, key, data):
        return self.client.put_object(Bucket=bucketname, Key=key, Body=data)

    def list_objects(self, bucketname):
        return self.client.list_objects(Bucket=bucketname)

    def delete_object(self, bucketname, key):
        return self.client.delete_object(Bucket=bucketname, Key=key)
    

class EXUSDataverseClient:

    def __init__(self, api_key: str = "fc66fe9a-c1ec-46c0-a55f-8b2d2636853b", base_url: str = "https://dataverse.museit.eu/"):
        self.api_key = api_key
        self.base_url = base_url
        self.api = NativeApi(api_token=self.api_key, base_url=self.base_url)
        self.search = SearchApi(self.base_url, self.api_key)

    def update_metadata(metadata, doi):
        url = f"https://dataverse.museit.eu/api/datasets/:persistentId/versions/:draft?persistentId={doi}"
        headers = {
            "X-Dataverse-key": "fc66fe9a-c1ec-46c0-a55f-8b2d2636853b",
        }
        response = requests.put(url, headers=headers, json=metadata)
        return response

    