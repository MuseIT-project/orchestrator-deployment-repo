from prefect import flow, task
from openpyxl.reader.excel import load_workbook
import boto3
import requests
import json
from prefect.logging import get_run_logger
import random
import base64


@task
def retrieve_file_for_metadata(key, bucketname):
    access_key = 'zGnGNFec3DKTXiN790kZ'
    secret_key = 'eBZts8xTc3wbU1UEe5E0fHufTiZtBqwMItFbC9oC'
    minio_client = boto3.client('s3', endpoint_url='http://nginxminio:9000', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    filedata = minio_client.get_object(Bucket=bucketname, Key=key)
    return filedata['Body'].read()
    #the bagpipe is angry

@flow
def transform_image(image, filename, transformation_type, color_format):
    filenoextension = filename.split('.')[0]
    new_filename = f'{filenoextension}_{str(transformation_type)}_{str(color_format)}'
    files = {'file': (filename, image)}
    response = requests.post(
        url='http://imageconverter:8855/convert',
        params={
            'output_format': transformation_type,
            'color_format': color_format
        },
        files=files,
    )
    return new_filename, response.content

@task
def get_file_keys(bucket_name):
    access_key = 'zGnGNFec3DKTXiN790kZ'
    secret_key = 'eBZts8xTc3wbU1UEe5E0fHufTiZtBqwMItFbC9oC'
    minio_client = boto3.client('s3', endpoint_url='http://nginxminio:9000', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    files = minio_client.list_objects_v2(Bucket=bucket_name)
    logger = get_run_logger()
    logger.info(files)
    print(files)
    foundfiles = []
    for file in files['Contents']:
        foundfiles.append(file['Key'])
    return foundfiles

@task
def save_to_minio(key, contents, bucketname):
    access_key = 'zGnGNFec3DKTXiN790kZ'
    secret_key = 'eBZts8xTc3wbU1UEe5E0fHufTiZtBqwMItFbC9oC'
    minio_client = boto3.client('s3', endpoint_url='http://nginxminio:9000', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    if 'mesh' in key:
        filedata = minio_client.put_object(Bucket=bucketname, Key=f"{key}.glb", Body=contents)
    filedata = minio_client.put_object(Bucket=bucketname, Key=f"{key}.jpg", Body=contents)
    #the bagpipe is angry

@flow
def transform_images():
    logger = get_run_logger()
    filebucketname = '300originals'
    files = get_file_keys(bucket_name=filebucketname)
    logger.info("Found files: ", files)
    transformations = ['combined_forced', 'colormerge_forced', 'contours']
    color_transformations = ['default', 'fifths', 'fifthsv2']
    resultbucketname = 'transformedassets'
    for foundfile in files:
        foundfile_content = retrieve_file_for_metadata(key=foundfile, bucketname=filebucketname)
        logger.info(f"Processing file {foundfile}")
        for transformation in transformations:
            for color_transformation in color_transformations:
                t_filename, t_image = transform_image(
                    image=foundfile_content,
                    filename=foundfile,
                    transformation_type=transformation,
                    color_format=color_transformation
                )
                save_to_minio(t_filename, t_image, resultbucketname)

if __name__ == '__main__':
    transform_images.serve(
        name='image_flow',
    )
