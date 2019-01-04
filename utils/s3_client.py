import ibm_boto3
import json
import requests
from ibm_botocore.client import Config
import os, os.path

def initS3IAMClient(cos_endpoint):
    with open(os.environ['COS_IAM_CREDENTIALS']) as data_file:
        credentials = json.load(data_file)
    # Request detailed endpoint list
    endpoints = requests.get(credentials.get('endpoints')).json()
    # Obtain iam and cos host from the the detailed endpoints
    iam_host = (endpoints['identity-endpoints']['iam-token'])
    if cos_endpoint is None:
        cos_host = (endpoints['service-endpoints']['cross-region']['us']['public']['us-geo'])
    else:
        cos_host = cos_endpoint
    api_key = credentials.get('apikey')
    service_instance_id = credentials.get('resource_instance_id')
    # Construct auth and cos endpoint
    auth_endpoint = "https://" + iam_host + "/oidc/token"
    service_endpoint = "https://" + cos_host
    print("Creating S3 IAM client...")
    cos = ibm_boto3.client('s3',
                    ibm_api_key_id=api_key,
                    ibm_service_instance_id=service_instance_id,
                    ibm_auth_endpoint=auth_endpoint,
                    config=Config(signature_version='oauth'),
                    endpoint_url=service_endpoint)
    return cos

def listObjectsWithSize(cos, bucketname):
    numObjs = 0
    try:
        for key in cos.list_objects(Bucket=bucketname)['Contents']:
            numObjs+=1
            response = cos.head_object(Bucket=bucketname, Key=key['Key'])
            size = response['ContentLength']
            print(key['Key']+" "+str(size))
 
    except KeyError: 
        err = 1
    print("Number of objects: "+str(numObjs))

