import ibm_boto3
import json
import requests
import random
from ibm_botocore.client import Config
from pprint import pprint
import os, os.path
import time
import test_helper as th

def initS3Client():
    access_key, secret_access_key = th.read_credentials()
    # Create an S3 client
    cos = ibm_boto3.client('s3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        endpoint_url='https://s3-api.us-geo.objectstorage.softlayer.net:443')
    return cos


def initS3IAMClient():
    with open(os.environ['COS_IAM_CREDENTIALS']) as data_file:
        credentials = json.load(data_file)
    # Request detailed endpoint list
    endpoints = requests.get(credentials.get('endpoints')).json()
    # Obtain iam and cos host from the the detailed endpoints
    iam_host = (endpoints['identity-endpoints']['iam-token'])
    cos_host = (endpoints['service-endpoints']['cross-region']['us']['public']['us-geo'])
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


def createBucketIAM():
    # get s3 client
    cos = initS3IAMClient()

    response = cos.list_buckets()
    # Get a list of all bucket names from the response
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    result = [bucket for bucket in buckets if 'streamsx-os-test-bucket-us-iam-' in bucket]
    if len(result) == 0 :
        # Create a bucket
        bucket_name = 'streamsx-os-test-bucket-us-iam-' + str(time.time());
        bucket_name = bucket_name.replace(".", "")
        print("create bucket "+bucket_name)
        cos.create_bucket(Bucket=bucket_name)
    else :
        bucket_name = result[0]
    print(bucket_name)
    return bucket_name, cos


def createBucket():
    # get s3 client
    cos = initS3Client()

    response = cos.list_buckets()
    # Get a list of all bucket names from the response
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    result = [bucket for bucket in buckets if 'streamsx-os-test-bucket-us-' in bucket]
    if len(result) == 0 :
        # Create a bucket
        bucket_name = 'streamsx-os-test-bucket-us-' + str(time.time());
        bucket_name = bucket_name.replace(".", "")
        print("create bucket "+bucket_name)
        cos.create_bucket(Bucket=bucket_name)
    else :
        bucket_name = result[0]
    print(bucket_name)
    return bucket_name, cos

def listObjects(cos, bucket_name):
    response = cos.list_objects(Bucket=bucket_name)
    try:
        # Get a list of all object names from the response
        objects = [object['Key'] for object in response['Contents']]
        # Print out the object list
        print("Objects in %s:" % bucket_name)
        print(json.dumps(objects, indent=2))
    except KeyError: 
        print("No objects in %s" % bucket_name)

def deleteAllObjects(cos, bucket_name):
    numObjs = 0
    try:
        for key in cos.list_objects(Bucket=bucket_name)['Contents']:
            numObjs+=1
            cos.delete_object(Bucket=bucket_name,Key=key['Key'])
    except KeyError: 
        err = 1

    print("Number of deleted objects: "+str(numObjs))

def validateObjects(cos, bucketname, objectnames):
    print("validate objects in %s:" % bucketname)
    for key in objectnames:
        response = cos.head_object(Bucket=bucketname, Key=key)
        size = response['ContentLength']
        print(key+" "+str(size))
        assert (size > 0), "Invalid object size (must not be zero)"

def uploadObject(cos, bucketname, filename, objectname):
    print("upload object in %s:" % bucketname)
    cos.upload_file(filename, bucketname, objectname)



