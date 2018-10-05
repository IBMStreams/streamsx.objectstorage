import argparse
import sys
import ibm_boto3
import threading
from ibm_botocore.client import Config
import s3_client as s3


parser = argparse.ArgumentParser(prog='listObjects')
parser.add_argument('-bucketName', dest='bucketName', help='name of bucket to be cleaned', required=True)
parser.add_argument('-endpoint', dest='endpoint', help='name of COS endpoint', required=False)
args = parser.parse_args()

targetBucketName = args.bucketName

print ("About to list objects in bucket '" + str(targetBucketName))

cosEndpoint = None
if args.endpoint:
    cosEndpoint = args.endpoint

cos = s3.initS3IAMClient(cosEndpoint)

targetBucket = None
response = cos.list_buckets()
# Get a list of all bucket names from the response
buckets = [bucket['Name'] for bucket in response['Buckets']]
print ("Found the following buckets:")
for b in buckets:   
   print ('\t' + b)
   if b == targetBucketName:
      targetBucket = b

if targetBucket is None:
   print ("Bucket '" + targetBucketName + "' not found");
   raise SystemExit

print ("List content of bucket '"  + targetBucketName + "'")

s3.listObjectsWithSize(cos, targetBucketName)

