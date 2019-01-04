import argparse
import sys
import ibm_boto3
import threading
from ibm_botocore.client import Config
import s3_client as s3


parser = argparse.ArgumentParser(prog='cleanBucket')
parser.add_argument('-bucket', dest='bucket', help='name of bucket to be cleaned', required=True)
parser.add_argument('-endpoint', dest='endpoint', help='name of COS endpoint', required=False)
args = parser.parse_args()

target_bucket_name = args.bucket

print ("About to clean bucket '" + str(target_bucket_name))

cos_endpoint = None
if args.endpoint:
    cos_endpoint = args.endpoint

cos = s3.initS3IAMClient(cos_endpoint)

target_bucket = None
response = cos.list_buckets()
# Get a list of all bucket names from the response
buckets = [bucket['Name'] for bucket in response['Buckets']]
print ("Found the following buckets:")
for b in buckets:   
   print ('\t' + b)
   if b == target_bucket_name:
      target_bucket = b

if target_bucket is None:
   print ("Bucket '" + target_bucket_name + "' not found");
   raise SystemExit

print ("About to clean up content of bucket '"  + target_bucket_name + "'")

numObjs = 0
try:
   for key in cos.list_objects(Bucket=target_bucket_name)['Contents']:
      numObjs+=1
      cos.delete_object(Bucket=target_bucket_name,Key=key['Key'])
except KeyError: 
   err = 1

print("Number of deleted objects: "+str(numObjs))

