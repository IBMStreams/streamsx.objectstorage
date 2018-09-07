import argparse
import sys
import ibm_boto3
import threading
from ibm_botocore.client import Config
import s3_client as s3


parser = argparse.ArgumentParser(prog='cleanBucket')
parser.add_argument('-bucketName', dest='bucketName', help='name of bucket to be cleaned', required=True)
args = parser.parse_args()

targetBucketName = args.bucketName

print ("About to clean bucket '" + str(targetBucketName))

cos = s3.initS3IAMClient()

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

print ("About to clean up content of bucket '"  + targetBucketName + "'")

numObjs = 0
try:
   for key in cos.list_objects(Bucket=targetBucketName)['Contents']:
      numObjs+=1
      cos.delete_object(Bucket=targetBucketName,Key=key['Key'])
except KeyError: 
   err = 1

print("Number of deleted objects: "+str(numObjs))

