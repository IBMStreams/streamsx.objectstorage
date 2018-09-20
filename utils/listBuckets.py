import sys
import ibm_boto3
from ibm_botocore.client import Config
import s3_client as s3

cos = s3.initS3IAMClient()

response = cos.list_buckets()
# Get a list of all bucket names from the response
buckets = [bucket['Name'] for bucket in response['Buckets']]

print ("list buckets:\n") 
for b in buckets:
   print (b)    

