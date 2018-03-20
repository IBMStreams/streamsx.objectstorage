import argparse
import sys
import ibm_boto3
from ibm_botocore.client import Config

staging_credentials = {"API_KEY": "aSqncfwx1_gbocrI-FgkkpjvfPSqXAaXhn4mczQ2Kazj", 
                      "RESOURCE_ID": "crn:v1:staging:public:cloud-object-storage:global:a/a91af37afdb0c34859953fa727bd779d:5815451d-1aca-4cae-b89c-1285ce2715a2::", 
                      "AUTH_ENDPOINT": "https://iam.stage1.ng.bluemix.net/oidc/token", 
                      "SERVICE_ENDPOINT": "https://s3.us-west.objectstorage.uat.softlayer.net"}

prod_credentials = {"API_KEY": "WaYAezQghvoyH51M6cZCrCIks43w4L4up4OQQFKjHShM", 
		    "RESOURCE_ID": "crn:v1:bluemix:public:cloud-object-storage:global:a/166b06133de3b115e20d6201f119da18:396f3af4-a99d-4e19-9469-a48e5b442caf::", 
		    "AUTH_ENDPOINT": "https://iam.bluemix.net/oidc/token", 
		    "SERVICE_ENDPOINT": "https://s3-api.us-geo.objectstorage.softlayer.net"}


parser = argparse.ArgumentParser(prog='bucketCleaner')
parser.add_argument('-env', nargs='?', default='staging', dest='envName',choices=set(('prod', 'staging')), help='environment to connect to. By default "staging" environment is about to be used.')
args = parser.parse_args()


env = args.envName

print ("Buckets list:")

credentials =  prod_credentials if env == 'prod' else staging_credentials


resource = ibm_boto3.resource('s3',
                      ibm_api_key_id=credentials["API_KEY"],
                      ibm_service_instance_id=credentials["RESOURCE_ID"],
                      ibm_auth_endpoint=credentials["AUTH_ENDPOINT"],
                      config=Config(signature_version='oauth'),
                      endpoint_url=credentials["SERVICE_ENDPOINT"])

allbuckets = resource.buckets.all()
for b in allbuckets:
   print (b)

