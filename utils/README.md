# streamsx.objectstorage utils

## Before launching the Python scripts

Ensure that you have Python 3.5 installed. For example, you can get Python 3.5 from the [Anaconda archive page](https://repo.continuum.io/archive/index.html).

Ensure that the bin directory is added to the PATH environment variable. If necessary, add the bin directory by entering the following command on the command line:

    export PATH="~/anaconda3/bin:$PATH"

Ensure that you have set the following environment variables for the S3 client credential files:

* `COS_IAM_CREDENTIALS` - name of JSON file containing settings to connect with IAM

Example of `COS_IAM_CREDENTIALS` file

    {
      "apikey": "xxxxxxxx",
      "endpoints": "https://cos-service.bluemix.net/endpoints",
      "iam_apikey_description": "Auto generated apikey during resource-key operation for Instance - crn:v1:bluemix:public:cloud-object-storage:global:a/xxxxxx::",
      "iam_apikey_name": "auto-generated-apikey-xxxx",
      "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Writer",
      "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity::a/xxxxxxxx",
      "resource_instance_id": "crn:v1:bluemix:public:cloud-object-storage:global:a/xxxxxx"
    }

### Required Python packages

Install the IBM Cloud Object Storage S3 client, by entering the following command on the command line:

    pip install ibm-cos-sdk


## Run the scripts

python3 listBuckets.py


python3 listObjects.py -bucket <your-bucket>


python3 cleanBucket.py -bucket <your-bucket>



