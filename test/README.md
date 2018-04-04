# streamsx.objectstorage tests

## Before launching the test

Ensure that you have Python 3.5 installed. For example, you can get Python 3.5 from the [Anaconda archive page](https://repo.continuum.io/archive/index.html).

Ensure that the bin directory is added to the PATH environment variable. If necessary, add the bin directory by entering the following command on the command line:

    export PATH="~/anaconda3/bin:$PATH"

Ensure that you have set the following environment variables:

* `STREAMING_ANALYTICS_SERVICE_NAME` - name of your Streaming Analytics service
* `VCAP_SERVICES` - [VCAP](https://console.bluemix.net/docs/services/StreamingAnalytics/r_vcap_services.html#r_vcap_services) information in JSON format or a JSON file

Install the latest streamsx package with pip, a package manager for Python, by entering the following command on the command line:

    pip install --user --upgrade streamsx


Install the IBM Cloud Object Storage S3 client, by entering the following command on the command line:

    pip install ibm-cos-sdk

Ensure that you have set the following environment variables for the S3 client credential files:

* `COS_IAM_CREDENTIALS` - name of JSON file containing settings to connect with IAM
* `COS_CREDENTIALS` - name of JSON file containing settings to connect with accessKey and secretAccessKey

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

Example of `COS_CREDENTIALS` file

    {
      "endpoint": "s3-api.us-geo.objectstorage.softlayer.net",
      "access_key": "xxxxx",
      "secret_access_key": "xxxxxxx"
    }


## Run the feature test

### Local Streams instance

    python3 -u -m unittest test_objectstorage_toolkit.TestDistributed

### Streaming Analytics service

    python3 -u -m unittest test_objectstorage_toolkit.TestCloud


## Run the performance test

### Streaming Analytics service

    python3 -u -m unittest test_performance.TestCloud

