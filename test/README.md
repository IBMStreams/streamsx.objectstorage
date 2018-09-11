# streamsx.objectstorage tests

## Before launching the test

Ensure that you have Python 3.5 installed. For example, you can get Python 3.5 from the [Anaconda archive page](https://repo.continuum.io/archive/index.html).

Ensure that the bin directory is added to the PATH environment variable. If necessary, add the bin directory by entering the following command on the command line:

    export PATH="~/anaconda3/bin:$PATH"

Ensure that you have set the following environment variables for testing with Streaming Analytics service in IBM Cloud:

* `STREAMING_ANALYTICS_SERVICE_NAME` - name of your Streaming Analytics service
* `VCAP_SERVICES` - [VCAP](https://console.bluemix.net/docs/services/StreamingAnalytics/r_vcap_services.html#r_vcap_services) information in JSON format or a JSON file

Ensure that you have set the following environment variables for testing with local Streams instance:

* `STREAMS_USERNAME` - username to connect with local Streams domain
* `STREAMS_PASSWORD` - password to connect with local Streams domain

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

### Required Python packages

Python unit test requires TopologyTester from Python streamsx package or com.ibm.streamsx.topology toolkit version 1.9 or later.

Install the latest streamsx package with pip, a package manager for Python, by entering the following command on the command line:

    pip install --user --upgrade streamsx


Install the IBM Cloud Object Storage S3 client, by entering the following command on the command line:

    pip install ibm-cos-sdk


Install the PyArrow with Parquet Support, by entering the following command on the command line:

    pip install pyarrow


## Run the feature test

### Local Streams instance

    python3 -u -m unittest test_objectstorage_toolkit.TestDistributed

### Streaming Analytics service

    python3 -u -m unittest test_objectstorage_toolkit.TestCloud


## Run the performance test

### Streaming Analytics service

    python3 -u -m unittest test_performance.TestCloud

### Streaming Analytics service - data historian format tests

Tests are intended to launch the app and verify manually with Streams console and check the logs afterwards.
They are split in tests with consistent region and without consistent region.
Tests are generating data historian tuples and write in different formats (raw, parquet, partioned parquet) and with different protocols (cos, s3a).

    python3 -u -m unittest test_dh.py

    python3 -u -m unittest test_cr_dh.py

Automated test case to verify the ObjectStorageSink creating objects in parquet format when running in a consistent region:

    python3 -u -m unittest test_cr_dh.TestCloud.test_consistent_region_with_resets_write_parquet_s3a_iam



