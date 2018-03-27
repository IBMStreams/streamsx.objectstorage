import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit as tk
import os, os.path
import streamsx.rest as sr
from subprocess import call, Popen, PIPE
import test_helper as th
import s3_client as s3
import time

class TestDistributed(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.s3_client_iam = None
        self.s3_client = None
        self.iam_api_key, self.service_instance_id = th.read_iam_credentials()
        if (self.iam_api_key != "") and (self.service_instance_id) :
            self.bucket_name_iam, self.s3_client_iam = s3.createBucketIAM()
            self.uri_cos = "cos://"+self.bucket_name_iam+"/"
            self.uri_s3a = "s3a://"+self.bucket_name_iam+"/"

        self.access_key, self.secret_access_key = th.read_credentials()
        if (self.access_key != "") and (self.secret_access_key != "") :
            self.bucket_name, self.s3_client = s3.createBucket()

    def tearDown(self):
        print ("")
        print ("clean-up")
        if self.s3_client is not None:
             s3.listObjects(self.s3_client, self.bucket_name)
             s3.deleteAllObjects(self.s3_client, self.bucket_name)
        if self.s3_client_iam is not None:
             s3.listObjects(self.s3_client_iam, self.bucket_name_iam)
             s3.deleteAllObjects(self.s3_client_iam, self.bucket_name_iam)

    def setUp(self):
        Tester.setup_distributed(self)
        self.object_storage_toolkit_location = "../com.ibm.streamsx.objectstorage"

    def _add_toolkits(self, topo, test_toolkit):
        tk.add_toolkit(topo, test_toolkit)
        tk.add_toolkit(topo, self.object_storage_toolkit_location)

    def _build_launch_validate(self, name, composite_name, parameters, num_result_tuples, test_toolkit):
        print ("------ "+name+" ------")
        topo = Topology(name)
        self._add_toolkits(topo, test_toolkit)
	
        params = parameters
        # Call the test composite
        test_op = op.Source(topo, composite_name, 'tuple<rstring result>', params=params)
        self.tester = Tester(topo)
        self.tester.run_for(60)
        self.tester.tuple_count(test_op.stream, num_result_tuples, exact=True)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='trace')
        job_config.add(cfg)

        # Run the test
        self.tester.test(self.test_ctxtype, cfg, assert_on_fail=True, always_collect_logs=True)
        print (str(self.tester.result))

    def _check_created_objects(self, n_objects, s3_client, bucket_name):
        test_object_names = []
        for num in range(n_objects):
             test_object_names.append('test_data_'+str(num)) # expected keys - n objects are created by SPL application
        # delay to ensure objects are in sync on COS
        time.sleep(15) 
        # check if n objects exists and if size is not zero
        s3.validateObjects(s3_client, bucket_name, test_object_names)


    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_scan_read_object_iam(self):
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/read.test/etc/input.txt", "scanTestData/input.txt")
        self._build_launch_validate("test_scan_read_object_iam", "com.ibm.streamsx.objectstorage.test::ScanReadTestIAMComp", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 1, 'feature/read.test')

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_scan_read_object(self):
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/read.test/etc/input.txt", "scanTestData/input.txt")
        self._build_launch_validate("test_scan_read_object", "com.ibm.streamsx.objectstorage.test::ScanReadTestComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 1, 'feature/read.test')

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_functions(self):
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/functions.test/etc/sample1", "sample1")
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/functions.test/etc/sample2", "sample2")
        self._build_launch_validate("test_functions", "com.ibm.streamsx.objectstorage.test::FunctionsTestComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/functions.test')

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_read_object_iam(self):
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/read.test/etc/input.txt", "input.txt")
        self._build_launch_validate("test_read_object_iam", "com.ibm.streamsx.objectstorage.test::ReadTestIAMComp", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 2, 'feature/read.test')

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_read_object(self):
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/read.test/etc/input.txt", "input.txt")
        self._build_launch_validate("test_read_object", "com.ibm.streamsx.objectstorage.test::ReadTestComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/read.test')

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_read_bin_object_iam(self):
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/read.test/etc/input.gz", "input.gz")
        self._build_launch_validate("test_read_bin_object_iam", "com.ibm.streamsx.objectstorage.test::ReadBinTestIAMComp", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 2, 'feature/read.test')

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_read_bin_object(self):
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/read.test/etc/input.gz", "input.gz")
        self._build_launch_validate("test_read_bin_object", "com.ibm.streamsx.objectstorage.test::ReadBinTestComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/read.test')

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_write_object(self):
        self._build_launch_validate("test_write_object", "com.ibm.streamsx.objectstorage.test::WriteTestComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/write.test')
        self._check_created_objects(2, self.s3_client, self.bucket_name)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_write_object_iam(self):
        self._build_launch_validate("test_write_object_iam", "com.ibm.streamsx.objectstorage.test::WriteTestIAMComp", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 2, 'feature/write.test')
        self._check_created_objects(2, self.s3_client_iam, self.bucket_name_iam)

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_s3a(self):
        nObjects = 5 # number of objects to be created by SPL application
        self._build_launch_validate("test_write_n_objects_s3a", "com.ibm.streamsx.objectstorage.s3.test::WriteDurationTestComp", {'dataSize':100000, 'numObjects':nObjects, 'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test')
        self._check_created_objects(nObjects, self.s3_client, self.bucket_name)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_s3a_iam(self):
        nObjects = 5 # number of objects to be created by SPL application
        self._build_launch_validate("test_write_n_objects_s3a_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteDurationTestIAMComp", {'dataSize':100000, 'numObjects':nObjects, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_s3a}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test')
        self._check_created_objects(nObjects, self.s3_client_iam, self.bucket_name_iam)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_cos_iam(self):
        nObjects = 5 # number of objects to be created by SPL application
        self._build_launch_validate("test_write_n_objects_cos_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteDurationTestIAMComp", {'dataSize':100000, 'numObjects':nObjects, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_cos}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test')
        self._check_created_objects(nObjects, self.s3_client_iam, self.bucket_name_iam)



class TestInstall(TestDistributed):
    def setUp(self):
        Tester.setup_distributed(self)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.object_storage_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.objectstorage'

class TestCloud(TestDistributed):
    """ Test invocations of composite operators in Streaming Analytics Service """

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.object_storage_toolkit_location = "../com.ibm.streamsx.objectstorage"


