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
import pyarrow.parquet as pq

class TestDistributed(unittest.TestCase):
    """ Test invocations of composite operators in local Streams instance """

    @classmethod
    def setUpClass(self):
        print (str(self))
        self.s3_client_iam = None
        self.s3_client = None
        self.bucket_name_iam = None
        self.bucket_name = None
        if (th.iam_credentials()):
            self.iam_api_key, self.service_instance_id = th.read_iam_credentials()
            if (self.iam_api_key != "") and (self.service_instance_id) :
                self.bucket_name_iam, self.s3_client_iam = s3.createBucketIAM("perf")
                self.uri_cos = "cos://"+self.bucket_name_iam+"/"
                self.uri_s3a = "s3a://"+self.bucket_name_iam+"/"

        if (th.cos_credentials()):
            self.access_key, self.secret_access_key = th.read_credentials()
            if (self.access_key != "") and (self.secret_access_key != "") :
                self.bucket_name, self.s3_client = s3.createBucket("perf")

        # need to index the test toolkits
        print ("index the test toolkits ...")
        th.run_shell_command_line("cd performance/com.ibm.streamsx.objectstorage.s3.test; make javacompile")
        th.run_shell_command_line("cd performance/com.ibm.streamsx.objectstorage.s3.test; make tkidx")

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
        # public endpoint (CROSS REGION)
        self.cos_endpoint = "s3-api.us-geo.objectstorage.softlayer.net"
        self.isCloudTest = False

    def _add_toolkits(self, topo, test_toolkit):
        tk.add_toolkit(topo, test_toolkit)
        if self.object_storage_toolkit_location is not None:
            tk.add_toolkit(topo, self.object_storage_toolkit_location)

    def _build_launch_validate(self, name, composite_name, parameters, num_result_tuples, test_toolkit, run_for=90, resets=0):
        print ("------ "+name+" ------")
        topo = Topology(name)
        self._add_toolkits(topo, test_toolkit)
	
        params = parameters
        # Call the test composite
        test_op = op.Source(topo, composite_name, 'tuple<rstring result>', params=params)
        self.tester = Tester(topo)
        if (self.isCloudTest):
            runFor = run_for
        else:
            runFor = run_for
        self.tester.run_for(runFor)
        if (resets > 0):
            self.tester.resets(resets) # minimum number of resets for each region, requires v1.11 of topology toolkit
        self.tester.tuple_count(test_op.stream, num_result_tuples, exact=False)

        cfg = {}
        #job_config = streamsx.topology.context.JobConfig(tracing='error')
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(cfg)

        # Run the test
        self.tester.test(self.test_ctxtype, cfg, assert_on_fail=False, always_collect_logs=True)
        print (str(self.tester.result))        


    # ------------------------------------  
    
    # CONSISTENT REGION TESTS
    
    # ------------------------------------

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_write_parquet_cos_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 60.0
            runFor = 400
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 10.0
            runFor = 200

        # run the test
        self._build_launch_validate("test_consistent_region_write_parquet_cos_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteParquet_consistent_region_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_cos, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', runFor)
        s3.listObjectsWithSize(self.s3_client_iam, self.bucket_name_iam)

    # ------------------------------------

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_write_parquet_s3a_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 60.0
            runFor = 400
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 4.0
            runFor = 200

        # run the test
        self._build_launch_validate("test_consistent_region_write_parquet_s3a_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteParquet_consistent_region_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_s3a, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', runFor)
        s3.listObjectsWithSize(self.s3_client_iam, self.bucket_name_iam)    

    # ------------------------------------

    # test exactly once semantics with resets triggered by ConsistentRegionResetter for parquet objects
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_with_resets_write_parquet_s3a_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 10.0
            runFor = 100
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 4.0
            runFor = 100

        # run the test
        # expect 100.000 tuples be processed with exactly once semantics
        # resets are triggered and Beacon re-submits the tuples, but resulting parquet objects should not have more than 100.000 rows
        self._build_launch_validate("test_consistent_region_with_resets_write_parquet_s3a_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteParquet_100000Tuples_consistent_region_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_s3a, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', runFor, 3)
        # download objects for validation
        th.run_shell_command_line("mkdir tmpdownload")
        print ("Download parquet objects ...")
        object_names = []
        num_rows_total = 0
        object_names = s3.listAndDownloadObjects(self.s3_client_iam, self.bucket_name_iam)    
        for key in object_names:
            parquet_file = pq.ParquetFile('tmpdownload/'+key)
            num_rows = parquet_file.metadata.num_rows
            num_rows_total += num_rows
            print (key+": num_rows="+str(num_rows))
        print ("num_rows_total="+str(num_rows_total))
        assert (num_rows_total==100000), "Expected 100000 messages in parquet objects, but found "+str(num_rows_total)

    # ------------------------------------

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_write_raw_cos_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 60.0
            runFor = 400
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 3.0
            runFor = 200

        # run the test
        self._build_launch_validate("test_consistent_region_write_raw_cos_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteRaw_consistent_region_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_cos, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', runFor)
        s3.listObjectsWithSize(self.s3_client_iam, self.bucket_name_iam)

    # ------------------------------------

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_write_raw_s3a_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 60.0
            runFor = 400
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 3.0
            runFor = 200

        # run the test
        self._build_launch_validate("test_consistent_region_write_raw_s3a_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteRaw_consistent_region_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_s3a, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', runFor)
        s3.listObjectsWithSize(self.s3_client_iam, self.bucket_name_iam)    

    # ------------------------------------

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_write_parquet_partitioned_cos_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 15.0
            runFor = 400
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 3.0
            runFor = 120

        # run the test
        self._build_launch_validate("test_consistent_region_write_parquet_partitioned_cos_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteParquetPartitioned_consistent_region_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_cos, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', runFor)
        s3.listObjectsWithSize(self.s3_client_iam, self.bucket_name_iam)

    # ------------------------------------

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_write_parquet_partitioned_s3a_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 15
            drainPeriod = 60.0
            runFor = 400
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 3.0
            runFor = 120

        # run the test
        self._build_launch_validate("test_consistent_region_write_parquet_partitioned_s3a_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteParquetPartitioned_consistent_region_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_s3a, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', runFor)
        s3.listObjectsWithSize(self.s3_client_iam, self.bucket_name_iam)

    # ------------------------------------

class TestInstall(TestDistributed):
    """ Test invocations of composite operators in local Streams instance using installed toolkit """

    def setUp(self):
        Tester.setup_distributed(self)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.object_storage_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.objectstorage'

class TestCloud(TestDistributed):
    """ Test invocations of composite operators in Streaming Analytics Service using local toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        th.stop_streams_cloud_instance()
        th.start_streams_cloud_instance()

    @classmethod
    def tearDownClass(self):
        th.stop_streams_cloud_instance()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.object_storage_toolkit_location = "../com.ibm.streamsx.objectstorage"
        # private endpoint (CROSS REGION)
        self.cos_endpoint = "s3-api.dal-us-geo.objectstorage.service.networklayer.com"
        self.isCloudTest = True
        

class TestCloudInstall(TestDistributed):
    """ Test invocations of composite operators in Streaming Analytics Service using remote toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        th.stop_streams_cloud_instance()
        th.start_streams_cloud_instance()

    @classmethod
    def tearDownClass(self):
        th.stop_streams_cloud_instance()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        # remote toolkit is used
        self.object_storage_toolkit_location = None
        # private endpoint (CROSS REGION)
        self.cos_endpoint = "s3-api.dal-us-geo.objectstorage.service.networklayer.com"
        self.isCloudTest = True


