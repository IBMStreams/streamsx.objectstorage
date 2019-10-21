import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.topology.context
import streamsx.spl.op as op
import streamsx.spl.toolkit as tk
import os, os.path
import streamsx.rest as sr
from subprocess import call, Popen, PIPE
import test_helper as th
import s3_client as s3
import time
import pyarrow.parquet as pq
import urllib3

class TestDistributed(unittest.TestCase):
    """ Test invocations of composite operators in local Streams instance """

    @classmethod
    def setUpClass(self):
        print (str(self))
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
            self.credentials = th.get_json_credentials()
        if (th.cos_credentials()):
            self.access_key, self.secret_access_key = th.read_credentials()
            if (self.access_key != "") and (self.secret_access_key != "") :
                self.bucket_name, self.s3_client = s3.createBucket("perf")

        # need to index the test toolkits
        print ("index the test toolkits ...")
        th.run_shell_command_line("cd performance/com.ibm.streamsx.objectstorage.s3.test; make javacompile")
        th.run_shell_command_line("cd performance/com.ibm.streamsx.objectstorage.s3.test; make tkidx")
        th.run_shell_command_line("cd feature; make tkidx")

    def tearDown(self):
        print ("")
        print ("clean-up")
        th.run_shell_command_line("rm -rf tmpdownload")
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
        self.cos_endpoint = "s3.us.cloud-object-storage.appdomain.cloud"
        self.isCloudTest = False

    def _add_toolkits(self, topo, test_toolkit):
        tk.add_toolkit(topo, test_toolkit)
        if self.object_storage_toolkit_location is not None:
            tk.add_toolkit(topo, self.object_storage_toolkit_location)

    def _build_launch_validate(self, name, composite_name, parameters, num_result_tuples, test_toolkit, exact=True, run_for=90, resets=0):
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

        if ("TestDistributed" in str(self)) or ("TestInstall" in str(self)) or ("TestICP" in str(self)):
            cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

        # Run the test
        test_res = self.tester.test(self.test_ctxtype, cfg, assert_on_fail=False, always_collect_logs=True)
        print (str(self.tester.result))        
        assert test_res, name+" FAILED ("+self.tester.result["application_logs"]+")"

    # ------------------------------------  
    
    # CONSISTENT REGION TESTS
    
    # ------------------------------------

    # CONSISTENT REGION: ObjectStorageSource (IAM), periodic, static name, text file (read 100 lines of 1 MB line size), no crash (1 reset)
    #@unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    #def test_read_object_consistent_region_static_name_periodic_iam(self):
    #    th.generate_large_text_file("input.txt")
    #    s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "input.txt", "input.txt")
    #    self._build_launch_validate("test_read_object_consistent_region_static_name_periodic_iam", "com.ibm.streamsx.objectstorage.test::ReadTestConsistentRegionPeriodicStaticNameIAMComp", {'credentials':self.credentials, 'objectStorageURI':self.uri_cos}, 1, 'feature/consistent.region.test', True, 90, 1)
        
    # CONSISTENT REGION: ObjectStorageSource (IAM), operatorDriven, static name, text file (read 100 lines of 1 MB line size), no crash (1 reset)
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_read_object_consistent_region_static_name_operatorDriven_iam(self):
        th.generate_large_text_file("input.txt")
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "input.txt", "input.txt")
        self._build_launch_validate("test_read_object_consistent_region_static_name_operatorDriven_iam", "com.ibm.streamsx.objectstorage.test::ReadTestConsistentRegionOperatorDrivenStaticNameIAMComp", {'credentials':self.credentials, 'objectStorageURI':self.uri_cos}, 1, 'feature/consistent.region.test', True, 90, 1)
    
    # -------------------

    # CONSISTENT REGION: ObjectStorageSource (IAM), periodic, static name, binary file (read 100 blocks of 1 MB block size), no crash (1 reset)
    #@unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    #def test_read_object_consistent_region_static_name_binary_periodic_iam(self):
    #    th.generate_large_bin_file("input.bin") # 100 MB
    #    s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "input.bin", "input.bin")
    #    self._build_launch_validate("test_read_object_consistent_region_static_name_binary_periodic_iam", "com.ibm.streamsx.objectstorage.test::ReadTestConsistentRegionPeriodicStaticNameBinaryIAMComp", {'credentials':self.credentials, 'objectStorageURI':self.uri_cos}, 1, 'feature/consistent.region.test', True, 90, 1)
       
    # CONSISTENT REGION: ObjectStorageSource (IAM), operatorDriven, static name, binary file (read 100 blocks of 1 MB block size), no crash (1 reset)
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_read_object_consistent_region_static_name_binary_operatorDriven_iam(self):
        th.generate_large_bin_file("input.bin") # 100 MB
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "input.bin", "input.bin")
        self._build_launch_validate("test_read_object_consistent_region_static_name_binary_operatorDriven_iam", "com.ibm.streamsx.objectstorage.test::ReadTestConsistentRegionOperatorDrivenStaticNameBinaryIAMComp", {'credentials':self.credentials, 'objectStorageURI':self.uri_cos}, 1, 'feature/consistent.region.test', True, 90, 1)
    
    # ------------------------------------

    # CONSISTENT REGION: ObjectStorageScan (IAM), operatorDriven, no crash (1 reset)
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_scan_consistent_region_operatorDriven_iam(self):
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/consistent.region.test/etc/input.txt", "scanTestData/input1.txt")
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/consistent.region.test/etc/input.txt", "scanTestData/input2.txt")
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/consistent.region.test/etc/input.txt", "scanTestData/input3.txt")
        self._build_launch_validate("test_scan_consistent_region_operatorDriven_iam", "com.ibm.streamsx.objectstorage.test::ScanTestConsistentRegionOperatorDrivenIAMComp", {'credentials':self.credentials, 'objectStorageURI':self.uri_cos}, 3, 'feature/consistent.region.test', True, 90, 1)

    # CONSISTENT REGION: ObjectStorageScan (IAM), periodic, no crash (1 reset)
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_scan_consistent_region_periodic_iam(self):
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/consistent.region.test/etc/input.txt", "scanTestData/input1.txt")
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/consistent.region.test/etc/input.txt", "scanTestData/input2.txt")
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/consistent.region.test/etc/input.txt", "scanTestData/input3.txt")
        self._build_launch_validate("test_scan_consistent_region_periodic_iam", "com.ibm.streamsx.objectstorage.test::ScanTestConsistentRegionPeriodicIAMComp", {'credentials':self.credentials, 'objectStorageURI':self.uri_cos}, 3, 'feature/consistent.region.test', True, 90, 1)

    # -------------------

    # CONSISTENT REGION: ObjectStorageSink (IAM), periodic, no crash (no reset)
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_sink_consistent_region_periodic_iam(self):
        self._build_launch_validate("test_sink_consistent_region_periodic_iam", "com.ibm.streamsx.objectstorage.test::SinkTestConsistentRegionIAMComp", {'credentials':self.credentials, 'objectStorageURI':self.uri_cos}, 1, 'feature/consistent.region.test', False)

    # -------------------

    # CONSISTENT REGION: ObjectStorageSink PARQUET (IAM), periodic, no crash (no reset)
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_sink_consistent_region_periodic_parquet_iam(self):
        self._build_launch_validate("test_sink_consistent_region_periodic_parquet_iam", "com.ibm.streamsx.objectstorage.test::ObjectStorageSink_consistent_region_parquetIAMComp", {'credentials':self.credentials, 'objectStorageURI':self.uri_cos, 'drainPeriod':3.0, 'uploadWorkersNum':10}, 1, 'feature/consistent.region.test', False, 120)
        s3.listObjectsWithSize(self.s3_client_iam, self.bucket_name_iam)

    # ------------------------------------

    # CONSISTENT REGION: ObjectStorageSink (IAM) - PARQUET
    # test exactly once semantics with resets triggered by ConsistentRegionResetter for parquet objects
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_with_resets_write_parquet_s3a_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 10.0
            runFor = 180
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 1.5
            runFor = 350
        numResets = 3
        # run the test
        # expect 100.000 tuples be processed with exactly once semantics
        # resets are triggered and Beacon re-submits the tuples, but resulting parquet objects should not have more than 100.000 rows
        self._build_launch_validate("test_consistent_region_with_resets_write_parquet_s3a_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteParquet_100000Tuples_consistent_region_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'credentials':self.credentials, 'objectStorageURI':self.uri_s3a, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', False, runFor, numResets)
        # download objects for validation
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

    # CONSISTENT REGION: ObjectStorageSink (IAM) - PARQUET - 2 Beacons - PEs isolated
    # test exactly once semantics with resets triggered by ConsistentRegionResetter for parquet objects
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_with_resets_write_parquet_s3a_pes_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 10.0
            runFor = 180
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 1.5
            runFor = 350
        numResets = 20
        # run the test
        # expect 200.000 tuples be processed with exactly once semantics
        # Each Beacon is configured with iteration of 100.000 tuples
        # resets are triggered and Beacon re-submits the tuples, but resulting parquet objects should not have more than 200.000 rows
        self._build_launch_validate("test_consistent_region_with_resets_write_parquet_s3a_pes_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteParquet_200000Tuples_consistent_region_pes_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'credentials':self.credentials, 'objectStorageURI':self.uri_s3a, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', False, runFor, numResets)
        # download objects for validation
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
        assert (num_rows_total==200000), "Expected 200000 messages in parquet objects, but found "+str(num_rows_total)

    # ------------------------------------

    # CONSISTENT REGION: ObjectStorageSink (IAM) - PARTITIONED PARQUET
    # test exactly once semantics with resets triggered by ConsistentRegionResetter for parquet objects with partitions
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_consistent_region_with_resets_write_partitioned_parquet_s3a_iam(self):  
        if (self.isCloudTest):
             # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 10.0
            runFor = 180
        else:
            # tweak performance parameters
            uploadWorkersNum = 10
            drainPeriod = 5.0
            runFor = 350
        numResets = 2
        # run the test
        # expect 100.000 tuples be processed with exactly once semantics
        # resets are triggered and Beacon re-submits the tuples, but resulting parquet objects should not have more than 100.000 rows
        self._build_launch_validate("test_consistent_region_with_resets_write_partitioned_parquet_s3a_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteParquetPartitioned_100000Tuples_consistent_region_IAMComp", {'drainPeriod':drainPeriod, 'uploadWorkersNum':uploadWorkersNum, 'credentials':self.credentials, 'objectStorageURI':self.uri_s3a, 'endpoint':self.cos_endpoint}, 1, 'performance/com.ibm.streamsx.objectstorage.s3.test', False, runFor, numResets)
        # download objects for validation
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

class TestInstall(TestDistributed):
    """ Test invocations of composite operators in local Streams instance using installed toolkit """

    def setUp(self):
        Tester.setup_distributed(self)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.object_storage_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.objectstorage'


class TestICP(TestDistributed):
    """ Test invocations of composite operators in remote Streams instance using local toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        env_chk = True
        try:
            print("CP4D_URL="+str(os.environ['CP4D_URL']))
        except KeyError:
            env_chk = False
        assert env_chk, "CP4D_URL environment variable must be set"

class TestICPInstall(TestICP):
    """ Test invocations of composite operators in remote Streams instance using local installed toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.object_storage_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.objectstorage'

    def setUp(self):
        Tester.setup_distributed(self)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.object_storage_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.objectstorage'


class TestCloud(TestDistributed):
    """ Test invocations of composite operators in Streaming Analytics Service using local toolkit """

    @classmethod
    def setUpClass(self):
        super().setUpClass()
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


