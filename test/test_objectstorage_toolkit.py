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
    """ Test invocations of composite operators in local Streams instance """

    @classmethod
    def setUpClass(self):
        print (str(self))
        self.s3_client_iam = None
        self.s3_client = None
        self.bucket_name_iam = None
        self.bucket_name = None
        self.uri_basic = None
        if (th.iam_credentials()):
            self.iam_api_key, self.service_instance_id = th.read_iam_credentials()
            if (self.iam_api_key != "") and (self.service_instance_id) :
                self.bucket_name_iam, self.s3_client_iam = s3.createBucketIAM()
                self.uri_cos = "cos://"+self.bucket_name_iam+"/"
                self.uri_s3a = "s3a://"+self.bucket_name_iam+"/"
        if (th.cos_credentials()):
            self.access_key, self.secret_access_key = th.read_credentials()
            if (self.access_key != "") and (self.secret_access_key != "") :
                self.bucket_name, self.s3_client = s3.createBucket()
                self.uri_basic = "s3a://"+self.bucket_name+"/"

        if (self is not TestCloud) and (self is not TestCloudInstall):
            # need to index the test toolkits
            print ("index the test toolkits ...")
            th.run_shell_command_line("cd feature; make tkidx")
            print ("index the samples ...")
            if (self is TestInstall):
                th.run_shell_command_line("cd "+self.object_storage_samples_location+"; make tkidx")
            else:
                th.run_shell_command_line("cd ../samples; make tkidx")

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
        self.object_storage_samples_location = "../samples"

    def _add_toolkits(self, topo, test_toolkit):
        tk.add_toolkit(topo, test_toolkit)
        if self.object_storage_toolkit_location is not None:
            tk.add_toolkit(topo, self.object_storage_toolkit_location)

    def _build_launch_validate(self, name, composite_name, parameters, num_result_tuples, test_toolkit, exact=True):
        print ("------ "+name+" ------")
        topo = Topology(name)
        self._add_toolkits(topo, test_toolkit)
	
        params = parameters
        # Call the test composite
        test_op = op.Source(topo, composite_name, 'tuple<rstring result>', params=params)
        self.tester = Tester(topo)
        self.tester.run_for(30)
        self.tester.tuple_count(test_op.stream, num_result_tuples, exact=exact)

        cfg = {}
        job_config = streamsx.topology.context.JobConfig(tracing='trace')
        job_config.add(cfg)

        # Run the test
        test_res = self.tester.test(self.test_ctxtype, cfg, assert_on_fail=False, always_collect_logs=True)
        print (str(self.tester.result))
        assert test_res, name+" FAILED ("+self.tester.result["application_logs"]+")"


    def _check_created_objects(self, n_objects, s3_client, bucket_name):
        test_object_names = []
        for num in range(n_objects):
             test_object_names.append('test_data_cos_'+str(num)) # expected keys - n objects are created by SPL application
             test_object_names.append('test_data_s3a_'+str(num)) # expected keys - n objects are created by SPL application
        # delay to ensure objects are in sync on COS
        time.sleep(5) 
        # check if n objects exists and if size is not zero
        s3.validateObjects(s3_client, bucket_name, test_object_names)

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_scan_read_object(self):
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/read.test/etc/input.txt", "scanTestData/input.txt")
        self._build_launch_validate("test_scan_read_object", "com.ibm.streamsx.objectstorage.test::ScanReadTestComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 1, 'feature/read.test')

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_scan_read_object_iam(self):
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/read.test/etc/input.txt", "scanTestData/input.txt")
        self._build_launch_validate("test_scan_read_object_iam", "com.ibm.streamsx.objectstorage.test::ScanReadTestIAMComp", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 1, 'feature/read.test')

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_scan_read_object_control_port(self):
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/read.test/etc/input.txt", "scanTestData/input.txt")
        self._build_launch_validate("test_scan_read_object_control_port", "com.ibm.streamsx.objectstorage.test::ScanReadTestControlPortComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 1, 'feature/read.test')

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_scan_read_object_control_port_iam(self):
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/read.test/etc/input.txt", "scanTestData/input.txt")
        self._build_launch_validate("test_scan_read_object_control_port_iam", "com.ibm.streamsx.objectstorage.test::ScanReadTestControlPortIAMComp", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 1, 'feature/read.test')

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_functions(self):
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/functions.test/etc/sample1", "sample1")
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/functions.test/etc/sample2", "sample2")
        self._build_launch_validate("test_functions", "com.ibm.streamsx.objectstorage.test::FunctionsTestComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/functions.test')

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_read_object_iam(self):
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/read.test/etc/input.txt", "input.txt")
        self._build_launch_validate("test_read_object_iam", "com.ibm.streamsx.objectstorage.test::ReadTestIAMComp", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 2, 'feature/read.test')

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_read_object(self):
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/read.test/etc/input.txt", "input.txt")
        self._build_launch_validate("test_read_object", "com.ibm.streamsx.objectstorage.test::ReadTestComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/read.test')

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_read_bin_object_iam(self):
        s3.uploadObject(self.s3_client_iam, self.bucket_name_iam, "feature/read.test/etc/input.gz", "input.gz")
        self._build_launch_validate("test_read_bin_object_iam", "com.ibm.streamsx.objectstorage.test::ReadBinTestIAMComp", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 2, 'feature/read.test')

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_read_bin_object(self):
        s3.uploadObject(self.s3_client, self.bucket_name, "feature/read.test/etc/input.gz", "input.gz")
        self._build_launch_validate("test_read_bin_object", "com.ibm.streamsx.objectstorage.test::ReadBinTestComp", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/read.test')

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_write_object_close_punct_static_name_final_punct(self):
        # expect 2 tuples received (one per object created)
        self._build_launch_validate("test_write_object_close_punct_static_name_final_punct", "com.ibm.streamsx.objectstorage.test::WriteTestClosePunctStaticObjectNameFinalPunctBasic", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/write.test')
        # expect 1 object per protocol (cos and s3a)
        self._check_created_objects(1, self.s3_client, self.bucket_name)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_write_object_close_punct_static_name_final_punct_iam(self):
        # expect 2 tuples received (one per object created)
        self._build_launch_validate("test_write_object_close_punct_static_name_final_punct_iam", "com.ibm.streamsx.objectstorage.test::WriteTestClosePunctStaticObjectNameFinalPunctIAM", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 2, 'feature/write.test')
        # expect 1 object per protocol (cos and s3a)
        self._check_created_objects(1, self.s3_client_iam, self.bucket_name_iam)

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_write_object_close_punct_dynamic_name(self):
        # expect 2 tuples received (one per object created)
        self._build_launch_validate("test_write_object_close_punct_dynamic_name", "com.ibm.streamsx.objectstorage.test::WriteTestClosePunctDynamicObjectNameBasic", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/write.test')
        # expect 1 object per protocol (cos and s3a)
        self._check_created_objects(1, self.s3_client, self.bucket_name)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_write_object_close_punct_dynamic_name_iam(self):
        # expect 2 tuples received (one per object created)
        self._build_launch_validate("test_write_object_close_punct_dynamic_name_iam", "com.ibm.streamsx.objectstorage.test::WriteTestClosePunctDynamicObjectNameIAM", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 2, 'feature/write.test')
        # expect 1 object per protocol (cos and s3a)
        self._check_created_objects(1, self.s3_client_iam, self.bucket_name_iam)

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_close_punct_dynamic_name(self):
        # expect 6 tuples received (one per object created) - test app creates 3 objects with cos and 3 with s3a protocol
        self._build_launch_validate("test_write_n_objects_close_punct_dynamic_name", "com.ibm.streamsx.objectstorage.test::WriteTestClosePunctDynamicObjectNameBasic", {'numObjects': 3, 'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 6, 'feature/write.test')
        # expect 3 objects per protocol (cos and s3a)
        self._check_created_objects(3, self.s3_client, self.bucket_name)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_close_punct_dynamic_name_iam(self):
        # expect 6 tuples received (one per object created) - test app creates 3 objects with cos and 3 with s3a protocol
        self._build_launch_validate("test_write_n_objects_close_punct_dynamic_name_iam", "com.ibm.streamsx.objectstorage.test::WriteTestClosePunctDynamicObjectNameIAM", {'numObjects': 3, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 6, 'feature/write.test')
        # expect 3 objects per protocol (cos and s3a)
        self._check_created_objects(3, self.s3_client_iam, self.bucket_name_iam)

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_close_by_tuples(self):
        # expect 6 tuples received (one per object created) - test app creates 3 objects with cos and 3 with s3a protocol
        self._build_launch_validate("test_write_n_objects_close_by_tuples", "com.ibm.streamsx.objectstorage.test::WriteTestCloseByTuplesBasic", {'numObjects': 3, 'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 6, 'feature/write.test')
        # expect 3 objects per protocol (cos and s3a)
        self._check_created_objects(3, self.s3_client, self.bucket_name)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_close_by_tuples_iam(self):
        # expect 6 tuples received (one per object created) - test app creates 3 objects with cos and 3 with s3a protocol
        self._build_launch_validate("test_write_n_objects_close_by_tuples_iam", "com.ibm.streamsx.objectstorage.test::WriteTestCloseByTuplesIAM", {'numObjects': 3, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 6, 'feature/write.test')
        # expect 3 objects per protocol (cos and s3a)
        self._check_created_objects(3, self.s3_client_iam, self.bucket_name_iam)

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_close_by_bytes(self):
        # expect 4 tuples received (one per object created) - test app creates 2 objects with cos and 2 with s3a protocol
        self._build_launch_validate("test_write_n_objects_close_by_bytes", "com.ibm.streamsx.objectstorage.test::WriteTestCloseByBytesBasic", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 4, 'feature/write.test')
        # expect 2 objects per protocol (cos and s3a)
        self._check_created_objects(2, self.s3_client, self.bucket_name)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_close_by_bytes_iam(self):
        # expect 4 tuples received (one per object created) - test app creates 2 objects with cos and 2 with s3a protocol
        self._build_launch_validate("test_write_n_objects_close_by_bytes_iam", "com.ibm.streamsx.objectstorage.test::WriteTestCloseByBytesIAM", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 4, 'feature/write.test')
        # expect 2 objects per protocol (cos and s3a)
        self._check_created_objects(2, self.s3_client_iam, self.bucket_name_iam)

    # -------------------

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_close_by_time(self):
        # expect at least two tuples received
        self._build_launch_validate("test_write_n_objects_close_by_time", "com.ibm.streamsx.objectstorage.test::WriteTestCloseByTimeBasic", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 2, 'feature/write.test', False)
        # expect at least one object per protocol (cos and s3a)
        self._check_created_objects(1, self.s3_client, self.bucket_name)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_write_n_objects_close_by_time_iam(self):
        # expect at least two tuples received
        self._build_launch_validate("test_write_n_objects_close_by_time_iam", "com.ibm.streamsx.objectstorage.test::WriteTestCloseByTimeIAM", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURIcos':self.uri_cos, 'objectStorageURIs3a':self.uri_s3a}, 2, 'feature/write.test', False)
        # expect at least one object per protocol (cos and s3a)
        self._check_created_objects(1, self.s3_client_iam, self.bucket_name_iam)

    # -------------------
    
    # samples/basic/TimeRollingPolicySample
    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_sample_TimeRollingPolicySample(self):
        self._build_launch_validate("test_sample_TimeRollingPolicySample", "com.ibm.streamsx.objectstorage.sample::TimeRollingPolicySampleBasic", {'objectName':'test_data_time_per_object_%TIME', 'timePerObject':10.0, 'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'objectStorageURI':self.uri_basic}, 1, self.object_storage_samples_location+'/basic/TimeRollingPolicySample', False)
        found = s3.isPresent(self.s3_client, self.bucket_name, 'test_data_time_per_object')
        assert (found), "Object not found"
    
    # samples/iam/TimeRollingPolicySample
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_sample_TimeRollingPolicySample_iam(self):
        self._build_launch_validate("test_sample_TimeRollingPolicySample_iam", "com.ibm.streamsx.objectstorage.sample.iam::TimeRollingPolicySampleIAM", {'objectName':'test_data_time_per_object_%TIME', 'timePerObject':10.0, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_s3a}, 1, self.object_storage_samples_location+'/iam/TimeRollingPolicySample', False)
        found = s3.isPresent(self.s3_client_iam, self.bucket_name_iam, 'test_data_time_per_object')
        assert (found), "Object not found"

    # -------------------

    # samples/basic/PartitionedParquetSample
    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_sample_PartitionedParquetSample(self):
        self._build_launch_validate("test_sample_PartitionedParquetSample", "com.ibm.streamsx.objectstorage.sample::PartitionedParquetSampleBasic", {'objectName':'test_data_time_per_object_%TIME', 'timePerObject':10.0, 'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'objectStorageURI':self.uri_basic}, 1, self.object_storage_samples_location+'/basic/PartitionedParquetSample', False)
        found = s3.isPresent(self.s3_client, self.bucket_name, 'test_data_time_per_object')
        assert (found), "Object not found"
    
    # samples/iam/PartitionedParquetSample
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_sample_PartitionedParquetSample_iam(self):
        self._build_launch_validate("test_sample_PartitionedParquetSample_iam", "com.ibm.streamsx.objectstorage.sample.iam::PartitionedParquetSampleIAM", {'objectName':'test_data_time_per_object_%TIME', 'timePerObject':10.0, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_s3a}, 1, self.object_storage_samples_location+'/iam/PartitionedParquetSample', False)
        found = s3.isPresent(self.s3_client_iam, self.bucket_name_iam, 'test_data_time_per_object')
        assert (found), "Object not found"

    # -------------------

    # samples/basic/SinkScanSourceSample
    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_sample_SinkScanSourceSample(self):
        self._build_launch_validate("test_sample_SinkScanSourceSample", "com.ibm.streamsx.objectstorage.sample::SinkScanSourceSampleBasic", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'objectStorageURI':self.uri_basic}, 1, self.object_storage_samples_location+'/basic/SinkScanSourceSample', False)
        found = s3.isPresent(self.s3_client, self.bucket_name, 'SAMPLE_')
        assert (found), "Object not found"
    
    # samples/iam/SinkScanSourceSample
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_sample_SinkScanSourceSample_iam(self):
        self._build_launch_validate("test_sample_SinkScanSourceSample_iam", "com.ibm.streamsx.objectstorage.sample.iam::SinkScanSourceSampleIAM", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_s3a}, 1, self.object_storage_samples_location+'/iam/SinkScanSourceSample', False)
        found = s3.isPresent(self.s3_client_iam, self.bucket_name_iam, 'SAMPLE_')
        assert (found), "Object not found"

    # -------------------

    # samples/basic/DynamicObjectNameSinkSample
    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_sample_DynamicObjectNameSinkSample(self):
        self._build_launch_validate("test_sample_DynamicObjectNameSinkSample", "com.ibm.streamsx.objectstorage.sample::DynamicObjectNameSinkSampleBasic", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'objectStorageURI':self.uri_basic}, 1, self.object_storage_samples_location+'/basic/DynamicObjectNameSinkSample', True)
        s3.validateObjects(self.s3_client, self.bucket_name, ["sample.txt"])
    
    # samples/iam/DynamicObjectNameSinkSample
    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_sample_DynamicObjectNameSinkSample_iam(self):
        self._build_launch_validate("test_sample_DynamicObjectNameSinkSample_iam", "com.ibm.streamsx.objectstorage.sample.iam::DynamicObjectNameSinkSampleIAM", {'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':self.uri_s3a}, 1, self.object_storage_samples_location+'/iam/DynamicObjectNameSinkSample', True)
        s3.validateObjects(self.s3_client_iam, self.bucket_name_iam, ["sample.txt"])

    # -------------------

    # samples/basic/FunctionsSample
    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_sample_FunctionsSample(self):
        tmp_bucket = 'streamsx-os-sample-' + str(time.time());
        tmp_bucket = tmp_bucket.replace(".", "")
        print("bucket for sample app: "+tmp_bucket)
        self._build_launch_validate("test_sample_FunctionsSample", "com.ibm.streamsx.objectstorage.sample::FunctionsSampleBasic", {'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':tmp_bucket}, 1, self.object_storage_samples_location+'/basic/FunctionsSample', True)

    # -------------------

    def test_compile_time_error_ObjectStorageScan_checkpoint_operatorDriven(self):
        th.verify_compile_time_error("ObjectStorageScan_checkpoint_operatorDriven", "CDIST3368E")

    def test_compile_time_error_ObjectStorageScan_checkpoint_periodic(self):
        th.verify_compile_time_error("ObjectStorageScan_checkpoint_periodic", "CDIST3367E")

    def test_compile_time_error_ObjectStorageScan_consistent_region_unsupported_configuration(self):
        th.verify_compile_time_error("ObjectStorageScan_consistent_region_unsupported_configuration", "CDIST3300E")

    def test_compile_time_error_ObjectStorageScan_invalid_output_port_attribute(self):
        th.verify_compile_time_error("ObjectStorageScan_invalid_output_port_attribute", "CDIST3309E")

    def test_compile_time_error_ObjectStorageSink_checkpoint_operatorDriven(self):
        th.verify_compile_time_error("ObjectStorageSink_checkpoint_operatorDriven", "CDIST3368E")

    def test_compile_time_error_ObjectStorageSink_checkpoint_periodic(self):
        th.verify_compile_time_error("ObjectStorageSink_checkpoint_periodic", "CDIST3367E")

    def test_compile_time_error_ObjectStorageSink_consistent_region_unsupported_configuration(self):
        th.verify_compile_time_error("ObjectStorageSink_consistent_region_unsupported_configuration", "CDIST3300E")

    def test_compile_time_error_ObjectStorageSink_invalid_output_port_attribute(self):
        th.verify_compile_time_error("ObjectStorageSink_invalid_output_port_attribute", "CDIST3330E")

    def test_compile_time_error_ObjectStorageSource_checkpoint_operatorDriven(self):
        th.verify_compile_time_error("ObjectStorageSource_checkpoint_operatorDriven", "CDIST3368E")

    def test_compile_time_error_ObjectStorageSource_checkpoint_periodic(self):
        th.verify_compile_time_error("ObjectStorageSource_checkpoint_periodic", "CDIST3367E")

    def test_compile_time_error_ObjectStorageSource_consistent_region_unsupported_configuration(self):
        th.verify_compile_time_error("ObjectStorageSource_consistent_region_unsupported_configuration", "CDIST3300E")

    def test_compile_time_error_ObjectStorageSource_missing_input_port_or_param(self):
        th.verify_compile_time_error("ObjectStorageSource_missing_input_port_or_param", "CDIST3348E")

    # -------------------


class TestInstall(TestDistributed):
    """ Test invocations of composite operators in local Streams instance using installed toolkit """

    @classmethod
    def setUpClass(self):
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.object_storage_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.objectstorage'
        self.object_storage_samples_location = self.streams_install+'/samples/com.ibm.streamsx.objectstorage'
        super().setUpClass()

    def setUp(self):
        Tester.setup_distributed(self)
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.object_storage_toolkit_location = self.streams_install+'/toolkits/com.ibm.streamsx.objectstorage'
        self.object_storage_samples_location = self.streams_install+'/samples/com.ibm.streamsx.objectstorage'

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
        # local toolkit from repository is used
        self.object_storage_toolkit_location = "../com.ibm.streamsx.objectstorage"

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

