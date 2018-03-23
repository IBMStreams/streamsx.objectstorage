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

class TestDistributed(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.iam_api_key, self.service_instance_id = th.read_iam_credentials()
        if (self.iam_api_key != "") and (self.service_instance_id) :
            self.bucket_name_iam = s3.createBucketIAM()

        self.access_key, self.secret_access_key = th.read_credentials()
        if (self.access_key != "") and (self.secret_access_key != "") :
            self.bucket_name = s3.createBucket()

#    @classmethod
#    def tearDownClass(self):
#        print ("clean-up")

    def setUp(self):
        Tester.setup_distributed(self)
        self.object_storage_toolkit_location = "../com.ibm.streamsx.objectstorage"

    def _add_toolkits(self, topo):
        tk.add_toolkit(topo, 'performance/com.ibm.streamsx.objectstorage.s3.test')
        tk.add_toolkit(topo, self.object_storage_toolkit_location)

    def _build_launch_validate(self, name, composite_name, parameters, num_result_tuples):
        topo = Topology(name)
        self._add_toolkits(topo)
	
        params = parameters
        # Call the test composite
        test_op = op.Source(topo, composite_name, 'tuple<rstring result>', params=params)
        self.tester = Tester(topo)
        self.tester.run_for(60)
        self.tester.tuple_count(test_op.stream, num_result_tuples, exact=True)

        # Run the test
        self.tester.test(self.test_ctxtype, self.test_config, assert_on_fail=True, always_collect_logs=False)
        print (str(self.tester.result))

    @unittest.skipIf(th.cos_credentials() == False, "Missing "+th.COS_CREDENTIALS()+" environment variable.")
    def test_s3a_write(self):
        self._build_launch_validate("test_s3_write", "com.ibm.streamsx.objectstorage.s3.test::WriteDurationTestComp", {'dataSize':100000, 'numObjects':5, 'accessKeyID':self.access_key, 'secretAccessKey':self.secret_access_key, 'bucket':self.bucket_name}, 1)

    @unittest.skipIf(th.iam_credentials() == False, "Missing "+th.COS_IAM_CREDENTIALS()+" environment variable.")
    def test_s3a_write_iam(self):
        uri = "s3a://"+self.bucket_name_iam+"/"
        self._build_launch_validate("test_s3a_write_iam", "com.ibm.streamsx.objectstorage.s3.test::WriteDurationTestIAMComp", {'dataSize':100000, 'numObjects':5, 'IAMApiKey':self.iam_api_key, 'IAMServiceInstanceId':self.service_instance_id, 'objectStorageURI':uri}, 1)


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


