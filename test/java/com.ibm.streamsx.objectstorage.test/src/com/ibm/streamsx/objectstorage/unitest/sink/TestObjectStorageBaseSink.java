package com.ibm.streamsx.objectstorage.unitest.sink;

import java.io.IOException;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.test.AbstractObjectStorageTest;
import com.ibm.streamsx.objectstorage.test.AuthenticationMode;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.test.Utils;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;

/**
 * ObjectStorageSink test for S3A protocol
 * @author streamsadmin
 *
 */
public abstract class TestObjectStorageBaseSink extends AbstractObjectStorageTest {
	
	//private static Logger logger = Topology.STREAMS_LOGGER;
	protected SPLStream _testData = null;
	protected int _tupleRate; 
	protected TestObjectStorageBaseSink _testInstance = null;
	protected String _outputFolder = null;
	protected String _expectedPath = null;
	
	
	protected static final String TEST_OUTPUT_ROOT_FOLDER = "/tmp/ost_unitest";
	protected static final String EXPECTED_ROOT_PATH_SUFFIX = "/test/java/com.ibm.streamsx.objectstorage.test/data/expected/";
	
	/*
	 * Ctor
	 */
	public TestObjectStorageBaseSink() {
		super();
		// check target folder existance - create if required
		if (Utils.folderExists(TEST_OUTPUT_ROOT_FOLDER)) {
			try {
				Utils.removeFolder(TEST_OUTPUT_ROOT_FOLDER);
			} catch (IOException ioe) {
				System.err.println("Failed to remove test output folder '" + TEST_OUTPUT_ROOT_FOLDER + "'. Error mgs: " + ioe.getMessage());
			}
		} 
		Utils.createFolder(TEST_OUTPUT_ROOT_FOLDER);
		_outputFolder = createTestOutput(this.getClass().getName());		
	}

	public void build(String testName, TraceLevel logLevel, String topologyType, String protocol, AuthenticationMode authMode, String bucket) throws Exception {
		 super.build(testName, logLevel, topologyType, protocol, authMode, bucket);

	}
		

	public abstract void initTestData() throws Exception;

	/**
	 * Test object storage sink operator
	 */
	public void createObjectTest(String protocol) throws Exception {
		
		try {
			initTestData();		
			genTestSpecificParams(_testConfiguration);
			_expectedPath = getTestRoot() + EXPECTED_ROOT_PATH_SUFFIX;
			
			System.out.println("Configuring OSSink invocation with params: ");
			for (String key:  _testConfiguration.keySet()) {
				System.out.println("\t " + key + "=" + _testConfiguration.get(key));
			}
			SPLStream osSink = SPL.invokeOperator(Constants.OBJECT_STORAGE_SINK_OP_NS + "::" + Constants.OBJECT_STORAGE_SINK_OP_NAME, 
												  _testData, Constants.OS_SINK_OUT_SCHEMA, _testConfiguration);     
			
			validateResults(osSink, protocol);
		} catch (Exception e) {
			System.err.println("Test failed: " + e.getMessage());
			e.printStackTrace();
			throw new Exception(e);
		}
	}
	
	public String getDefaultBucket() {
		return Constants.DEFAULT_BUCKET_NAME;
	}

	public String getDefaultContainer() {
		return Constants.DEFAULT_CONTAINER_NAME;
	}


	public String createTestOutput(String testName) {
		String testOutputFolderName = TEST_OUTPUT_ROOT_FOLDER + "/" + testName + "/";
		if (!Utils.folderExists(testOutputFolderName)) Utils.createFolder(testOutputFolderName);
	
		return testOutputFolderName;
	}
}
