package com.ibm.streamsx.objectstorage.test.sink;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.test.AbstractObjectStorageTest;
import com.ibm.streamsx.objectstorage.test.AuthenticationMode;
import com.ibm.streamsx.objectstorage.test.Constants;
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
	
	
	
	/*
	 * Ctor
	 */
	public TestObjectStorageBaseSink() {
		super();
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


}
