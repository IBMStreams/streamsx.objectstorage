package com.ibm.streamsx.objectstorage.test.sink;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.objectstorage.test.AbstractObjectStorageTest;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.test.Utils;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streams.operator.logging.TraceLevel;

/**
 * ObjectStorageSink test for S3A protocol
 * @author streamsadmin
 *
 */
public abstract class TestObjectStorageBaseSink extends AbstractObjectStorageTest {
	
	private static Logger logger = Topology.STREAMS_LOGGER;
	protected SPLStream _testData = null;
	protected int _tupleRate; 
	protected TestObjectStorageBaseSink _testInstance = null;
	
	
	
	/*
	 * Ctor
	 */
	public TestObjectStorageBaseSink() {
		super();
	}

	public void build(String testName, TraceLevel logLevel, String topologyType, String protocol, String bucket) throws Exception {
		 super.build(testName, logLevel, topologyType, protocol, bucket);
	}
		

	public abstract void initTestData() throws Exception;

	/**
	 * Test object storage sink operator
	 */
	public void createObjectTest(String protocol) throws Exception {
		
		initTestData();		
		genTestSpecificParams(_testConfiguration);
		SPLStream osSink = SPL.invokeOperator(Constants.OBJECT_STORAGE_SINK_OP_NS + "::" + Constants.OBJECT_STORAGE_SINK_OP_NAME, 
											  _testData, Constants.OS_SINK_OUT_SCHEMA, _testConfiguration);        
		validateResults(osSink, protocol);
		
	}
	
	public String getDefaultBucket() {
		return Constants.DEFAULT_BUCKET_NAME;
	}

	public String getDefaultContainer() {
		return Constants.DEFAULT_CONTAINER_NAME;
	}


}
