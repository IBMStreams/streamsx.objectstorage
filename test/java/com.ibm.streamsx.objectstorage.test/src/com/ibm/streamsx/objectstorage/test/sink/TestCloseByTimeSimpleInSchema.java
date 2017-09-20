package com.ibm.streamsx.objectstorage.test.sink;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.test.Utils;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Condition;


/**
 * Tests object rolling policy "by time".
 * Sink operator input schema:  tuple<rstring line>
 *  1. output object close time: 10 seconds
 *  2. storage format: raw
 * 
 * @author streamsadmin
 *
 */
public class TestCloseByTimeSimpleInSchema extends TestObjectStorageBaseSink {

	/*
	 * Ctor
	 */
	public TestCloseByTimeSimpleInSchema()  {
		super();
	}

	@Before
	public void prepareTest() {
		_testInstance = new TestCloseByTimeSimpleInSchema();
	}

	@Override
	public void initTestData() throws Exception {
		_testData = Utils.getEndlessStringStream(_testTopology, 100);		
	}
	
	@Test
	public void testCOS() throws Exception {
		String testName = Constants.COS + TestCloseByTimeSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.COS, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);
	}

	@Test
	public void testS3A() throws Exception {
		String testName = Constants.S3A + TestCloseByTimeSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}

	@Test
	public void testSwift2d() throws Exception {
		String testName = Constants.SWIFT2D + TestCloseByTimeSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.SWIFT2D, Constants.DEFAULT_CONTAINER_NAME);
		_testInstance.createObjectTest(Constants.SWIFT2D);
	}
		
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) {
		params.put("objectName", _protocol +  TestCloseByTimeSimpleInSchema.class.getSimpleName() + " %OBJECTNUM.txt");
		params.put("timePerObject", 10.0); 
	}

	/**
	 * Test case runtime timeout
	 */
	public int getTestTimeout() {
		return 60;
	}

	@Override
	public void validateResults(SPLStream osSink, String protocol) throws Exception {
		// @TODO:should returned object name starts with "/"
		String expectedObjectName = ((String) _testConfiguration.get("objectName")).replace("%OBJECTNUM", "0");		
		System.out.println("Expected Object name: " + expectedObjectName);
				
		Condition<Long> expectedCount = _tester.atLeastTupleCount(osSink, 1);

		// build and run application
		complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);		

		// check that at least one tuple returned
		assertTrue(expectedCount.toString(), expectedCount.valid());
	}
}
