package com.ibm.streamsx.objectstorage.test.sink;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.objectstorage.test.AuthenticationMode;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.test.Utils;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Condition;


/**
 * Tests object rolling policy "by tuple count".
 * Sink operator input schema:  tuple<timestamp line>
 *  1. output object tuple count: 1000 records
 *  2. storage format: parquet
 * 
 * @author streamsadmin
 *
 */
public class TestCloseByTupleCountSimpleInSchema extends TestObjectStorageBaseSink {

	public TestCloseByTupleCountSimpleInSchema() {
		super();
	}
	
	@Before
	public void prepareTest() {
		_testInstance = new TestCloseByTupleCountSimpleInSchema();
	}

	@Override
	public void initTestData() throws Exception {	
		_testData = Utils.getEndlessStringStream(_testTopology, 100);
	}

	@Test
	public void testCOSBasicAuthSchema() throws Exception {
		String testName = Constants.COS + TestCloseByTupleCountSimpleInSchema.class.getName();		
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.COS, AuthenticationMode.BASIC, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);	
	}
	
	@Test
	public void testCOSIAMAuthSchema() throws Exception {
		String testName = Constants.COS + TestCloseByTupleCountSimpleInSchema.class.getName();		
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.COS, AuthenticationMode.IAM, Constants.DEFAULT_IAM_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);	
	}

	@Test
	public void testS3ABasicAuthSchema() throws Exception {
		String testName = Constants.S3A + TestCloseByTupleCountSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, AuthenticationMode.BASIC, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}

	@Test
	public void testS3AIAMAuthSchema() throws Exception {
		String testName = Constants.S3A + TestCloseByTupleCountSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, AuthenticationMode.IAM, Constants.DEFAULT_IAM_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}

	@Test
	public void testSwift2dBasicAuthSchema() throws Exception {
		String testName = Constants.SWIFT2D + TestCloseByTupleCountSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.SWIFT2D, AuthenticationMode.BASIC, Constants.DEFAULT_CONTAINER_NAME);
		_testInstance.createObjectTest(Constants.SWIFT2D);
	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) {
		params.put("objectName", _protocol + TestCloseByTupleCountSimpleInSchema.class.getSimpleName() + "%OBJECTNUM.txt");
		params.put("tuplesPerObject", 1000L);
	}


	
	public int getTestTimeout() {
		return 40;
	}

	public void validateResults(SPLStream osSink, String protocol) throws Exception {
		// Sink operator generates single output tuple per object
		// containing object name and size
		Condition<Long> expectedCount = _tester.tupleCount(osSink, 1);

		// @TODO:should returned object name starts with "/"
		String expectedObjectName = "/" + ((String) _testConfiguration.get("objectName")).replace("%OBJECTNUM", "0");		
		System.out.println("Expected Object name: " + expectedObjectName);
		
		Tuple expectedTuple = Constants.OS_SINK_OUT_SCHEMA
				.getTuple(new Object[] { new RString(expectedObjectName), new Long(14000) });
		Condition<List<Tuple>> expectedTuples = _tester.tupleContents(osSink, expectedTuple);

		// build and run application
		complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);		

		// check that at least one tuple returned
		assertTrue(expectedCount.toString(), expectedCount.valid());
		assertTrue(expectedTuples.toString(), expectedTuples.valid());
	}
}
