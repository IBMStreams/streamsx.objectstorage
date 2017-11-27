package com.ibm.streamsx.objectstorage.unitest.sink;

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
import com.ibm.streamsx.objectstorage.test.sink.TestObjectStorageBaseSink;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Condition;


/**
 * Tests object rolling policy "by object size" with header.
 * Sink operator input schema:  tuple<rstring line>
 * Sink operator parameterization:
 *  1. output object size: 50K
 *  2. storage format: raw
 *  3. header row: "HEADER"
 * 
 * @author streamsadmin
 *
 */
public class TestCloseBySizeSimpleInSchema extends TestObjectStorageBaseSink {
	
	private static final long OBJECT_SIZE = 50L * 1024L;
	
	/*
	 * Ctor
	 */
	public TestCloseBySizeSimpleInSchema()  {
		super();
	}

	@Before
	public void prepareTest() throws Exception {	
		_testInstance = new TestCloseBySizeSimpleInSchema();
	}

	@Override
	public void initTestData() throws Exception {
		_testData = Utils.getEndlessStringStream(_testTopology, 100);		
	}

	@Test
	public void testCOSBasicAuthSchema() throws Exception {
		String testName = Constants.COS + TestCloseBySizeSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.DISTRIBUTED, Constants.COS, AuthenticationMode.BASIC, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);
	}

	@Test
	public void testCOSIAMAuthSchema() throws Exception {
		String testName = Constants.COS + TestCloseBySizeSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.COS, AuthenticationMode.IAM, Constants.DEFAULT_IAM_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);
	}

	@Test
	public void testS3ABasicAuthSchema() throws Exception {
		String testName = Constants.S3A + TestCloseBySizeSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, AuthenticationMode.BASIC, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}

	@Test
	public void testS3AIAMAuthSchema() throws Exception {
		String testName = Constants.S3A + TestCloseBySizeSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, AuthenticationMode.IAM, Constants.DEFAULT_IAM_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}

	@Test
	public void testSwift2d() throws Exception {
		String testName = Constants.SWIFT2D + TestCloseBySizeSimpleInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.SWIFT2D, AuthenticationMode.BASIC, Constants.DEFAULT_CONTAINER_NAME);
		_testInstance.createObjectTest(Constants.SWIFT2D);
	}
		

	@Override
	public void genTestSpecificParams(Map<String, Object> params) {
		params.put("objectName", _protocol + TestCloseBySizeSimpleInSchema.class.getSimpleName() + "%OBJECTNUM.txt");
		params.put("bytesPerObject", OBJECT_SIZE);
		params.put("headerRow", "HEADER");
	}
	
	public int getTestTimeout() {
		return 40;
	}

	@Override
	public void validateResults(SPLStream osSink, String protocol) throws Exception {
		// @TODO:should returned object name starts with "/"
		String expectedObjectName = "/" + ((String) _testConfiguration.get("objectName")).replace("%OBJECTNUM", "0");		
		System.out.println("Expected Object name: " + expectedObjectName);
				
		Condition<Long> expectedCount = _tester.atLeastTupleCount(osSink, 1);

		Tuple expectedTuple = Constants.OS_SINK_OUT_SCHEMA
				.getTuple(new Object[] { new RString(expectedObjectName), new Long(55139) });
		Condition<List<Tuple>> expectedTuples = _tester.tupleContents(osSink, expectedTuple);

		
		// build and run application
		complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);		


		// check that at least one tuple returned
		assertTrue(expectedCount.toString(), expectedCount.valid());
		//assertTrue(expectedTuples.toString(), expectedTuples.valid());

	}
}
