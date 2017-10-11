package com.ibm.streamsx.objectstorage.test.sink;

import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
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
 * Tests object rolling policy "by object size".
 * Sink operator input schema:  tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>
 * Sink operator parameterization:
 *  1. output object size: 10K
 *  2. output object data attribute: longitude
 *  3. storage format: raw
 * 
 * @author streamsadmin
 *
 */
public class TestCloseBySizeComplexInSchema extends TestObjectStorageBaseSink {

	public TestCloseBySizeComplexInSchema() {
		super();
	}
	
	@Before
	public void prepareTest() {
		_testInstance = new TestCloseBySizeComplexInSchema();
	}

	@Override
	public void initTestData() throws Exception {	
		//configureTest(Level.FINEST, Constants.STANDALONE);

		// data injection composite	
		_tupleRate = 100; // tuple rate in tuples per second
		_testDataFileName = Constants.OS_MULTI_ATTR_TEST_OBJECT_NAME;
		
		String injectionOutShema = "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>"; // input schema
				
		// generates injection logic based on input loaded from file
		// populates sample data stream
		Tuple[] testTuples = Utils.genTuplesFromFile(_osDefaultTestDataFileAbsPath, _testDataFileName, Constants.TEST_DATA_FILE_DELIMITER, injectionOutShema);
				
		_testData = Utils.getTestStream(_testTopology, testTuples, injectionOutShema, _tupleRate); 
	}

	@Test
	public void testCOSBasicAuthSchema() throws Exception {
		String testName = Constants.COS + TestCloseBySizeComplexInSchema.class.getName();		
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.COS, AuthenticationMode.BASIC, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);	
	}
	
	@Test
	public void testCOSIAMAuthSchema() throws Exception {
		String testName = Constants.COS + TestCloseBySizeComplexInSchema.class.getName();		
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.COS, AuthenticationMode.IAM, Constants.DEFAULT_IAM_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);	
	}


	@Test
	public void testS3ABasicAuthSchema() throws Exception {
		String testName = Constants.S3A + AuthenticationMode.BASIC + TestCloseBySizeComplexInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, AuthenticationMode.BASIC, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}

	@Test
	public void testS3AIAMAuthSchema() throws Exception {
		String testName = Constants.S3A  + AuthenticationMode.IAM + TestCloseBySizeComplexInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, AuthenticationMode.IAM, Constants.DEFAULT_IAM_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}

	@Test
	public void testSwift2d() throws Exception {
		String testName = Constants.SWIFT2D + TestCloseBySizeComplexInSchema.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.SWIFT2D, AuthenticationMode.BASIC, Constants.DEFAULT_CONTAINER_NAME);
		_testInstance.createObjectTest(Constants.SWIFT2D);
	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String objectName = _protocol + TestCloseBySizeComplexInSchema.class.getSimpleName() + "%OBJECTNUM.txt"; 
		params.put("objectName", objectName);
		params.put("dataAttribute", _testData.getSchema().getAttribute("longitude"));
		params.put("bytesPerObject", 10L * 1024L); // 10K per object
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
				.getTuple(new Object[] { new RString(expectedObjectName), new Long(9784) });
		Condition<List<Tuple>> expectedTuples = _tester.tupleContents(osSink, expectedTuple);

		// build and run application
		complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);		

		// check that at least one tuple returned
		assertTrue(expectedCount.toString(), expectedCount.valid());
		assertTrue(expectedTuples.toString(), expectedTuples.valid());
	}

}
