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
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.test.Utils;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Tests object rolling policy "by time".
 * Sink operator parameterization: tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>
 *  1. output object close time: 10 seconds
 *  2. storage format: parquet
 * 
 * @author streamsadmin
 *
 */
public class TestCloseByTimeParquet extends TestObjectStorageBaseSink {

	public TestCloseByTimeParquet() {
		super();
	}
	
	@Before
	public void prepareTest() {
		_testInstance = new TestCloseByTimeParquet();
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
	public void testCOS() throws Exception {
		String testName = Constants.COS + TestCloseByTimeParquet.class.getName();		
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.COS, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);	
	}
	
	@Test
	public void testS3A() throws Exception {
		String testName = Constants.S3A + TestCloseByTimeParquet.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}

	@Test
	public void testSwift2d() throws Exception {
		String testName = Constants.SWIFT2D + TestCloseByTimeParquet.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.SWIFT2D, Constants.DEFAULT_CONTAINER_NAME);
		_testInstance.createObjectTest(Constants.SWIFT2D);
	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String storageFormat = "parquet";
		String objectName = _protocol + TestCloseByTimeParquet.class.getSimpleName() + ".%OBJECTNUM." + storageFormat; 
		params.put("objectName", objectName);
		params.put("timePerObject", 10.0);
		params.put("storageFormat", storageFormat);
	}


	
	public int getTestTimeout() {
		return 40;
	}

	public void validateResults(SPLStream osSink, String protocol) throws Exception {
		// Sink operator generates single output tuple per object
		// containing object name and size
		Condition<Long> expectedCount = _tester.atLeastTupleCount(osSink, 1);

		// @TODO:should returned object name starts with "/"
		String expectedObjectName = "/" + ((String) _testConfiguration.get("objectName")).replace("%OBJECTNUM", "0");		
		System.out.println("Expected Object name: " + expectedObjectName);
		
		Tuple expectedTuple = Constants.OS_SINK_OUT_SCHEMA
				.getTuple(new Object[] { new RString(expectedObjectName), new Long(9178) });
		Condition<List<Tuple>> expectedTuples = _tester.tupleContents(osSink, expectedTuple);

		// build and run application
		complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);		

		// check that at least one tuple returned
		assertTrue(expectedCount.toString(), expectedCount.valid());		
		//assertTrue(expectedTuples.toString(), expectedTuples.valid());
	}

}
