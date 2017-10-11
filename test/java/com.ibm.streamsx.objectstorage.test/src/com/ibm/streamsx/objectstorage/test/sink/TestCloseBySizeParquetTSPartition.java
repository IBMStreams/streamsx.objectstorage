package com.ibm.streamsx.objectstorage.test.sink;

import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;
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
 *  2. storage format: parquet
 *  3. parquet partition attributes: customerId
 * 
 * @author streamsadmin
 *
 */
public class TestCloseBySizeParquetTSPartition extends TestObjectStorageBaseSink {

	public TestCloseBySizeParquetTSPartition() {
		super();
	}
	
	@Before
	public void prepareTest() {
		_testInstance = new TestCloseBySizeParquetTSPartition();
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

//	@Test - timestamp partitioning works from SPL, but not in unitests. Should be resolved!
	public void testCOSBasicAuthSchema() throws Exception {
		String testName = Constants.COS + TestCloseBySizeParquetTSPartition.class.getName();		
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.COS, AuthenticationMode.BASIC, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);	
	}
	
	public void testCOSIAMAuthSchema() throws Exception {
		String testName = Constants.COS + TestCloseBySizeParquetTSPartition.class.getName();		
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.COS, AuthenticationMode.IAM, Constants.DEFAULT_IAM_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.COS);	
	}

	
//	@Test - timestamp partitioning works from SPL
	public void testS3ABasicAuthSchema() throws Exception {
		String testName = Constants.S3A + TestCloseBySizeParquetTSPartition.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, AuthenticationMode.BASIC, Constants.DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}


	public void testS3AIAMAuthSchema() throws Exception {
		String testName = Constants.S3A + TestCloseBySizeParquetTSPartition.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.S3A, AuthenticationMode.IAM, Constants.DEFAULT_IAM_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.S3A);
	}

	
//	@Test - as folders are not supported by swift - the feature is blocked on a toolkit level
	public void testSwift2d() throws Exception {
		String testName = Constants.SWIFT2D + TestCloseBySizeParquetTSPartition.class.getName();
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.SWIFT2D, AuthenticationMode.BASIC, Constants.DEFAULT_CONTAINER_NAME);
		_testInstance.createObjectTest(Constants.SWIFT2D);
	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String storageFormat = "parquet";
		String objectName = _protocol + TestCloseBySizeParquetTSPartition.class.getSimpleName()  + "%OBJECTNUM." + storageFormat; 
		params.put("objectName", objectName);
		params.put("storageFormat", storageFormat);
		params.put("bytesPerObject", 10 * 1024L); // 1OK per object
		String[] partitionAttrs = {"ts"};
		params.put("partitionValueAttributes", partitionAttrs);
	}


	
	public int getTestTimeout() {
		return 40;
	}

	public void validateResults(SPLStream osSink, String protocol) throws Exception {
		// @TODO: work on more precise results validation logic
		
		// Sink operator generates single output tuple per object
		// containing object name and size
		Condition<Long> expectedCount = _tester.atLeastTupleCount(osSink, 1);

		// build and run application
		complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);		

		// check that at least one tuple returned
		assertTrue(expectedCount.toString(), expectedCount.valid());
	}

}
