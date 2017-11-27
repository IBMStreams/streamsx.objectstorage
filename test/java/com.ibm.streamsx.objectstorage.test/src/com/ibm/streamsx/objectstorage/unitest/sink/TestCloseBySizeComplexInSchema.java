package com.ibm.streamsx.objectstorage.unitest.sink;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.objectstorage.test.AuthenticationMode;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.test.Utils;
import com.ibm.streamsx.objectstorage.unitest.sink.TestObjectStorageBaseSink;
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

	private static final String OUT_EXTENSION = "txt";


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
		_tupleRate = 1000; // tuple rate in tuples per second
		_testDataFileName = Constants.OS_MULTI_ATTR_TEST_OBJECT_NAME;
						
		
		String injectionOutShema = "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>"; // input schema
				
		// generates injection logic based on input loaded from file
		// populates sample data stream
		Tuple[] testTuples = Utils.genTuplesFromFile(_osDefaultTestDataFileAbsPath, _testDataFileName, Constants.TEST_DATA_FILE_DELIMITER, injectionOutShema);
				
		_testData = Utils.getTestStream(_testTopology, testTuples, injectionOutShema, _tupleRate); 
	}


	@Test
	public void testLocalBasicAuthSchema() throws Exception {
		String testName = Constants.FILE + TestCloseBySizeComplexInSchema.class.getName();		
		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.FILE, AuthenticationMode.BASIC, Constants.FILE_DEFAULT_BUCKET_NAME);
		_testInstance.createObjectTest(Constants.FILE);	
	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String objectName = _outputFolder + _protocol + TestCloseBySizeComplexInSchema.class.getSimpleName() + "%OBJECTNUM." + OUT_EXTENSION; 
		params.put("objectName", objectName);
		params.put("dataAttribute", _testData.getSchema().getAttribute("longitude"));
		params.put("bytesPerObject", 3L * 1024L); // 10K per object
	}

	public int getTestTimeout() {
		return 5;
	}

	public void validateResults(SPLStream osSink, String protocol) throws Exception {
		// Sink operator generates single output tuple per object
		// containing object name and size
		Condition<Long> expectedCount = _tester.atLeastTupleCount(osSink, 2);

		// @TODO:should returned object name starts with "/"
		String expectedObjectName = ((String) _testConfiguration.get("objectName")).replace("%OBJECTNUM", "0");		
		System.out.println("Expected Object name: " + expectedObjectName);
			
		Tuple expectedTuple = Constants.OS_SINK_OUT_SCHEMA
				.getTuple(new Object[] { new RString(expectedObjectName), new Long(3072), new RString(expectedObjectName), new Long(3073)});
		Condition<List<Tuple>> expectedTuples = _tester.tupleContents(osSink, expectedTuple);

		// build and run application
		complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);		
		
		// check that at least one tuple returned
		assertTrue(expectedCount.toString(), expectedCount.valid());
		assertTrue(expectedTuples.toString(), expectedTuples.valid());

		String expectedPath = _expectedPath + this.getClass().getName();		
		System.out.println("Expected data location: '" + expectedPath + "'");
		HashMap<String, File> expected = Utils.getFilesInFolder(expectedPath, OUT_EXTENSION);
		System.out.println("Actual data location: '" + _outputFolder + "'");
		HashMap<String, File> actual = Utils.getFilesInFolder(_outputFolder, OUT_EXTENSION);
		
		System.out.println("Expected objects count: '" + expected.size() + "''");
		System.out.println("Actual objects count: '" + actual.size() + "'");
		
		// checks that number of expected and 
		// actual objects is the same
		assertTrue(expected.size() == actual.size());
		
		
		// checks that the file content is the same		
		for (String key: expected.keySet()) {
			assertTrue(actual.containsKey(key));
			boolean expectedEqActual = FileUtils.contentEquals(expected.get(key), actual.get(key));
			// expected and actual are different - show differences
			if (!expectedEqActual) {
				Utils.showTextFileDiffs(expected.get(key), actual.get(key));
				assertTrue(true);
			}
			
		}
	}


	@After
	public void testCleanup() {
		
	}
}
