package com.ibm.streamsx.objectstorage.unitest.sink;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.test.AbstractObjectStorageTest;
import com.ibm.streamsx.objectstorage.test.AuthenticationMode;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.test.Utils;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * ObjectStorageSink test for S3A protocol
 * @author streamsadmin
 *
 */
public abstract class TestObjectStorageBaseSink extends AbstractObjectStorageTest {
	
	//private static Logger logger = Topology.STREAMS_LOGGER;
	protected SPLStream _testData = null;
	protected int _tupleRate; 
//	protected TestObjectStorageBaseSink _testInstance = null;
	protected String _outputFolder = null;
	protected String _expectedPath = null;
	protected List<Tuple> _expectedTupleList = new LinkedList<Tuple>();
	
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
	/**
	 * Test object storage sink operator
	 */
	public void createObjectTest(String protocol) throws Exception {
		
		try {
			initTestData();		
			genTestSpecificParams(_testConfiguration);
			_expectedPath = Utils.getTestRoot() + EXPECTED_ROOT_PATH_SUFFIX;
			
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


	public void runUnitest(Class clazz) throws Exception {
		String testName = Constants.FILE + clazz.getName();		
//		_testInstance.build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.FILE, AuthenticationMode.BASIC, Constants.FILE_DEFAULT_BUCKET_NAME);
//		_testInstance.createObjectTest(Constants.FILE);	
		build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.FILE, AuthenticationMode.BASIC, Constants.FILE_DEFAULT_BUCKET_NAME);
		createObjectTest(Constants.FILE);	
	}
	
	public int getTestTimeout() {
		return 5;
	}

	public void checkOperatorOutputData(String outExtension) throws Exception {
		String expectedPath = _expectedPath + this.getClass().getName();		
		HashMap<String, File> expected = Utils.getFilesInFolder(expectedPath, outExtension);
		HashMap<String, File> actual = Utils.getFilesInFolder(_outputFolder, outExtension);
		
		System.out.println("-> Expected operator output location: '" + expectedPath + "'");
		System.out.println("-> Expected output tuples count: '" + expected.size() + "'");

		System.out.println("-> Actual operator output location: '" + _outputFolder + "'");
		System.out.println("-> Actual output tuples count: '" + actual.size() + "'");
		
		// checks that number of actual files is greater 
		// than expected
		assertTrue(actual.size() >= expected.size());
		
		
		// checks that the file content is the same		
		for (String key: expected.keySet()) {
			assertTrue(actual.containsKey(key));
			boolean expectedEqActual = FileUtils.contentEquals(expected.get(key), actual.get(key));
			// expected and actual are different - show differences
			System.out.println("-> Comparing expected '" + expected.get(key) + "' with actual '" + actual.get(key) + "'");
			if (!expectedEqActual) {
				System.out.println("-> EXPECTED AND ACTUAL ARE DIFFERENT");
				Utils.showTextFileDiffs(expected.get(key), actual.get(key));
				assertTrue(true);
			} else {
				System.out.println("-> EXPECTED AND ACTUAL ARE SAME");
			}
		}
	}

	public void checkOperatorOutTuples(Condition<Long> expectedCount, 
									   List<Tuple> expectedTuples, 
									   List<Tuple> resultTuples, 
									   String[] skipAttributes) {
		
		// check that at least one tuple returned
		assertTrue(expectedCount.toString(), expectedCount.valid());
		List<String> attrNames = new ArrayList<String>(expectedTuples.get(0).getStreamSchema().getAttributeNames());		
		
		attrNames.removeAll(Arrays.asList(skipAttributes));
		for (int i = 0; i < Math.min(resultTuples.size(), expectedTuples.size()); i++) {
			for (String attrName: attrNames) {				
				assertTrue(resultTuples.get(i).getObject(attrName).equals(expectedTuples.get(i).getObject(attrName)));
			}
		}

	}
	
//	@Before
//	public void prepareTest() {
//		try {
//			_testInstance = this.getClass().newInstance();
//		} catch (IllegalAccessException e) {
//			org.junit.Assert.fail("Failed to instantiate test '" + this.getClass().getName() + "'. Error: " + e.getMessage());
//		} catch (InstantiationException e) {
//			org.junit.Assert.fail("Failed to instantiate test '" + this.getClass().getName() + "'. Error: " + e.getMessage());
//		}
//	}


}
