package com.ibm.streamsx.objectstorage.unitest.sink;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.objectstorage.test.AbstractObjectStorageTest;
import com.ibm.streamsx.objectstorage.test.AuthenticationMode;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.test.OSTFileUtils;
import com.ibm.streamsx.objectstorage.test.OSTParquetFileUtils;
import com.ibm.streamsx.objectstorage.test.OSTRawFileUtils;
import com.ibm.streamsx.objectstorage.test.Utils;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * ObjectStorageSink test for S3A protocol
 * @author streamsadmin
 *
 */
public abstract class BaseObjectStorageTestSink extends AbstractObjectStorageTest {
	
	protected SPLStream _testData = null;
	protected int _tupleRate; 
	protected String _outputFolder = null;
	protected String _expectedPath = null;
	protected List<Tuple> _expectedTupleList = new LinkedList<Tuple>();
	
	protected static final String TEST_OUTPUT_ROOT_FOLDER = "/tmp/ost_unitest";
	protected static final String EXPECTED_ROOT_PATH_SUFFIX = "/test/java/com.ibm.streamsx.objectstorage.test/data/expected/";
	protected static final int DEFAULT_TUPLE_RATE = 1000;
	protected static final String TXT_OUT_EXTENSION = "txt";	
	protected static final String PARQUET_OUT_EXTENSION = "parquet";
	protected static final int SHUTDOWN_DELAY = 5;
	protected static final boolean GEN_EMPTY_STR_DATA = false;
	
	/*
	 * Ctor
	 */
	public BaseObjectStorageTestSink() {
		super();
		// check target folder existance - create if required
		if (!OSTRawFileUtils.getInstance().folderExists(TEST_OUTPUT_ROOT_FOLDER)) {
			OSTRawFileUtils.getInstance().createFolder(TEST_OUTPUT_ROOT_FOLDER);
		}
		_outputFolder = createTestOutput(this.getClass().getName());		
		
	}

	
	
	
	public void build(String testName, TraceLevel logLevel, String topologyType, String protocol, AuthenticationMode authMode, String bucket) throws Exception {
		 super.build(testName, logLevel, topologyType, protocol, authMode, bucket);

	}
		
	public void initTestData() throws Exception {
		initTestData(DEFAULT_TUPLE_RATE, GEN_EMPTY_STR_DATA);
	}
	
	public abstract String getInjectionOutSchema();
	
	public void initTestData(int tupleRate, boolean genEmptyStrData) throws Exception {	

		// data injection composite	
		_tupleRate = tupleRate; // tuple rate in tuples per second
		_testDataFileName = getTestDataFileName();
						
		
		String injectionOutShema = getInjectionOutSchema();
				
		// generates injection logic based on input loaded from file
		// populates sample data stream
		Tuple[] testTuples = Utils.genTuplesFromFile(_osDefaultTestDataFileAbsPath, _testDataFileName, Constants.TEST_DATA_FILE_DELIMITER, injectionOutShema);					
		
		_testData = genEmptyStrData ? 
				Utils.getTestStreamWithEmptyStr(_testTopology, testTuples, injectionOutShema, _tupleRate):
				Utils.getTestStream(_testTopology, testTuples, injectionOutShema, _tupleRate);	
	}
	
	public String getTestDataFileName() {
		return Constants.OS_MULTI_ATTR_2K_TEST_OBJECT_NAME;
	}
	
	/**
	 * Test object storage sink operator
	 */
	public void createObjectTest(String protocol) throws Exception {
		createObjectTest(protocol, Constants.DEFAULT_SINK_PARALLELITY_LEVEL);
	}
	
	public void createObjectTest(String protocol, int sinkParallelityLevel) throws Exception {
		
		try {
			initTestData();		
			genTestSpecificParams(_testConfiguration);
			_expectedPath = Utils.getTestRoot() + EXPECTED_ROOT_PATH_SUFFIX;
			
			System.out.println("Configuring OSSink invocation with params: ");
			Object value = null;
			for (String key:  _testConfiguration.keySet()) {
				value = _testConfiguration.get(key);
				if (value instanceof String[]) {
					System.out.println("\t " + key + "=" + Arrays.toString((String[])_testConfiguration.get(key)));
				} else {
					System.out.println("\t " + key + "=" + _testConfiguration.get(key));
				}
			}
			
			_testData = _testData.parallel(sinkParallelityLevel);

			SPLStream osSink = SPL.invokeOperator(Constants.OBJECT_STORAGE_SINK_OP_NS + "::" + Constants.OBJECT_STORAGE_SINK_OP_NAME, 
												  _testData, Constants.OS_SINK_OUT_SCHEMA, _testConfiguration);     
			
			
			validateResults(osSink, protocol, getStorageFormat(), getExpectedCount());
		} catch (Exception e) {
			System.err.println("Test failed: " + e.getMessage());
			e.printStackTrace();
			throw new Exception(e);
		}
	}
	
	
	private String getStorageFormat() {		
		if (_testConfiguration.containsKey(Constants.SINK_STORAGE_FORMAT_PARAM_NAME)) 
			return (String)_testConfiguration.get(Constants.SINK_STORAGE_FORMAT_PARAM_NAME);
		return Constants.DEFAULT_STORAGE_FORMAT;
	}

	public String getDefaultBucket() {
		return Constants.DEFAULT_BUCKET_NAME;
	}

	public String getDefaultContainer() {
		return Constants.DEFAULT_CONTAINER_NAME;
	}


	public String createTestOutput(String testName)  {
		String testOutputFolderName = TEST_OUTPUT_ROOT_FOLDER + "/" + testName + "/";
		if (OSTRawFileUtils.getInstance().folderExists(testOutputFolderName))  
			OSTRawFileUtils.getInstance().removeFolder(testOutputFolderName);		
		OSTRawFileUtils.getInstance().createFolder(testOutputFolderName);
	
		return testOutputFolderName;
	}

	
	public void runUnitest(String topologyType, int sinkParallelityLevel) throws Exception {
		getConfig().put(ContextProperties.KEEP_ARTIFACTS, false);
        getConfig().put(ContextProperties.TRACING_LEVEL, java.util.logging.Level.FINE);
        
		String testName = Constants.FILE + this.getClass().getName();		
		build(testName, TraceLevel.TRACE, topologyType, Constants.FILE, AuthenticationMode.NONE, Constants.FILE_DEFAULT_BUCKET_NAME);
		createObjectTest(Constants.FILE, sinkParallelityLevel);
	}
	
	public void runUnitest(String topologyType) throws Exception {
		runUnitest(topologyType, Constants.DEFAULT_SINK_PARALLELITY_LEVEL);
	}
	
	public void runUnitest() throws Exception {
		runUnitest(Constants.STANDALONE, Constants.DEFAULT_SINK_PARALLELITY_LEVEL);
	}
	
	public int getTestTimeout() {
		return 5;
	}

	public void checkOperatorOutputData(boolean strictMode, OSTFileUtils fileUtil, String fileExtension) throws Exception {
		String expectedPath = _expectedPath + this.getClass().getName();		
		HashMap<String, File> expected = OSTRawFileUtils.getInstance().getFilesInFolder(expectedPath, fileExtension);
		HashMap<String, File> actual = OSTRawFileUtils.getInstance().getFilesInFolder(_outputFolder, fileExtension);
		
		System.out.println("-> Expected operator output location: '" + expectedPath + "'");
		System.out.println("-> Expected output tuples count: '" + expected.size() + "'");

		System.out.println("-> Actual operator output location: '" + _outputFolder + "'");
		System.out.println("-> Actual output tuples count: '" + actual.size() + "'");
		
		// checks that number of actual files is greater 
		// than expected
		assertTrue("Actual output object count must be greater than expected. Actual = '" + 
					actual.size() + ", expected = " + expected.size(), actual.size() >= expected.size());				
		
		// checks that the file content is the same		
		for (String key: expected.keySet()) {
			assertTrue("The file '" + key + "' appears in expected, but missing in actual", actual.containsKey(key));
			
			boolean expectedEqActual = strictMode ? 
											fileUtil.contentEquals(expected.get(key), actual.get(key)):
											fileUtil.contentContains(expected.get(key), actual.get(key));
											
			// expected and actual are different - show differences
			System.out.println("-> Comparing expected '" + expected.get(key) + "' with actual '" + actual.get(key) + "'");
			if (!expectedEqActual) {
				System.out.println("-> EXPECTED AND ACTUAL ARE DIFFERENT");
				fileUtil.showFileDiffs(expected.get(key), actual.get(key));
				org.junit.Assert.fail("EXPECTED AND ACTUAL ARE DIFFERENT");
			} else {
				if (strictMode) {
					System.out.println("-> EXPECTED AND ACTUAL ARE IDENTICAL");
				} else {
					System.out.println("-> ACTUAL CONTAINS ALL EXPECTED CONTENT");
				}
			}
		}	
	}	
	

	public void checkOperatorOutputData(String storageFormat, boolean strictMode) throws Exception {
		
		if (storageFormat.equals(Constants.PARQUET_STORAGE_FORMAT)) {
			checkOperatorOutputData(strictMode, OSTParquetFileUtils.getInstance(), PARQUET_OUT_EXTENSION);			
		} else {
			checkOperatorOutputData(strictMode, OSTRawFileUtils.getInstance(), TXT_OUT_EXTENSION);
		}
														
	}

	public void checkOperatorOutTuples(Condition<Long> expectedCount, 
									   List<Tuple> expectedTuples, 
									   List<Tuple> resultTuples, 
									   String[] skipAttributes) {
		
		// check that at least one tuple returned
		if (enableExpectedCountChecking()) assertTrue(expectedCount.toString(), expectedCount.valid());
		if (expectedTuples.size() == 0) return;
		List<String> attrNames = new ArrayList<String>(expectedTuples.get(0).getStreamSchema().getAttributeNames());		
		
		attrNames.removeAll(Arrays.asList(skipAttributes));
		
		// check if actual tuples are super set of expected
		expectedTuples.removeAll(resultTuples);
		assertTrue(expectedTuples.size() + " tuples appears in expected, but not in actual", expectedTuples.size() > 0);
	}
	
	public  Tuple[] getExpectedOutputTuples() {
		// load expected files
		String expectedPath = _expectedPath + this.getClass().getName();		
		List<File> expected = OSTRawFileUtils.getInstance().getAllFilesInFolder(expectedPath);

		// generates output tuples based on expected files
		int expectedTuplesCount = expected.size();
		Tuple[] res = new Tuple[expectedTuplesCount];
		int i = 0;
		for (File entry :  expected) {
			res[i++] = Constants.OS_SINK_OUT_SCHEMA
					.getTuple(new Object[] { new RString(entry.getAbsolutePath()), new Long(0)});
			
		}

		return res;	
	}
	
	public void validateResults(SPLStream osSink, String protocol, String storageFormat, Integer expectedCnt) throws Exception {

		Tuple[] expectedTupleArr = getExpectedOutputTuples();
		int expectedTuplesCount = expectedCnt == null ? expectedTupleArr.length : expectedCnt.intValue();
		
			
		// Sink operator generates single output tuple per object
		// containing object name and size
		Condition<Long> expectedCount = _tester.atLeastTupleCount(osSink, expectedTuplesCount);
		
		Condition<List<Tuple>> expectedTuples = _tester.tupleContents(osSink, expectedTupleArr);
						
		System.out.println("About to build and execute the test job...");
		
		// build and run application
		try {
			complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);		
						
		} catch (InterruptedException ie) {
			// no need to worry about InterruptedException
			System.out.println("InterruptedException is catched and skipped");
		}
		
		System.out.println("Job build and execution completed. Starting results validation.");
		
		// check output tuples
		checkOperatorOutTuples(expectedCount, Arrays.asList(expectedTupleArr), expectedTuples.getResult(), new String[] {"size"});
					
		// check output data
		checkOperatorOutputData(storageFormat, useStrictOutputValidationMode());
	}	
	

	public boolean useStrictOutputValidationMode() {
		return true;
	}
	
	public boolean enableExpectedCountChecking() {
		return false;
	}
	
	public Integer getExpectedCount() {
		return null;
	}
}
