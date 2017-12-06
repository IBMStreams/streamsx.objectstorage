package com.ibm.streamsx.objectstorage.unitest.sink;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.test.AuthenticationMode;
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

	

	@Override
	public void initTestData() throws Exception {
		_testData = Utils.getEndlessStringStream(_testTopology, 100);		
	}
	
	@Test
	public void testLocalBasicAuthSchema() throws Exception {
		String testName = Constants.FILE + TestCloseByTimeSimpleInSchema.class.getName();
		build(testName, TraceLevel.TRACE, Constants.STANDALONE, Constants.FILE, AuthenticationMode.BASIC, Constants.FILE_DEFAULT_BUCKET_NAME);
		createObjectTest(Constants.FILE);
	}

	
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) {
		params.put("objectName", _protocol +  TestCloseByTimeSimpleInSchema.class.getSimpleName());
		params.put("headerRow", "id,tz,dateutc,latitude,longitude,temperature,baromin,humidity,rainin,time_stamp");
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
