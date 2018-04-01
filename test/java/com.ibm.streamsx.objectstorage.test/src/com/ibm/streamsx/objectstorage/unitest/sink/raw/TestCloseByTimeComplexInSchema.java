package com.ibm.streamsx.objectstorage.unitest.sink.raw;

import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.objectstorage.unitest.sink.BaseObjectStorageTestSink;


/**
 * Tests object rolling policy "by time".
 * Sink operator input schema:  tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>
 * Sink operator parameterization:
 *  1. output object close time: 
 *  2. output object data attribute: longitude
 *  3. storage format: raw
 * 
 * @author streamsadmin
 *
 */
public class TestCloseByTimeComplexInSchema extends BaseObjectStorageTestSink {
	
	private static final double TIME_PER_OBJECT = 10.0;
	
	public String getInjectionOutSchema() {
		return "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>"; 

	}	
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) {
		String objectName = _outputFolder + _protocol + this.getClass().getSimpleName() + "%OBJECTNUM." + TXT_OUT_EXTENSION; 

		params.put("objectName", objectName);
		params.put("timePerObject", TIME_PER_OBJECT);		
		params.put("headerRow", "HEADER");
		params.put("dataAttribute", _testData.getSchema().getAttribute("longitude"));
	}

	@Test
	public void testCloseTimeComplexInSchema() throws Exception {
        runUnitest();
	}
	

	/**
	 * Check output files CONTAINMENT only,
	 * i.e. make sure that expected is a superset of actual.
	 */
	public boolean useStrictOutputValidationMode() {
		return false;
	}
	
	@Override
	public void initTestData() throws Exception {
		super.initTestData(100, false);
	}
}
