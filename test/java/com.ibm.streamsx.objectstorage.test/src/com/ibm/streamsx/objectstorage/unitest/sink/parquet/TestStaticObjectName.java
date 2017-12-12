package com.ibm.streamsx.objectstorage.unitest.sink.parquet;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.objectstorage.unitest.sink.BaseObjectStorageTestSink;

/**
 * Tests object rolling policy "by object size".
 * Sink operator input schema:  tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>
 * Sink operator parameterization:
 *  1. output object size: 3K
 *  2. output object data attribute: longitude
 *  3. storage format: raw
 * 
 * @author streamsadmin
 *
 */
public class TestStaticObjectName extends BaseObjectStorageTestSink {

	private static final Integer EXPECTED_COUNT = 4;
	
	public String getInjectionOutSchema() {
		return "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>"; 

	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String objectName = _outputFolder + _protocol + this.getClass().getSimpleName() + "." + TXT_OUT_EXTENSION; 
		params.put("objectName", objectName);
		params.put("dataAttribute", _testData.getSchema().getAttribute("longitude"));
		params.put("tuplesPerObject", 500L ); 
	}
	
	
	@Test
	public void testCloseBySizeComplexInSchema() throws Exception {        
        runUnitest();
	}	
	
	public Integer getExpectedCount() {
		return EXPECTED_COUNT;
	}
	
	/**
	 * Check output files CONTAINMENT only,
	 * i.e. make sure that expected is a superset of actual.
	 */
	public boolean useStrictOutputValidationMode() {
		return false;
	}
}
