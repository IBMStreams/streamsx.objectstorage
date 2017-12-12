package com.ibm.streamsx.objectstorage.unitest.sink.raw;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.objectstorage.unitest.sink.BaseObjectStorageTestSink;


/**
 * Tests object rolling policy "by tuple count".
 * Sink operator input schema:  tuple<timestamp ts, rstring customerId, float64 latitude, float64 longitude>
 *  1. output object tuple count: 1000 records
 *  2. storage format: raw
 * 
 * @author streamsadmin
 *
 */

public class TestCloseByTupleCountComplexInSchema extends BaseObjectStorageTestSink {
	
	public String getInjectionOutSchema() {
		return "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>"; 

	}	

	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String objectName = _outputFolder + _protocol + getClass().getSimpleName() + "%OBJECTNUM." + TXT_OUT_EXTENSION; 
		params.put("objectName", objectName);
		params.put("tuplesPerObject", 100L); // 100 lines per object
		params.put("dataAttribute", _testData.getSchema().getAttribute("longitude"));
	}
	
	@Test
	public void testCloseByTupleCountComplexInSchema() throws Exception {
        runUnitest();
	}
}
