package com.ibm.streamsx.objectstorage.unitest.sink.raw;

import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.objectstorage.unitest.sink.BaseObjectStorageTestSink;


/**
 * Tests object rolling policy "by tuple count".
 * Sink operator input schema:  tuple<timestamp line>
 *  1. output object tuple count: 1000 records
 *  2. storage format: parquet
 * 
 * @author streamsadmin
 *
 */
public class TestCloseByTupleCountSimpleInSchema extends BaseObjectStorageTestSink {

	public String getInjectionOutSchema() {
		return "tuple<rstring line>"; 

	}
	
	@Test
	public void testCloseByTupleCountSimpleInSchema() throws Exception {
        runUnitest();
	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) {
		String objectName = _outputFolder + _protocol + getClass().getSimpleName() + "%OBJECTNUM." + TXT_OUT_EXTENSION; 

		params.put("objectName", objectName);
		params.put("tuplesPerObject", 500L);
	}
}
