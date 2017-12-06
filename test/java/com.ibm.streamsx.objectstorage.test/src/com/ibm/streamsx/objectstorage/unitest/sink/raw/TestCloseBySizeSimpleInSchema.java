package com.ibm.streamsx.objectstorage.unitest.sink.raw;

import java.util.Map;

import org.junit.Test;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.unitest.sink.TestObjectStorageBaseSink;


/**
 * Tests object rolling policy "by object size" with header.
 * Sink operator input schema:  tuple<rstring line>
 * Sink operator parameterization:
 *  1. output object size: 50K
 *  2. storage format: raw
 *  3. header row: "HEADER"
 * 
 * @author streamsadmin
 *
 */
public class TestCloseBySizeSimpleInSchema extends TestObjectStorageBaseSink {
	
	public String getInjectionOutSchema() {
		return "tuple<rstring line>"; 

	}

	@Override
	public void genTestSpecificParams(Map<String, Object> params) {
		String objectName = _outputFolder + _protocol + getClass().getSimpleName() + "%OBJECTNUM." + TXT_OUT_EXTENSION; 
		params.put("objectName", objectName);
		params.put("bytesPerObject", 3L * 1024L);
		params.put("headerRow", "HEADER");
	}
	
	@Test
	public void testCloseBySizeSimpleInSchema() throws Exception {		
        runUnitest();
	}
}
