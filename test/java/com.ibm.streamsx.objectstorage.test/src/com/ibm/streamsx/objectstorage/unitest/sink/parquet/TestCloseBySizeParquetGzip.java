package com.ibm.streamsx.objectstorage.unitest.sink.parquet;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.unitest.sink.BaseObjectStorageTestSink;

/**
 * Tests object rolling policy "by object size". Sink operator input schema:
 * tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude,
 * timestamp ts> Sink operator parameterization: 1. output object size: 10K 2.
 * storage format: parquet 3. parquet compression: SNAPPY
 * 
 * @author streamsadmin
 *
 */
public class TestCloseBySizeParquetGzip extends BaseObjectStorageTestSink {

	public String getInjectionOutSchema() {
		return "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>";

	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String objectName = _outputFolder + _protocol + this.getClass().getSimpleName() + "%OBJECTNUM." + PARQUET_OUT_EXTENSION; 

		params.put("objectName", objectName);
		params.put("storageFormat", Constants.PARQUET_STORAGE_FORMAT);
		params.put("bytesPerObject", 10L * 1024L); // 10K per object
		params.put("parquetCompression", "GZIP");
		params.put("parquetWriterVersion", "v1");

	}

	@Test
	public void testCloseBySizeParquetConfigGzip() throws Exception {
		//runUnitest();
		runUnitest(Constants.DISTRIBUTED);
	}

}