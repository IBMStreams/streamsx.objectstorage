package com.ibm.streamsx.objectstorage.unitest.sink.parquet.partitioning;

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
public class TestCloseBySizeParquetPartitionVar extends BaseObjectStorageTestSink {

	public String getInjectionOutSchema() {
		return "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude>";

	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String objectName = _outputFolder + "prefix/%PARTITIONS" + "suffix/" + _protocol + this.getClass().getSimpleName() + "%OBJECTNUM." + PARQUET_OUT_EXTENSION; 

		params.put("objectName", objectName);
		params.put("storageFormat", Constants.PARQUET_STORAGE_FORMAT);
		params.put("parquetEnableDict", true);
		params.put("bytesPerObject", 1024L); 
		params.put("partitionValueAttributes", new String[] {"customerId"});
	}


	@Test
	public void testCloseBySizeParquetPartitionVar() throws Exception {
		runUnitest();
	}

}