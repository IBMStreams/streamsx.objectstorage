package com.ibm.streamsx.objectstorage.unitest.sink.parquet.partitioning;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.unitest.sink.BaseObjectStorageTestSink;

/**
 * Add partition automatically after prefix and before object name
 * with prefix of depth 2 
 * Close by Size
 * Partition of Depth 1
 * Automatically creates empty partition for missing partition attribute values
 * 
 * @author streamsadmin
 *
 */
public class TestEmptyPartition extends BaseObjectStorageTestSink {

	public String getInjectionOutSchema() {
		return "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude>";

	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String objectName = _outputFolder + "prefix1/prefix2/" + _protocol + this.getClass().getSimpleName() + "%OBJECTNUM." + PARQUET_OUT_EXTENSION; 

		params.put("objectName", objectName);
		params.put("storageFormat", Constants.PARQUET_STORAGE_FORMAT);
		params.put("parquetEnableDict", true);
		params.put("bytesPerObject", 1024L); 
		params.put("partitionValueAttributes", new String[] {"customerId"});
		
	}

	@Test
	public void testEmptyPartition() throws Exception {
		runUnitest();
	}
	
	public void initTestData() throws Exception {
		super.initTestData(DEFAULT_TUPLE_RATE, true);
	}

}