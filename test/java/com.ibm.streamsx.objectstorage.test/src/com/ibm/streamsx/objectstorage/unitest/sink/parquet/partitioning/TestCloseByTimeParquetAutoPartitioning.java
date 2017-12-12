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
public class TestCloseByTimeParquetAutoPartitioning extends BaseObjectStorageTestSink {

	private static final int TIME_PER_OBJECT_SECS = 10;
	
	public String getInjectionOutSchema() {
		return "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>";

	}
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String objectName = _outputFolder + "prefix1/prefix2/" + _protocol + this.getClass().getSimpleName() + "%OBJECTNUM." + PARQUET_OUT_EXTENSION; 

		params.put("objectName", objectName);
		params.put("storageFormat", Constants.PARQUET_STORAGE_FORMAT);
		params.put("parquetEnableDict", true);
		params.put("timePerObject", (double)TIME_PER_OBJECT_SECS); 
		params.put("partitionValueAttributes", new String[] {"customerId"});
		
	}

	@Test
	public void testCloseByTimeParquetAutoPartioning() throws Exception {
		//runUnitest(Constants.DISTRIBUTED);
		runUnitest();
	}
	
	public int getTestTimeout() {
		return TIME_PER_OBJECT_SECS + SHUTDOWN_DELAY;
	}

}