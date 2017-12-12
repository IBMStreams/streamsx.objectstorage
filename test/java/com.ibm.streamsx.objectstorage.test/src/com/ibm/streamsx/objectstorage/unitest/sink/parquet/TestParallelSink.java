package com.ibm.streamsx.objectstorage.unitest.sink.parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.unitest.sink.BaseObjectStorageTestWithMetricsSink;
import com.ibm.streamsx.rest.Metric;

/**
 * Tests object rolling policy "by object size". Sink operator input schema:
 * tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude,
 * timestamp ts> Sink operator parameterization: 1. output object size: 10K 2.
 * storage format: parquet 3. parquet compression: SNAPPY
 * 
 * @author streamsadmin
 *
 */
public class TestParallelSink extends BaseObjectStorageTestWithMetricsSink {

	public String getInjectionOutSchema() {
		return "tuple<rstring tsStr, rstring customerId, float64 latitude, float64 longitude, timestamp ts>";
	}
	
	
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
		runUnitest(Constants.DISTRIBUTED, 3);
	}

    
	@Override
	public void checkOperatorMetrics(List<Metric> metrics) {
		for (Metric  m: metrics) {
			System.out.println("Checking metric '" + m.getName() + "' with value '" + m.getValue() + "'");
			assertTrue("Metrics kind should be custom or system. Received '" +  m.getMetricKind() + "'",
					   m.getMetricKind().equals("unknown") || m.getMetricKind().equals("system") || m.getMetricKind().equals("counter"));
            assertEquals("metric", m.getResourceType());
            assertNotNull("Metric with empty name detected", m.getName());
            assertTrue(m.getLastTimeRetrieved() > 0);
            switch (m.getName()) {
            case "nActiveObjects": 
            	assertEquals(m.getValue() , 0);
            	break;
            case "nClosedObjects":
            	assertEquals(m.getValue() , 4);
            	break;
            case "nEvictedObjects":
            	assertEquals(m.getValue() , 0);
            	break;
            case "nExpiredObjects":
            	assertEquals(m.getValue() , 3);
            	break;
            case "nTuplesProcessed":
            	assertTrue(m.getValue() >= 666);
            	break;
            case "nTuplesSubmitted":
            	assertTrue(m.getValue() >= 0);
            	break;
            case "startupTimeMillisecs":
            	assertTrue(m.getValue() < 10000);
            	break;
            default:
            	fail("Unknown metric '" + m.getName() + "'");
            }
		}
		
	}

}