package com.ibm.streamsx.objectstorage.unitest.sink;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Condition;

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
public class TestCloseBySizeComplexInSchema extends TestObjectStorageBaseSink {

	private static final String OUT_EXTENSION = "txt";
	
	@Override
	public void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException {
		String objectName = _outputFolder + _protocol + TestCloseBySizeComplexInSchema.class.getSimpleName() + "%OBJECTNUM." + OUT_EXTENSION; 
		params.put("objectName", objectName);
		params.put("dataAttribute", _testData.getSchema().getAttribute("longitude"));
		params.put("bytesPerObject", 3L * 1024L); // 10K per object
	}
	
	@Test
	public void testCloseBySizeComplexInSchema() throws Exception {
        getConfig().put(ContextProperties.KEEP_ARTIFACTS, true);
        getConfig().put(ContextProperties.TRACING_LEVEL, java.util.logging.Level.FINE);

        runUnitest(this.getClass());
	}	

	public void validateResults(SPLStream osSink, String protocol) throws Exception {
		// Sink operator generates single output tuple per object
		// containing object name and size
		Condition<Long> expectedCount = _tester.atLeastTupleCount(osSink, 2);

		List<Tuple> expectedTupleList = new ArrayList<Tuple>();
		Tuple expectedTupleObj1 = Constants.OS_SINK_OUT_SCHEMA
				.getTuple(new Object[] { new RString(((String) _testConfiguration.get("objectName")).replace("%OBJECTNUM", "0")), new Long(3072)});		
		Tuple expectedTupleObj2 = Constants.OS_SINK_OUT_SCHEMA
				.getTuple(new Object[] { new RString(((String) _testConfiguration.get("objectName")).replace("%OBJECTNUM", "1")), new Long(3073)});
		expectedTupleList.add(expectedTupleObj1);
		expectedTupleList.add(expectedTupleObj2);

		Condition<List<Tuple>> expectedTuples = _tester.tupleContents(osSink, expectedTupleObj1, expectedTupleObj2);
		
		// build and run application
		complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);		
		
		System.out.println("Job build and execution completed. Starting results validation.");
		
		// check output tuples
		checkOperatorOutTuples(expectedCount, expectedTupleList, expectedTuples.getResult(), new String[] {"size"});
			
		// check output data
		checkOperatorOutputData(OUT_EXTENSION);
	}
}
