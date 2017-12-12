package com.ibm.streamsx.objectstorage.unitest.sink;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.After;

import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.objectstorage.test.Constants;
import com.ibm.streamsx.objectstorage.test.StreamsRestConnector;
import com.ibm.streamsx.rest.Metric;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Condition;

public abstract class BaseObjectStorageTestWithMetricsSink extends BaseObjectStorageTestSink {
	
	private static final int DEFAULT_METRICS_COLLECTION_TIMEOUT_SECS = 10;
	private StreamsRestConnector fRestConnector = new StreamsRestConnector();
	
	public abstract void checkOperatorMetrics(List<Metric> metrics);
	
	@Override
	public void validateResults(SPLStream osSink, String protocol, String storageFormat) throws Exception {

		Tuple[] expectedTupleArr = getExpectedOutputTuples();
		int expectedTuplesCount = expectedTupleArr.length;
		
			
		// Sink operator generates single output tuple per object
		// containing object name and size
		Condition<Long> expectedCount = _tester.atLeastTupleCount(osSink, expectedTuplesCount);
		
		Condition<List<Tuple>> expectedTuples = _tester.tupleContents(osSink, expectedTupleArr);
						
		System.out.println("About to build and execute the test job...");
		
		List<Metric> metrics = null;
		// build and run application
		try {
			//complete(_tester, expectedCount, getTestTimeout(), TimeUnit.SECONDS);
			if (getTesterType().equals(StreamsContext.Type.DISTRIBUTED_TESTER)) {
				fRestConnector.submitJob(_testTopology, getTesterType());
				metrics = fRestConnector.getMetrics(Constants.OBJECT_STORAGE_SINK_OP_NS + "::" + Constants.OBJECT_STORAGE_SINK_OP_NAME,
									                DEFAULT_METRICS_COLLECTION_TIMEOUT_SECS);	
	        } else {
	            fail("This test should be skipped");
	        }						
		} catch (InterruptedException ie) {
			// no need to worry about InterruptedException
			System.out.println("InterruptedException is catched and skipped");
		}
		
		System.out.println("Job build and execution completed. Starting results validation.");
		
		// check output tuples
		checkOperatorOutTuples(expectedCount, Arrays.asList(expectedTupleArr), expectedTuples.getResult(), new String[] {"size"});
					
		// check output data
		//checkOperatorOutputData(storageFormat, useStrictOutputValidationMode());
		
		// check operator metrics
		checkOperatorMetrics(metrics);
	}	
	
	@After
	public void removeJob() throws Exception {
		fRestConnector.removeJob();
	}

}
