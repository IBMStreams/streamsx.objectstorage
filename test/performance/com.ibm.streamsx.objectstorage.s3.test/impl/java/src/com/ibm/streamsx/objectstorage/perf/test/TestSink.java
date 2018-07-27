/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* THIS SAMPLE CODE IS PROVIDED ON AN "AS IS" BASIS. IBM MAKES NO   */
/* REPRESENTATIONS OR WARRANTIES, EXPRESS OR IMPLIED, CONCERNING    */
/* USE OF THE SAMPLE CODE, OR THE COMPLETENESS OR ACCURACY OF THE   */
/* SAMPLE CODE. IBM DOES NOT WARRANT UNINTERRUPTED OR ERROR-FREE    */
/* OPERATION OF THIS SAMPLE CODE. IBM IS NOT RESPONSIBLE FOR THE    */
/* RESULTS OBTAINED FROM THE USE OF THE SAMPLE CODE OR ANY PORTION  */
/* OF THIS SAMPLE CODE.                                             */
/*                                                                  */
/* LIMITATION OF LIABILITY. IN NO EVENT WILL IBM BE LIABLE TO ANY   */
/* PARTY FOR ANY DIRECT, INDIRECT, SPECIAL OR OTHER CONSEQUENTIAL   */
/* DAMAGES FOR ANY USE OF THIS SAMPLE CODE, THE USE OF CODE FROM    */
/* THIS [ SAMPLE PACKAGE,] INCLUDING, WITHOUT LIMITATION, ANY LOST  */
/* PROFITS, BUSINESS INTERRUPTION, LOSS OF PROGRAMS OR OTHER DATA   */
/* ON YOUR INFORMATION HANDLING SYSTEM OR OTHERWISE.                */
/*                                                                  */
/* (C) Copyright IBM Corp. 2018  All Rights reserved.         */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
package com.ibm.streamsx.objectstorage.perf.test;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.types.Timestamp;
import java.util.logging.Logger;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;


@PrimitiveOperator(description="Operator counts the number of bytes uploaded to COS and measures the upload rate (bytes/s).", comment="")
@InputPortSet(cardinality=2)
@OutputPortSet(cardinality=1)
public class TestSink extends AbstractOperator {

    private static final String CLASS_NAME = TestSink.class.getName();

    /**
     * Create a {@code Logger} specific to this class that will write
     * to the SPL trace facility as a child of the root {@code Logger}.
     */
    private final Logger trace = Logger.getLogger(CLASS_NAME);
    
    /**
     * Create a {@code Logger} specific to this class that will write
     * to the SPL log facility as a child of the {@link LoggerNames#LOG_FACILITY}
     * {@code Logger}.
     * The {@code Logger} uses a resource bundle.
     */
    private final Logger log =
        Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME);

    private long numTuples = 10;
    private long receivedTuples = 0;
    private String testName = "";
    private int tuplesPerSec = 0;
    private int tupleSize = 0;
    private int numPuncts = 0;
    private long startTime = 0;
    private long numBytes = 0;
    private long dataSize = 0;

    @Parameter(name="testName", description="Used for test result log entry only", optional=true)
    public void setTestName(String testName) {
        this.testName = testName;
    }

    @Parameter(name="numTuples", description="Number of tuples to be received before stopping the time measurement.", optional=true)
    public void setNumTuples(long numTuples) {
        this.numTuples = numTuples;
    }
    
    @Parameter(name="dataSize", description="Size of test data sent to COS Writer. Parquet format can result in smaller objects, than input data.", optional=true)
    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }    
    
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
    }

    @Override
    public void process(StreamingInput<Tuple> port, Tuple tuple) throws Exception {
    	if (port.getPortNumber() == 0) {
    		receivedTuples++;
    		numBytes += tuple.getLong(1);
    		if (numTuples == receivedTuples) {
    			long elapsedTime = System.currentTimeMillis() - startTime;
    			trace.log(TraceLevel.ERROR, "STOP " + numBytes);
    			float elapsedTimeSec = elapsedTime/1000;
    			System.out.println("{'object_storage_test': '"+testName+"', 'num_objects': "+receivedTuples+", 'num_bytes': "+numBytes+", 'data_sent_KB_per_sec': "+(numBytes/elapsedTime)+", 'duration_sec': "+elapsedTimeSec+"}");
    		}
    		if (((receivedTuples % 10) == 0) || (receivedTuples == 1)) {    		
    			final StreamingOutput<OutputTuple> output = getOutput(0);
    			final OutputTuple otuple = output.newTuple();
    			otuple.setString(0, "ok"); // result tuple for the topology tester
    			output.submit(otuple);
    		}
    	}
    }

    @Override
    public void processPunctuation(StreamingInput<Tuple> stream, Punctuation punct) throws Exception {
        if (punct == Punctuation.WINDOW_MARKER) {
        	if (stream.getPortNumber() == 1) {
                trace.log(TraceLevel.ERROR, "START");
                startTime = System.currentTimeMillis();

    			final StreamingOutput<OutputTuple> output = getOutput(0);
    			final OutputTuple otuple = output.newTuple();
    			otuple.setString(0, "ok"); // result tuple for the topology tester
    			output.submit(otuple);                
        	}
        }
        super.processPunctuation(stream, punct);
    }
}
