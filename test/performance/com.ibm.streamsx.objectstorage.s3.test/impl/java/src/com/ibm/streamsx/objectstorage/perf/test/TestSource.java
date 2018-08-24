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
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.samples.patterns.ProcessTupleProducer;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import java.util.Random;

@PrimitiveOperator(description="Tuple generator operator.", comment="")
@OutputPortSet(cardinality=2)
@Libraries({"impl/lib/samples.jar"})
public class TestSource extends ProcessTupleProducer {

    private long numTuples = 50000000;
    private long sentTuples = 0;
    private int tupleSize = 1024;

    @Parameter(name="numTuples", description="Number of tuples to be sent", optional=true)
    public void setNumTuples(long numTuples) {
        this.numTuples = numTuples;
    }

    @Parameter(name="tupleSize", description="Tuple size to be generated", optional=true)
    public void setTupleSize(int tupleSize) {
        this.tupleSize = tupleSize;
    }
    
    @Override
    public synchronized void initialize(OperatorContext context) throws Exception {
        super.initialize(context);
    }

    @Override
    public void process() throws Exception {
        
        final StreamingOutput<OutputTuple> output = getOutput(0);

        //StringBuffer buf = new StringBuffer(tupleSize);
        //for (int i = 0; i < tupleSize; i++){
        //    buf.append("a");
        //}
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = tupleSize;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int) 
              (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        } 

        final StreamingOutput<OutputTuple> output1 = getOutput(1);
        output1.punctuate(Punctuation.WINDOW_MARKER);

        while (true) {
            final OutputTuple tuple = output.newTuple();
            //tuple.setString(0, buf.toString()); // expects string attribute as first output stream attribute 
            tuple.setString(0, buffer.toString());
            
            output.submit(tuple);
            sentTuples++;
            if (sentTuples == numTuples) {
                break;
            }
        }
        while (true) {
        	try {
        	    Thread.sleep(1000);
        	} 
        	catch(InterruptedException ex) {
        	    Thread.currentThread().interrupt();
        	    break;
        	}
        }
    }

}
