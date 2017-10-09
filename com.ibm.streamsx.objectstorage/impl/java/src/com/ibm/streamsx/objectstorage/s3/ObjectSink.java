//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.objectstorage.s3;


import java.io.ByteArrayInputStream;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * <P>
 * The following event methods from the Operator interface can be called:
 * </p>
 * <ul>
 * <li><code>initialize()</code> to perform operator initialization</li>
 * <li>allPortsReady() notification indicates the operator's ports are ready to process and submit tuples</li> 
 * <li>process() handles a tuple arriving on an input port 
 * <li>processPuncuation() handles a punctuation mark arriving on an input port 
 * <li>shutdown() to shutdown the operator. A shutdown request may occur at any time, 
 * such as a request to stop a PE or cancel a job. 
 * Thus the shutdown() may occur while the operator is processing tuples, punctuation marks, 
 * or even during port ready notification.</li>
 * </ul>
 * <p>With the exception of operator initialization, all the other events may occur concurrently with each other, 
 * which lead to these methods being called concurrently by different threads.</p> 
 */
@Libraries("opt/downloaded/*")
@PrimitiveOperator(name="ObjectSink", namespace="com.ibm.streamsx.objectstorage.s3",
description="The Java Operator ObjectSink uses the S3 API interface to store objects in IBM’s Cloud Object Storage System.")
@InputPorts({@InputPortSet(id="0", description="Port that ingests tuples containing the data to be put to the object storage.", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Port that sends the tuples after the put operation. All attributes matching with the input port 0 attributes are forwarded.", cardinality=1, optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving, windowPunctuationInputPort="0")})
public class ObjectSink extends AbstractOperator {
    
    private String accessKeyID;
    private String secretAccessKey;
    private String endpoint;
    private String bucket; // Bucket name should be between 3 and 63 characters long
    
    private TupleAttribute<Tuple,String> objectNameAttribute;
    private TupleAttribute<Tuple,String> objectDataAttribute;
    
    private String errorCodeAttribute = null;
    
    private boolean preservePunctuation = false;
    
    private AmazonS3 client;
    
    private boolean hasOutputPort = false;

    /**
     * Logger for tracing.
     */
    private static Logger _trace = Logger.getLogger(ObjectSink.class.getName());
    
    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        // Must call super.initialize(context) to correctly setup an operator.
        super.initialize(context);
        _trace.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());

        // check if optional output port is present
        if (context.getNumberOfStreamingOutputs() > 0) {
            hasOutputPort = true;
        };

        int timeout = 15 * 60 * 1000;
        // initialize S3 client
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setConnectionTimeout(timeout);
        clientConf.setSocketTimeout(timeout);
//        clientConf.withUseExpectContinue(false);
//        clientConf.withSignerOverride("S3SignerType");
        clientConf.setProtocol(Protocol.HTTP);
        
        AWSCredentials creds = new BasicAWSCredentials(getAccessKeyID(), getSecretAccessKey());
        client = new AmazonS3Client(creds, clientConf);        
        client.setEndpoint(endpoint);
//        client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(pathStyleAccess));
        
        _trace.trace("Operator " + context.getName() + " S3 client has been initialized" + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
        
    }

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
        // This method is commonly used by source operators. 
        // Operators that process incoming tuples generally do not need this notification. 
        OperatorContext context = getOperatorContext();
        _trace.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    }

    /**
     * Process an incoming tuple that arrived on the specified port.
     * @param stream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {

    	String errorCode = "";
        String keyName = objectNameAttribute.getValue(tuple);
        try {
        	byte[]                  contentAsBytes = objectDataAttribute.getValue(tuple).getBytes("UTF-8");
            ByteArrayInputStream    contentsAsStream      = new ByteArrayInputStream(contentAsBytes);
            ObjectMetadata          md = new ObjectMetadata();
            md.setContentLength(contentAsBytes.length);
            client.putObject(new PutObjectRequest(getBucket(), keyName, contentsAsStream, md));
        } catch (AmazonServiceException ase) {
            String errMessage = "Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to S3, but was rejected with an error response" +
                    " for some reason.\n";            
            errMessage+="Error Message:    " + ase.getMessage()+"\n";
            errMessage+="HTTP Status Code: " + ase.getStatusCode()+"\n";
            errMessage+="AWS Error Code:   " + ase.getErrorCode()+"\n";
            errMessage+="Error Type:       " + ase.getErrorType()+"\n";
            errMessage+="Request ID:       " + ase.getRequestId();
            _trace.error(errMessage);
            if ((hasOutputPort) && (null != errorCodeAttribute)) {
            	errorCode = ase.getErrorCode();
            }
        }
        // do not catch AmazonClientException
        
        // emit objectName tuple if optional output port is present
        if (hasOutputPort) {
            StreamingOutput<OutputTuple> outStream = getOutput(0);
            OutputTuple outTuple = outStream.newTuple();
    		// Copy across all matching attributes.
    		outTuple.assign(tuple);
    		if (null != errorCodeAttribute) {
    			outTuple.setString(errorCodeAttribute, errorCode);
    		}
            outStream.submit(outTuple);
        }
    }
    
    /**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream,
            Punctuation mark) throws Exception {
    	if (mark == Punctuation.WINDOW_MARKER) {
    		if (preservePunctuation) {
    			super.processPunctuation(stream, mark);
    		}
    	}
    	else {
    		super.processPunctuation(stream, mark);
    	}
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        // TODO: If needed, close connections or release resources related to any external system or data store.

        // Must call super.shutdown()
        super.shutdown();
    }
    
    // Mandatory parameter accessKeyID mapping to the user's S3 Access Key ID
    @Parameter(name="accessKeyID", description="Object Storage access key ID", optional=false)
    public void setAccessKeyID(String accessKeyID) {
        this.accessKeyID = accessKeyID;
    }
    public String getAccessKeyID() {
        return accessKeyID;
    }
    
    // Mandatory parameter secretAccessKey mapping to the user's S3 Secret Access Key
    @Parameter(name="secretAccessKey", description="Object Storage secret access key", optional=false)
    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }
    public String getSecretAccessKey() {
        return secretAccessKey;
    }
    
    // Mandatory parameter endpoint mapping to the user's S3 endpoint
    @Parameter(name="endpoint", description="Object Storage endpoint URL", optional=false)
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }    
    public String getEndpoint() {
        return endpoint;
    }
    
    @Parameter(name="bucket", description="Object Storage bucket name" , optional=false)
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
    public String getBucket() {
        return bucket;
    }
   
	@Parameter(name="preservePunctuation", description="If set to true then the operator forwards punctuation from input port 0 to output port 0. The default value is false.", optional=true)
	public void setPreservePunctuation(boolean preservePunctuation) {
		this.preservePunctuation = preservePunctuation;
	}
    
	@Parameter(name="objectNameAttribute", description="This parameter specifies the attribute of the input tuple that contains the object name.", optional=false)
	public void setObjectNameAttribute(TupleAttribute<Tuple,String> objectNameAttribute) {
		this.objectNameAttribute = objectNameAttribute;
	}
	
	@Parameter(name="objectDataAttribute", description="This parameter specifies the attribute of the input tuple that contains the object content.", optional=false)
	public void setObjectDataAttribute(TupleAttribute<Tuple,String> objectDataAttribute) {
		this.objectDataAttribute = objectDataAttribute;
	}
	
	@Parameter(name="errorCodeAttribute", description="This parameter specifies the attribute name in the output tuple for error codes returned by the S3 client.", optional=true)
	public void setErrorCodeAttribute(String errorCodeAttribute) {
		this.errorCodeAttribute = errorCodeAttribute;
	}	
}
