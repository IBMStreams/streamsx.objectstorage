//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.objectstorage.s3;


import java.io.ByteArrayInputStream;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
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
@PrimitiveOperator(name="S3Util", namespace="com.ibm.streamsx.objectstorage.s3",
description="The Java Operator S3Util uses the S3 API interface to create/delete buckets or delete objects in IBM’s Cloud Object Storage System. It expects a command attribute of type rstring and supports the following commands: CREATE_BUCKET, DELETE_ALL_OBJECTS.")
@InputPorts({@InputPortSet(id="0", description="Port that ingests the command tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
//@OutputPorts({@OutputPortSet(description="Port that sends the objectName tuples", cardinality=1, optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving, windowPunctuationInputPort="0")})
public class S3Util extends AbstractOperator {
    
    private String accessKeyID;
    private String secretAccessKey;
    private String endpoint;
    private String bucket; // Bucket name should be between 3 and 63 characters long
    
    private AmazonS3 client;
    
    private boolean hasOutputPort = false;

    /**
     * Logger for tracing.
     */
    private static Logger _trace = Logger.getLogger(S3Util.class.getName());
    
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

        String command = tuple.getString("command");

        try {
        	if (command.equals("CREATE_BUCKET")) {
            	List<Bucket> Buckets = client.listBuckets(); // get a list of buckets
            	boolean isFound = false;
            	for (Bucket b : Buckets) { // for each bucket...
            		_trace.trace("Found bucket: " + b.getName());
            		//System.out.println("Found bucket: " + b.getName());
            		if (getBucket().equals(b.getName())) {
            			isFound = true;
            		}
            	}
            	if (!isFound) {
            		client.createBucket(getBucket(), "us-standard");
            	}
        	}
        	else if (command.equals("DELETE_ALL_OBJECTS")) {
            	ObjectListing listing = client.listObjects(getBucket()); // get the list of objects in the 'sample' bucket
            	
            	List<S3ObjectSummary> summaries = listing.getObjectSummaries(); // create a list of object summaries

            	for (S3ObjectSummary obj : summaries){ // for each object...
					_trace.trace("Delete object: " + obj.getKey());
					//System.out.println("Delete object: " + obj.getKey());
					client.deleteObject(getBucket(), obj.getKey());
            	}        		
        	}
        	
        } catch (AmazonServiceException ase) {
            String errMessage = "Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.\n";            
            errMessage+="Error Message:    " + ase.getMessage()+"\n";
            errMessage+="HTTP Status Code: " + ase.getStatusCode()+"\n";
            errMessage+="AWS Error Code:   " + ase.getErrorCode()+"\n";
            errMessage+="Error Type:       " + ase.getErrorType()+"\n";
            errMessage+="Request ID:       " + ase.getRequestId();
            _trace.error(errMessage);
        } catch (AmazonClientException ace) {
            String errMessage = "Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.\n";
            errMessage+="Error Message: " + ace.getMessage();
            _trace.error(errMessage);
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
        //super.processPunctuation(stream, mark);
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
    @Parameter(name="accessKeyID", optional=false)
    public void setAccessKeyID(String accessKeyID) {
        this.accessKeyID = accessKeyID;
    }
    public String getAccessKeyID() {
        return accessKeyID;
    }
    
    // Mandatory parameter secretAccessKey mapping to the user's S3 Secret Access Key
    @Parameter(name="secretAccessKey", optional=false)
    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }
    public String getSecretAccessKey() {
        return secretAccessKey;
    }
    
    // Mandatory parameter endpoint mapping to the user's S3 endpoint
    @Parameter(name="endpoint", optional=false)
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }    
    public String getEndpoint() {
        return endpoint;
    }
    
    // Mandatory parameter endpoint mapping to the user's S3 endpoint
    @Parameter(name="bucket", optional=false)
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
    public String getBucket() {
        return bucket;
    }
    
}
