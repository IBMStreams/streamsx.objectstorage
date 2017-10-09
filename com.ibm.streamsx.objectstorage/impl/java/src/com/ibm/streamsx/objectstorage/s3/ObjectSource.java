//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.objectstorage.s3;



import java.io.ByteArrayOutputStream;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
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
@PrimitiveOperator(name="ObjectSource", namespace="com.ibm.streamsx.objectstorage.s3",
description="The Java Operator ObjectSource uses the S3 API interface to read objects from IBM’s Cloud Object Storage System.")
@InputPorts({@InputPortSet(id="0", description="Port that ingests tuples with the object name to get objects from object storage. The object name attribute is specified with the objectNameAttribute parameter.", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Port that sends the data tuples. The output stream needs an attribute with the name given by the objectDataAttribute parameter. All attributes matching with the input port 0 attributes are forwarded.", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving, windowPunctuationInputPort="0")})
public class ObjectSource extends AbstractOperator {
    
    private String accessKeyID;
    private String secretAccessKey;
    private String endpoint;
    private String bucket;
    
    private TupleAttribute<Tuple,String> objectNameAttribute;
    private String objectDataAttribute = null;
    private String errorCodeAttribute = null;
    
    private boolean preservePunctuation = false;
    
    private AmazonS3 client;
    
    /**
     * Logger for tracing.
     */
    private static Logger _trace = Logger.getLogger(ObjectSource.class.getName());
    
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
        StreamingOutput<OutputTuple> outStream = getOutput(0);
        OutputTuple outTuple = outStream.newTuple();

        String keyName = objectNameAttribute.getValue(tuple);
		// Copy across all matching attributes.
		outTuple.assign(tuple);

        try {
            S3Object s3Response = client.getObject(getBucket(), keyName);           
            S3ObjectInputStream s3Input = s3Response.getObjectContent(); // set the object stream

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3Input.read(read_buf)) > 0) {
            	bos.write(read_buf, 0, read_len);
            }
            outTuple.setString(objectDataAttribute, bos.toString());
            s3Input.close();
            
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
            errorCode = ase.getErrorCode();
        }
        // do not catch AmazonClientException
		if (null != errorCodeAttribute) {
			outTuple.setString(errorCodeAttribute, errorCode);
		}        
        
        outStream.submit(outTuple);
        if (!preservePunctuation) {
			// submit window_marker on output port 0
        	outStream.punctuate(Punctuation.WINDOW_MARKER);
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
        
	@Parameter(name="preservePunctuation", description="If set to true then the operator forwards punctuation from input port 0 to output port 0. The default value is false and a window punctuation is generated on output port 0 after object is read.", optional=true)
	public void setPreservePunctuation(boolean preservePunctuation) {
		this.preservePunctuation = preservePunctuation;
	}
    
	@Parameter(name="objectNameAttribute", description="This parameter specifies the attribute of the input tuple that contains the object name.", optional=false)
	public void setObjectNameAttribute(TupleAttribute<Tuple,String> objectNameAttribute) {
		this.objectNameAttribute = objectNameAttribute;
	}
	
	@Parameter(name="objectDataAttribute", description="This parameter specifies the attribute name of the output tuple for the object content of type rstring.", optional=false)
	public void setObjectDataAttribute(String objectDataAttribute) {
		this.objectDataAttribute = objectDataAttribute;
	}
	
	@Parameter(name="errorCodeAttribute", description="This parameter specifies the attribute name in the output tuple for error codes returned by the S3 client.", optional=true)
	public void setErrorCodeAttribute(String errorCodeAttribute) {
		this.errorCodeAttribute = errorCodeAttribute;
	}	

}
