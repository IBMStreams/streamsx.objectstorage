//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.objectstorage.s3;

import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.ibm.streams.function.model.Function;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.toolkit.model.ToolkitLibraries;

/**
 * Class for implementing SPL Java native function. 
 */
@ToolkitLibraries({"opt/downloaded/*"})
public class FunctionsImpl {

	static Logger TRACER = Logger.getLogger("com.ibm.streamsx.objectstorage.s3");	

    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="deleteObject", description="Deletes an object in a bucket.", stateful=false)
    public static boolean deleteObject(String objectName, String bucket, String accessKeyID, String secretAccessKey, String endpoint) {
        // initialize S3 client
        ClientConfiguration clientConf = new ClientConfiguration();
//        clientConf.setConnectionTimeout(timeout);
//        clientConf.setSocketTimeout(timeout);
//        clientConf.withUseExpectContinue(false);
//        clientConf.withSignerOverride("S3SignerType");
        clientConf.setProtocol(Protocol.HTTP);
        
        AWSCredentials creds = new BasicAWSCredentials(accessKeyID, secretAccessKey);
        AmazonS3Client client = new AmazonS3Client(creds, clientConf);        
        client.setEndpoint(endpoint);
    	
        boolean result = true;
        try {
        	client.deleteObject(bucket, objectName);
        }
        catch (AmazonClientException ace) {
        	result = false;
            String errMessage = "Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.\n";
            errMessage+="Error Message: " + ace.getMessage();
            TRACER.log(TraceLevel.ERROR, errMessage);
        }
        
    	return result;
    }
    
}
