//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.objectstorage.s3;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.ibm.streams.function.model.Function;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.toolkit.model.ToolkitLibraries;

/**
 * Class for implementing SPL Java native function. 
 */
@ToolkitLibraries({"opt/downloaded/*","opt/*"})
public class FunctionsImpl  {

	static Logger TRACER = Logger.getLogger("com.ibm.streamsx.objectstorage.s3");	

	private static AmazonS3Client client = null;
	
	private static String S3_ERROR_MESSAGE = "Caught an AmazonClientException, which " +
            "means the client encountered " +
            "an internal error while trying to " +
            "communicate with S3, " +
            "such as not being able to access the network.\n"; 
	
	private static DateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	
    @SuppressWarnings("deprecation")
	@Function(namespace="com.ibm.streamsx.objectstorage.s3", name="initialize", description="Initialize S3 client. This method must be called first.", stateful=false)
    public static boolean initialize(String accessKeyID, String secretAccessKey, String endpoint) {
    	if (null == client) {
	        // initialize S3 client
	        ClientConfiguration clientConf = new ClientConfiguration();
	        clientConf.setProtocol(Protocol.HTTP);
	        
	        AWSCredentials creds = new BasicAWSCredentials(accessKeyID, secretAccessKey);
	        client = new AmazonS3Client(creds, clientConf);        
	        client.setEndpoint(endpoint);
    	}
    	return true;
    }	
	
    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="createBucket", description="Creates a bucket if it doesn't exist.", stateful=false)
    public static boolean createBucket(String bucket) {
    	return createBucket(bucket, "us-standard"); // create a standard bucket
    }
    
    
    @SuppressWarnings("deprecation")
	@Function(namespace="com.ibm.streamsx.objectstorage.s3", name="createBucket", description="Creates a bucket if it doesn't exist. Select with the locationConstraint argument the bucket type: Standard bucket (us-bucket), Vault bucket (us-vault) or Cold Vault bucket (us-cold)", stateful=false)
    public static boolean createBucket(String bucket, String locationConstraint) {
        boolean result = true;
        try {
        	if (!client.doesBucketExist(bucket)) {
        		client.createBucket(bucket, locationConstraint);
        	}
        }
        catch (AmazonClientException ace) {
        	if (!ace.getMessage().contains("BucketAlreadyExists")) {
        		result = false;
        		TRACER.log(TraceLevel.ERROR, S3_ERROR_MESSAGE + "ERROR: " + ace.getMessage());
        	}
        }
    	return result;
    }    
    
    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="deleteBucket", description="Deletes a bucket.", stateful=false)
    public static boolean deleteBucket(String bucket) {
        boolean result = true;
        try {
       		TRACER.log(TraceLevel.TRACE, "deleteBucket "+ bucket);
       		client.deleteBucket(bucket);
        }
        catch (AmazonClientException ace) {
        	result = false;
        	TRACER.log(TraceLevel.ERROR, S3_ERROR_MESSAGE + "ERROR: " + ace.getMessage());
        }
    	return result;
    }
    
    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="listBuckets", description="Lists all bucket names.", stateful=false)
    public static String[] listBuckets() {
    	String[] resultList = null;
        try {        	
        	List<Bucket> buckets = client.listBuckets(); // get a list of buckets
        	resultList = new String[buckets.size()];
        	int i = 0;
        	for (Bucket b : buckets) { // for each bucket...
        		resultList[i] = b.getName();
        		i++;
        	}
        }
        catch (AmazonClientException ace) {
        	resultList = null;
            TRACER.log(TraceLevel.ERROR, S3_ERROR_MESSAGE + "ERROR: " + ace.getMessage());
        }
    	return resultList;
    }
    
    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="listObjects", description="Lists all object names in a bucket.", stateful=false)
    public static String[] listObjects(String bucket) {
    	String[] resultList = null;
        try {
        	ObjectListing listing = client.listObjects(bucket);
        	List<S3ObjectSummary> summaries = listing.getObjectSummaries(); // create a list of object summaries
        	resultList = new String[summaries.size()];
        	int i = 0;
        	for (S3ObjectSummary obj : summaries){ // for each object...
        		resultList[i] = obj.getKey();
        		i++;
        	}
        }
        catch (AmazonClientException ace) {
        	resultList = null;
            TRACER.log(TraceLevel.ERROR, S3_ERROR_MESSAGE + "ERROR: " + ace.getMessage());
        }
    	return resultList;
    }    
    
    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="getObjectMetadata", description="Get object metadata.", stateful=false)
    public static String[] getObjectMetadata(String bucket, String objectName) {
    	String[] resultList = null;
        try {
        	ObjectMetadata objectMetadata = client.getObjectMetadata(bucket, objectName);        	        	
        	return new String[] {String.valueOf(objectMetadata.getContentLength()), DATE_FORMAT.format(objectMetadata.getLastModified())};
        }
        catch (AmazonClientException ace) {
        	resultList = null;
            TRACER.log(TraceLevel.ERROR, S3_ERROR_MESSAGE + "ERROR: " + ace.getMessage());
        }
    	return resultList;
    }    
    
    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="deleteAllObjects", description="Deletes all objects in a bucket.", stateful=false)
    public static boolean deleteAllObjects(String bucket) {
        boolean result = true;
        try {
        	ObjectListing listing = client.listObjects(bucket);
        	List<S3ObjectSummary> summaries = listing.getObjectSummaries(); // create a list of object summaries
        	for (S3ObjectSummary obj : summaries){ // for each object...
				deleteObject(obj.getKey(), bucket);
        	}
        }
        catch (AmazonClientException ace) {
        	result = false;
        	TRACER.log(TraceLevel.ERROR, S3_ERROR_MESSAGE + "ERROR: " + ace.getMessage());
        }
    	return result;
    }
    
    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="deleteObject", description="Deletes an object in a bucket.", stateful=false)
    public static boolean deleteObject(String objectName, String bucket) {
        boolean result = true;
        try {
        	TRACER.log(TraceLevel.TRACE, "deleteObject "+ objectName + " in bucket "+ bucket);
        	client.deleteObject(bucket, objectName);
        }
        catch (AmazonClientException ace) {
        	result = false;
        	TRACER.log(TraceLevel.ERROR, S3_ERROR_MESSAGE + "ERROR: " + ace.getMessage());
        }
    	return result;
    }
    
    
}
