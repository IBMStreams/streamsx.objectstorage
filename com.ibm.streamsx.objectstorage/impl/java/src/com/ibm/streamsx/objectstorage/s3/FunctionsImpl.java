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
import java.util.Map;
import java.util.logging.Logger;

import com.ibm.cloud.objectstorage.AmazonClientException;
import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
//import com.ibm.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;

import com.ibm.cloud.objectstorage.auth.BasicAWSCredentials;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.Protocol;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.model.Bucket;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectListing;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.model.S3ObjectSummary;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.ibm.streams.function.model.Function;
import com.ibm.streams.operator.PERuntime;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.toolkit.model.ToolkitLibraries;
import com.ibm.streamsx.objectstorage.IObjectStorageConstants;
import com.ibm.streamsx.objectstorage.auth.CosCredentials;

/**
 * Class for implementing SPL Java native function. 
 */
@ToolkitLibraries({"opt/downloaded/*","opt/*"})
public class FunctionsImpl  {

	static Logger TRACER = Logger.getLogger("com.ibm.streamsx.objectstorage.s3");	

	private static AmazonS3 client = null;
	
	private static String S3_ERROR_MESSAGE = "Caught an AmazonClientException, which " +
            "means the client encountered " +
            "an internal error while trying to " +
            "communicate with S3, " +
            "such as not being able to access the network.\n"; 
	
	private static DateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	

	@Function(namespace="com.ibm.streamsx.objectstorage.s3", name="initialize", description="Initialize S3 client using basic authentication. **This method must be called first**. For IBM COS the recommended `endpoint` is the public **us-geo** (CROSS REGION) endpoint `s3.us.cloud-object-storage.appdomain.cloud`.", stateful=false)
    public static boolean initialize(String accessKeyID, String secretAccessKey, String endpoint) {
    	if (null == client) {
    		client = createClient(accessKeyID, secretAccessKey, endpoint, "us", false, true);
    	}
    	return true;
    }
	
	@Function(namespace="com.ibm.streamsx.objectstorage.s3", name="initialize", description="Initialize S3 client using basic authentication. **This method must be called first**. For IBM COS the recommended `endpoint` is the public **us-geo** (CROSS REGION) endpoint `s3.us.cloud-object-storage.appdomain.cloud`.", stateful=false)
    public static boolean initialize(String accessKeyID, String secretAccessKey, String endpoint, boolean sslEnabled) {
    	if (null == client) {
    		client = createClient(accessKeyID, secretAccessKey, endpoint, "us", false, sslEnabled);
    	}
    	return true;
    }
    
	@Function(namespace="com.ibm.streamsx.objectstorage.s3", name="initialize_iam", description="Initialize S3 client using IAM credentials. **This method must be called first**. For IBM COS the recommended `endpoint` is the public **us-geo** (CROSS REGION) endpoint `s3.us.cloud-object-storage.appdomain.cloud`.", stateful=false)
	public static boolean initialize_iam(String apiKey, String serviceInstanceId, String endpoint) {
		boolean result = false;
    	if (null == client) {
    		if ( ((null == apiKey) && (null == serviceInstanceId)) || ((apiKey.isEmpty()) && (serviceInstanceId.isEmpty())) ) {
    			result = initialize(endpoint);
    		}
    		else {
    			client = createClient(apiKey, serviceInstanceId, endpoint, "us", true, true);
    			if (null != client) {
    				result = true;
    			}
    		}    		
    	}
    	return result;
    }

	@Function(namespace="com.ibm.streamsx.objectstorage.s3", name="initialize", description="Initialize S3 client using JSON IAM credentials from IBM Cloud Object Storage service. **This method must be called first** and requires `cos` **application configuration** with property `cos.creds` that contains the IAM credentials. . For IBM COS the recommended `endpoint` is the public **us-geo** (CROSS REGION) endpoint `s3.us.cloud-object-storage.appdomain.cloud`.", stateful=false)
    public static boolean initialize(String endpoint) {
		return initialize(IObjectStorageConstants.DEFAULT_COS_APP_CONFIG_NAME, endpoint);
	}
	
	@Function(namespace="com.ibm.streamsx.objectstorage.s3", name="initialize", description="Initialize S3 client using JSON IAM credentials from IBM Cloud Object Storage service. **This method must be called first** and requires an **application configuration** with name given as `appConfigName` parameter with property `cos.creds` that contains the IAM credentials. For IBM COS the recommended `endpoint` is the public **us-geo** (CROSS REGION) endpoint `s3.us.cloud-object-storage.appdomain.cloud`.", stateful=false)
    public static boolean initialize(String appConfigName, String endpoint) {
		boolean result = false; 
    	if (null == client) {
    		if (TRACER.isLoggable(TraceLevel.TRACE)) {
    			TRACER.log(TraceLevel.TRACE, "getApplicationConfiguration "+appConfigName);
    		}
    		// get application configuration with default name
    		Map<String, String> appConfig = PERuntime.getCurrentContext().getPE().getApplicationConfiguration(appConfigName);
    		if (appConfig.containsKey(IObjectStorageConstants.DEFAULT_COS_CREDS_PROPERTY_NAME)) {
	            String credentials = appConfig.get(IObjectStorageConstants.DEFAULT_COS_CREDS_PROPERTY_NAME);
	            // get credentials property and parse the JSON
	            if (credentials != null) {
	                Gson gson = new Gson();
	                CosCredentials cosCreds;
	                try {
	                	cosCreds = gson.fromJson(credentials, CosCredentials.class);	
	                	String iamApiKey = cosCreds.getApiKey();	                    
	                    String serviceInstanceId = "";
	                    String[] tokens = cosCreds.getResourceInstanceId().split(":");
	                    for(String element:tokens) {
	                    	if (element != "") {
	                    		serviceInstanceId = element;	
	                    	}
	                    }
	                    result = initialize_iam(iamApiKey, serviceInstanceId, endpoint);
	                } catch (JsonSyntaxException e) {
	                	TRACER.log(TraceLevel.ERROR, "Failed to parse credentials property from application configuration. " + e.getMessage());	                	
	                }
	            }
    		}
    	}
    	return result;
    }
	
	@Function(namespace="com.ibm.streamsx.objectstorage.s3", name="initialize_cos", description="Initialize S3 client using JSON IAM credentials from IBM Cloud Object Storage service. **This method must be called first**. For IBM COS the recommended `endpoint` is the public **us-geo** (CROSS REGION) endpoint `s3.us.cloud-object-storage.appdomain.cloud`.", stateful=false)
    public static boolean initialize_cos(String credentials, String endpoint) {
		boolean result = false; 
    	if (null == client) {
            // parse the JSON
            if (credentials != null) {
            	if (credentials.isEmpty()) {
            		return initialize(endpoint); // read default app config
            	}
            	else {
	                Gson gson = new Gson();
	                CosCredentials cosCreds;
	                try {
	                	cosCreds = gson.fromJson(credentials, CosCredentials.class);	
	                	String iamApiKey = cosCreds.getApiKey();	                    
	                    String serviceInstanceId = "";
	                    String[] tokens = cosCreds.getResourceInstanceId().split(":");
	                    for(String element:tokens) {
	                    	if (element != "") {
	                    		serviceInstanceId = element;	
	                    	}
	                    }
	                    result = initialize_iam(iamApiKey, serviceInstanceId, endpoint);
	                } catch (JsonSyntaxException e) {
	                	TRACER.log(TraceLevel.ERROR, "Failed to parse credentials parameter. " + e.getMessage());	                	
	                }
            	}
            }
    	}
    	return result;
    }		
    
    /**
     * @param apiKey (or accessKey)
     * @param serviceInstanceId (or secretKey)
     * @param endpoint
     * @param location
     * @return AmazonS3
     */
    public static AmazonS3 createClient(String apiKey, String serviceInstanceId, String endpoint, String location, boolean isIAM, boolean sslEnabled) {
    	if (TRACER.isLoggable(TraceLevel.TRACE)) {
    		TRACER.log(TraceLevel.TRACE, "createClient");
    	}
		AWSCredentials credentials;
		if (isIAM) {
			credentials = new BasicIBMOAuthCredentials(apiKey, serviceInstanceId);
		}
		else {
			String accessKey = apiKey;
            String secretKey = serviceInstanceId;			
			credentials = new BasicAWSCredentials(accessKey, secretKey);
		}
		ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(5000);
		clientConfig.setUseTcpKeepAlive(true);
		if (false == sslEnabled) {
			clientConfig.setProtocol(Protocol.HTTP);
		}

		return AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withEndpointConfiguration(new EndpointConfiguration(endpoint, location)).withPathStyleAccessEnabled(true)
            .withClientConfiguration(clientConfig).build();
    }
	
    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="createBucket", description="Creates a bucket if it doesn't exist.", stateful=false)
    public static boolean createBucket(String bucket) {
    	return createBucket(bucket, "us-standard"); // create a standard bucket
    }
    
    
    @SuppressWarnings("deprecation")
	@Function(namespace="com.ibm.streamsx.objectstorage.s3", name="createBucket", description="Creates a bucket if it doesn't exist. Select with the locationConstraint argument the bucket type: Standard bucket (us-bucket), Vault bucket (us-vault) or Cold Vault bucket (us-cold)", stateful=false)
    public static boolean createBucket(String bucket, String locationConstraint) {
        boolean result = true;
        if (TRACER.isLoggable(TraceLevel.TRACE)) {
        	TRACER.log(TraceLevel.TRACE, "createBucket "+ bucket);
        }
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
        	if (TRACER.isLoggable(TraceLevel.TRACE)) {
        		TRACER.log(TraceLevel.TRACE, "deleteBucket "+ bucket);
        	}
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
        	if (TRACER.isLoggable(TraceLevel.TRACE)) {
        		TRACER.log(TraceLevel.TRACE, "deleteObject "+ objectName + " in bucket "+ bucket);
        	}
        	client.deleteObject(bucket, objectName);
        }
        catch (AmazonClientException ace) {
        	result = false;
        	TRACER.log(TraceLevel.ERROR, S3_ERROR_MESSAGE + "ERROR: " + ace.getMessage());
        }
    	return result;
    }
    
    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="getObjectStorageURI", description="Converts the bucket to a URI for the objectStorageURI parameter of the ObjectStorageSource, ObjectStorageScan and ObjectStorageSink operators. Returns an URI in format: `s3a://<bucket>/` ", stateful=false)
    public static String getObjectStorageURI(String bucket) {
    	return "s3a://"+bucket+"/";
    }

    @Function(namespace="com.ibm.streamsx.objectstorage.s3", name="getProtocolFromURI", description="Extracts the protocol of the URI. Returns either `cos` or `s3a`", stateful=false)
    public static String getProtocolFromURI(String uri) {
    	int idx = uri.indexOf("://");
    	if (idx > 0)
    		return uri.substring(0, idx);
    	else
    		return "";
    }

}
