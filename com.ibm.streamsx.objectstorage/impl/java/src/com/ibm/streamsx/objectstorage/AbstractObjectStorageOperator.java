/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.objectstorage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.objectstorage.auth.AuthenticationType;
import com.ibm.streamsx.objectstorage.auth.OSAuthenticationHelper;
import com.ibm.streamsx.objectstorage.auth.CosCredentials;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streamsx.objectstorage.client.IObjectStorageClient;
import com.ibm.streamsx.objectstorage.client.ObjectStorageClientFactory;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;


/**
 * Base class for all toolkit operators.
 * Contains common operator logic, like
 * object storage connection establishment. 
 * @author streamsadmin
 *
 */
@SharedLoader
public abstract class AbstractObjectStorageOperator extends AbstractOperator  {

	private static final String CLASS_NAME = "com.ibm.streamsx.objectstorage.AbstractObjectStorageOperator";
	public static final String EMPTY_STR = "";

	/**
	 * Create a logger specific to this class
	 */
	private static Logger TRACE = Logger.getLogger(CLASS_NAME);
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME);

	// Common parameters and variables for connection
	private IObjectStorageClient fObjectStorageClient;
	private String fObjectStorageUser;
	private String fObjectStoragePassword;
	private String fObjectStorageProjectID;
	private String fObjectStorageURI;
	// IAM specific authentication parameteres
	private String fIAMApiKey = null;
	public static String defaultIAMTokenEndpoint = "https://iam.bluemix.net/oidc/token";
	private String fIAMTokenEndpoint = defaultIAMTokenEndpoint;
	private String fIAMServiceInstanceId = null;
	private String fEndpoint;
	private String fBucketName;
	private String fAppConfigName;
	private String fCredentials;
	protected boolean sslEnabled = true;
	
	protected Properties fAppConfigCredentials = null;

	// Other variables
	protected Thread processThread = null;
	protected boolean shutdownRequested = false;
	
	private String endpoint;

	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
		
		Configuration config = new Configuration();	
		// "hadoop.home.dir" must be defined to avoid exception
		System.setProperty(Constants.HADOOP_HOME_DIR_CONFIG_NAME, Constants.HADOOP_HOME_DIR_DEFAULT);
		
		if (TRACE.isLoggable(TraceLevel.TRACE)) {
			TRACE.log(TraceLevel.TRACE, "fObjectStorageURI: '" + fObjectStorageURI + "'");
		}
		
		// set endpoint
		// for stocator scheme (cos) - add hadoop service name 
		config.set(Utils.formatProperty(Constants.S3_SERVICE_ENDPOINT_CONFIG_NAME, Utils.getProtocol(fObjectStorageURI)), getEndpoint());
		// for s3a set global one as well
		config.set(Utils.formatProperty(Constants.S3_ENDPOINT_CONFIG_NAME, Utils.getProtocol(fObjectStorageURI)), getEndpoint());
		// set maximum number of connection attempts
		config.set(Constants.S3_MAX_CONNECTION_ATTEMPTS_CONFIG_NAME, String.valueOf(Constants.S3_DEFAULT_MAX_CONNECTION_ATTEMPTS_NUM));
		
		
	    fObjectStorageURI = Utils.getEncodedURIStr(genServiceExtendedURI());
	    fBucketName = Utils.getBucket(fObjectStorageURI);
	    
	    if (TRACE.isLoggable(TraceLevel.INFO)) {
	    	TRACE.log(TraceLevel.INFO, "Formatted URI: '" + fObjectStorageURI + "'");
	    }
	    boolean isProtocolS3a = Utils.isProtocolS3a(fObjectStorageURI);
	    
	    if ((AuthenticationType.IAM == OSAuthenticationHelper.getAuthenticationType(context)) && (!fObjectStorageURI.startsWith("file:")) ) {  	
	        // operator is not configured for basic authentication
			// check if application configuration contains the IAM credentials in JSON
			String appConfigName = Utils.getParamSingleStringValue(context, IObjectStorageConstants.PARAM_APP_CONFIG_NAME, IObjectStorageConstants.DEFAULT_COS_APP_CONFIG_NAME);
	        Map<String, String> appConfig = context.getPE().getApplicationConfiguration(appConfigName);
	        if (appConfig.containsKey(IObjectStorageConstants.DEFAULT_COS_CREDS_PROPERTY_NAME)) {
	            String credentials = appConfig.get(IObjectStorageConstants.DEFAULT_COS_CREDS_PROPERTY_NAME);
	            if (credentials != null) {
	                try {
	                	if (isProtocolS3a) {
	                		parseHmacKeys(credentials);
	                	}
	                	else {
	                		parseIAMParams(credentials);
	                	}
	                } catch (JsonSyntaxException e) {
	                	TRACE.log(TraceLevel.ERROR,	"Failed to parse credentials property (IAM) from application configuration '" + appConfigName + "'. ERROR: '" + e.getMessage() + "'");
	                	fAppConfigCredentials = null;
	                } 
	                if (null == fAppConfigCredentials) {
	                	// check if credentials JSON contains HMAC keys
	            		try {
	            			JSONObject obj = JSONObject.parse(credentials);         		 
	            			String accessKeyID = null;
	            			fAppConfigCredentials = new Properties();
	            			if (obj.containsKey("access_key_id")) {
	            				accessKeyID = (String)obj.get("access_key_id");
	            			}
	            			else if (obj.containsKey("accessKeyID")) {
	            				accessKeyID = (String)obj.get("accessKeyID");
	            			}
	            			if ((accessKeyID != null) && (false == accessKeyID.trim().isEmpty())) {
	            				fAppConfigCredentials.put(IObjectStorageConstants.PARAM_ACCESS_KEY_ID, accessKeyID);
	            			}
	            			String secretAccessKey = null;
	            			if (obj.containsKey("secret_access_key")) {
	            				secretAccessKey = (String)obj.get("secret_access_key");
	            			}
	            			else if (obj.containsKey("secretAccessKey")) {
	            				secretAccessKey = (String)obj.get("secretAccessKey");
	            			}
	            			if ((secretAccessKey != null) && (false == secretAccessKey.trim().isEmpty())) {
	            				fAppConfigCredentials.put(IObjectStorageConstants.PARAM_SECRET_ACCESS_KEY, secretAccessKey);
	            			}
	            			if ((accessKeyID == null) && (secretAccessKey == null)) {
	            				TRACE.log(TraceLevel.ERROR,	"Failed to parse credentials HMAC properties (access_key_id, secret_access_key) from application configuration.");
	            				fAppConfigCredentials = null;
	            			}
						} catch (Exception e) {
							TRACE.log(TraceLevel.ERROR,	"Failed to parse credentials property (HMAC) from application configuration '" + appConfigName + "'. ERROR: '" + e.getMessage() + "'");
							fAppConfigCredentials = null;
						}
	                }
	            }
		        else {
		        	TRACE.log(TraceLevel.ERROR,	"Missing credentials property '" + IObjectStorageConstants.DEFAULT_COS_CREDS_PROPERTY_NAME + "' in application configuration '" + appConfigName + "'");
		        }
	        }
	        // check if application configuration contains properties for HMAC keys
	        else if ((appConfig.containsKey(IObjectStorageConstants.DEFAULT_ACCESS_KEY_ID_PROPERTY_NAME)) && (appConfig.containsKey(IObjectStorageConstants.DEFAULT_SECRET_ACCESS_KEY_PROPERTY_NAME))) {
	        	String accessKeyID = appConfig.get(IObjectStorageConstants.DEFAULT_ACCESS_KEY_ID_PROPERTY_NAME);
	        	String secretAccessKey = appConfig.get(IObjectStorageConstants.DEFAULT_SECRET_ACCESS_KEY_PROPERTY_NAME);
	        	fAppConfigCredentials = new Properties();
	        	if ((accessKeyID != null) && (false == accessKeyID.trim().isEmpty())) {
    				fAppConfigCredentials.put(IObjectStorageConstants.PARAM_ACCESS_KEY_ID, accessKeyID);
    			}
    			if ((secretAccessKey != null) && (false == secretAccessKey.trim().isEmpty())) {
    				fAppConfigCredentials.put(IObjectStorageConstants.PARAM_SECRET_ACCESS_KEY, secretAccessKey);
    			}
    			if ((accessKeyID == null) && (secretAccessKey == null)) {
    				TRACE.log(TraceLevel.ERROR,	"Empty HMAC property values (accessKeyID, secretAccessKey) in application configuration.");
    				fAppConfigCredentials = null;
    			}
	        }
	        // check if credentials are part operator parameter credentials
	        if (null != fCredentials)  {
	        	if  (!fCredentials.isEmpty()) {
	                try {
	                	if (isProtocolS3a) {
	                		// check for HMAC keys
	                		parseHmacKeys(fCredentials);
	                	}
	                	else {	               
	                		parseIAMParams(fCredentials);
	                	}
	                } catch (JsonSyntaxException e) {
	                	TRACE.log(TraceLevel.ERROR,	"Failed to parse credentials from operator parameter 'credentials'. ERROR: '" + e.getMessage() + "'");
	                }
	        	}
	        	else if (TRACE.isLoggable(TraceLevel.DEBUG)) {
            		TRACE.log(TraceLevel.DEBUG,	"credentials parameter is empty");
            	}
	        }
	        if ((null == fAppConfigCredentials) && (null == fIAMApiKey || fIAMApiKey.isEmpty()) && (null == fIAMServiceInstanceId || fIAMServiceInstanceId.isEmpty())) {
	        	String errMessage = "Missing IAM/HMAC credentials. Either set '" + IObjectStorageConstants.DEFAULT_COS_CREDS_PROPERTY_NAME + "' in application configuration for IAM/HMAC or set operator parameter 'credentials' (COS service JSON) or parameter 'credentials' or parameters 'accessKeyID' ('objectStorageUser') and 'secretAccessKey' ('objectStoragePassword').";
	        	TRACE.log(TraceLevel.ERROR,	errMessage);
	        	throw new Exception(errMessage);
	        }
	    }
	    
		// set up operator specific configuration
		setOpConfig(config);
		String formattedPropertyName = Utils.formatProperty(Constants.S3_SERVICE_ENDPOINT_CONFIG_NAME, Utils.getProtocol(fObjectStorageURI));
		endpoint = config.get(formattedPropertyName); // required for error message in connect()
		
		fObjectStorageClient = createObjectStorageClient(context, config, fAppConfigCredentials);
		connect();
	}

	
	private void parseIAMParams (String credentials) throws Exception {
        Gson gson = new Gson();
        CosCredentials cosCreds;		
		cosCreds = gson.fromJson(credentials, CosCredentials.class);
    	String iamApiKey = cosCreds.getApiKey();
    	if (TRACE.isLoggable(TraceLevel.DEBUG)) {
    		TRACE.log(TraceLevel.DEBUG,	"iamApiKey (from credentials parameter): " + iamApiKey);
    	}
    	if ((iamApiKey != null) && (false == iamApiKey.trim().isEmpty())) {
    		fAppConfigCredentials = new Properties();
    		fIAMApiKey = iamApiKey;
    		fAppConfigCredentials.put(IObjectStorageConstants.PARAM_IAM_APIKEY, iamApiKey);
    	
            String serviceInstanceId = "";
            if (cosCreds.getResourceInstanceId() != null) {
                String[] tokens = cosCreds.getResourceInstanceId().split(":");
                for(String element:tokens) {
                	if (element != "") {
                		serviceInstanceId = element;	
                	}
                }
            }
            if (TRACE.isLoggable(TraceLevel.DEBUG)) {
            	TRACE.log(TraceLevel.DEBUG,	"serviceInstanceId (from credentials parameter): " + serviceInstanceId);
            }
            if ((serviceInstanceId != null) && (false == serviceInstanceId.trim().isEmpty())) {
            	fIAMServiceInstanceId = serviceInstanceId;
            	fAppConfigCredentials.put(IObjectStorageConstants.PARAM_IAM_SERVICE_INSTANCE_ID, serviceInstanceId);
            }	                    
            String IAMTokenEndpoint = getIAMTokenEndpoint(cosCreds.getEndpoints());
            fIAMTokenEndpoint = ((IAMTokenEndpoint != null) ? IAMTokenEndpoint : defaultIAMTokenEndpoint);
            fAppConfigCredentials.put(IObjectStorageConstants.PARAM_IAM_TOKEN_ENDPOINT, ((IAMTokenEndpoint != null) ? IAMTokenEndpoint : defaultIAMTokenEndpoint));
    	}	
	}
	
	private void parseHmacKeys (String credentials) throws Exception {
		// check for HMAC keys under cos_hmac_keys
		JSONObject obj = JSONObject.parse(credentials);
		String accessKeyID = null;
		String secretAccessKey = null;
		if (obj.containsKey("cos_hmac_keys")) {
			fAppConfigCredentials = new Properties();
			JSONObject obj1 = (JSONObject) obj.get("cos_hmac_keys"); 
			accessKeyID = (String)obj1.get("access_key_id");   
			if ((accessKeyID != null) && (false == accessKeyID.trim().isEmpty())) {
				fAppConfigCredentials.put(IObjectStorageConstants.PARAM_ACCESS_KEY_ID, accessKeyID);
			}
			secretAccessKey = (String)obj1.get("secret_access_key");
			if ((secretAccessKey != null) && (false == secretAccessKey.trim().isEmpty())) {
				fAppConfigCredentials.put(IObjectStorageConstants.PARAM_SECRET_ACCESS_KEY, secretAccessKey);
			}
		}
		if ((accessKeyID == null) && (secretAccessKey == null)) {
			TRACE.log(TraceLevel.ERROR,	"Failed to parse cos_hmac_keys (access_key_id, secret_access_key) in credentials JSON.");
			fAppConfigCredentials = null;
		}
	}
	
	protected void connect() throws Exception {
		
	    try {
	    	// The client will try  to connect "fs.s3a.attempts.maximum"
	    	// times and then IOException will be thrown
	    	fObjectStorageClient.connect();
	    }  
	    // no bucket with given name found
	    catch (FileNotFoundException fnfe) {
	    	String errMsg = Messages.getString("OBJECTSTORAGE_BUCKET_NOT_FOUND", fBucketName);
			
	    	if (TRACE.isLoggable(TraceLevel.ERROR)) {
				TRACE.log(TraceLevel.ERROR,	errMsg); 
				TRACE.log(TraceLevel.ERROR,	"Bucket '" + fBucketName + "' does not exist. Exception: " + fnfe.getMessage());
			}
	    	LOGGER.log(TraceLevel.ERROR, Messages.getString("OBJECTSTORAGE_BUCKET_NOT_FOUND", fBucketName));
	    	throw new Exception(fnfe);
	    }
	    catch (IOException ioe) {
			String errMsg = Messages.getString("OBJECTSTORAGE_SINK_AUTH_CONNECT", endpoint);
			
	    	if (TRACE.isLoggable(TraceLevel.ERROR)) {
				TRACE.log(TraceLevel.ERROR,	errMsg); 
				TRACE.log(TraceLevel.ERROR,	"Failed to connect to cloud object storage with endpoint '" + endpoint + "'. Exception: " + ioe.getMessage());
			}
	    	LOGGER.log(TraceLevel.ERROR, Messages.getString("OBJECTSTORAGE_SINK_AUTH_CONNECT", endpoint));
	    	throw new Exception(ioe);
	    }
	}

	protected abstract void setOpConfig(Configuration config) throws Exception;

	@Override
	public void allPortsReady() throws Exception {
		super.allPortsReady();
		if (processThread != null) {
			startProcessing();
		}
	}

	protected synchronized void startProcessing() {
		processThread.start();
	}

	/**
	 * By default, this does nothing.
	 */
	protected void process() throws Exception {

	}

	public void shutdown() throws Exception {

		shutdownRequested = true;
		if (fObjectStorageClient != null) {
			fObjectStorageClient.disconnect();
		}

		super.shutdown();
	}

	protected Thread createProcessThread() {
		Thread toReturn = getOperatorContext().getThreadFactory().newThread(
				new Runnable() {

					@Override
					public void run() {
						try {
							process();
						} catch (Exception e) {
							TRACE.log(TraceLevel.ERROR, e.getMessage());
							// if we get to the point where we got an exception
							// here we should rethrow the exception to cause the
							// operator to shut down.
							throw new RuntimeException(e);	
							

						}
					}
				});
		toReturn.setDaemon(false);
		return toReturn;
	}

	protected IObjectStorageClient createObjectStorageClient(OperatorContext opContext, Configuration config, Properties appConfigCredentials) throws Exception {
		return ObjectStorageClientFactory.getObjectStorageClient(fObjectStorageURI, opContext, appConfigCredentials, config);
	}
	
	protected String getAbsolutePath(String filePath) {
		if(filePath == null) 
			return null;
		
		Path p = new Path(filePath);
		if(p.isAbsolute()) {
			return filePath;
		} else {
			File f = new File (getOperatorContext().getPE().getApplicationDirectory(), filePath);
			return f.getAbsolutePath();
		}
	}
	
	@Parameter(optional = true, description = "Enables or disables SSL connections to S3, default is true.")
	public void setSslEnabled(boolean sslEnabled) {
		this.sslEnabled = sslEnabled;
	}

	public boolean isSslEnabled() {
		return sslEnabled;
	}
	
	public IObjectStorageClient getObjectStorageClient() {
		return fObjectStorageClient;
	}


	public void setUserID(String objectStorageUser) {
		fObjectStorageUser = objectStorageUser;
	}

	public String getUserID() {
		return fObjectStorageUser;
	}



	public void setPassword(String objectStoragePassword) {
		fObjectStoragePassword = objectStoragePassword;
	}

	public String getPassword() {
		return fObjectStoragePassword;
	}


	public void setProjectID(String objectStorageProjectID) {		
		fObjectStorageProjectID = objectStorageProjectID;
	}
	
	public String getProjectID() {
		return fObjectStorageProjectID;
	}

	public void setURI(String objectStorageURI) {
		if (objectStorageURI.endsWith("/")) {
			fObjectStorageURI = objectStorageURI;
		}
		else {
			fObjectStorageURI = objectStorageURI+"/";
		}
	}
	
	public String getURI() {
		return fObjectStorageURI;
	}
		
	
	public void setEndpoint(String endpoint) {
		fEndpoint = endpoint;
	}
	
	public String getEndpoint() {
		return fEndpoint;
	}

	public void setIAMApiKey(String iamApiKey) {
		fIAMApiKey  = iamApiKey;
	}
	
	public String getIAMApiKey() {
		return fIAMApiKey;
	}
	
	public void setIAMTokenEndpoint(String iamTokenEndpoint) {
		fIAMTokenEndpoint = iamTokenEndpoint;
	}
	
	public String getIAMTokenEndpoint() {
		return fIAMTokenEndpoint;
	}
	
	public void setIAMServiceInstanceId(String iamServiceInstanceId) {
		fIAMServiceInstanceId = iamServiceInstanceId;
	}
	
	public String getIAMServiceInstanceId() {
		return fIAMServiceInstanceId;
	}

	public void setCredentials(String credentials) {
		fCredentials = credentials;
	}
	
	public String getCredentials() {
		return fCredentials;
	}	
	
	public void setAppConfigName(String appConfigName) {
		fAppConfigName = appConfigName;
	}
	
	public String getAppConfigName() {
		return fAppConfigName;
	}
	
	public String getBucketName() {
		return fBucketName;				
	}
	
	public String genServiceExtendedURI()  {
		String protocol = Utils.getProtocol(fObjectStorageURI);
		String authority = Utils.getBucket(fObjectStorageURI);
		// stocator requires hostname in the form container.service
		if (protocol.equals(Constants.COS) &&  !authority.endsWith("." + Constants.DEFAULT_SERVICE_NAME)) {
			authority += "." + Constants.DEFAULT_SERVICE_NAME;
		}
				
		return protocol + Constants.PROTOCOL_URI_DELIM +  authority + Constants.URI_DELIM ;
	}

	@ContextCheck(compile = true)
	public static void checkCompileParameters(OperatorContextChecker checker)
			throws Exception {
				
		checker.checkDependentParameters(IObjectStorageConstants.PARAM_OS_USER, 
										 IObjectStorageConstants.PARAM_OS_PASSWORD);
		
	}
	
	private String getIAMTokenEndpoint(String url) throws Exception {		
		String iamHost = null;
		if (TRACE.isLoggable(TraceLevel.DEBUG)) {
			TRACE.log(TraceLevel.DEBUG,	"get iam token host with endpoints url:" + url);
		}
	
		String[] cmd = { "/bin/sh", "-c", "curl -s "+url };
			
		StringBuffer output = new StringBuffer();
		Process p;
		try {
			p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = br.readLine())!= null) {
				output.append(line);
			}
			String cmdResult = output.toString();
			TRACE.log(TraceLevel.DEBUG,	"cmdResult: " + cmdResult);
			JSONArtifact root = JSON.parse(cmdResult);
			JSONObject json = (JSONObject)root;
			JSONObject endpointsObj = (JSONObject) json.get("identity-endpoints");
			Object tokenObj = endpointsObj.get("iam-token");
			iamHost = tokenObj.toString();
			iamHost = "https://" + iamHost + "/oidc/token";
			if (TRACE.isLoggable(TraceLevel.INFO)) {
				TRACE.log(TraceLevel.INFO,	"IAMTokenEndpoint: " + iamHost);
			}		
		} catch (Exception e) {
			e.printStackTrace();
		}
		return iamHost;
	}	
	
	public static final String AUTHENTICATION_DESC =
			"\\n"+
			"\\n+ Supported Authentication Schemes" +
			"\\n"+
			"\\nThe operator supports IBM Cloud Identity and Access Management (IAM) and HMAC for authentication."+
			"\\n"+
			"\\n++ IAM authentication\\n"+
			"\\nIAM authentication can be configured with operator parameters or application configuration."+
			"\\n"+
			"\\nThe priority of the IAM authentication options is\\n"
	        + "1. **credentials** operator parameter\\n"
	        + "1. application configuration with property called `cos.creds` (ignored if option above is set)\\n"
	        + "\\n"+
			"\\n+++ 1) IBM COS (IAM) authentication with credentials operator parameter\\n"+
			"\\nFor IBM COS service authentication the following operator parameter should be used:"+
			"\\n* credentials\\n"+
			"\\nThe parameter accepts the IBM Cloud Object Storage Credentials JSON from the IBM Cloud Object Storage service."+
			"\\nThe operator reads the required IAM credentials from the JSON."+
			"\\n\\n"+
			"\\n**Create IBM Cloud Object Storage Credentials**\\n" +
    		"\\nA service credential provides the necessary information to connect an application to Object Storage packaged in a JSON document. Service credentials are always associated with a Service ID, and new Service IDs can be created along with a new credential.\\n" +
    		"\\nUse the following steps to create a service credential:\\n" +
    		"\\n" + 
    		" 1. Log in to the IBM Cloud console and navigate to your instance of Object Storage.\\n" +
    		" 2. In the side navigation, click Service Credentials.\\n" +
    		" 3. Click New credential and provide the necessary information.\\n" +
    		" 4. Click Add to generate service credential.\\n" +
    		" 5. Click View credentials and copy JSON into clipboard (use this as value of the **credentials** operator parameter).\\n" +
			"\\n"+
			"This is an example of a service credential:\\n"+    		
			"\\n    {"+
			"\\n         \\\"apikey\\\": \\\"0viPHOY7LbLNa9eLftrtHPpTjoGv6hbLD1QalRXikliJ\\\","+
			"\\n         \\\"endpoints\\\": \\\"https://cos-service.bluemix.net/endpoints\\\","+
			"\\n         \\\"iam_apikey_description\\\": \\\"Auto generated apikey during resource-key operation for Instance - crn:v1:bluemix:public:cloud-object-storage:global:a/3ag0e9402tyfd5d29761c3e97696b71n:d6f74k03-6k4f-4a82-b165-697354o63903::\\\","+
			"\\n         \\\"iam_apikey_name\\\": \\\"auto-generated-apikey-f9274b63-ef0b-4b4e-a00b-b3bf9023f9dd\\\","+
			"\\n         \\\"iam_role_crn\\\": \\\"crn:v1:bluemix:public:iam::::serviceRole:Manager\\\","+
			"\\n         \\\"iam_serviceid_crn\\\": \\\"crn:v1:bluemix:public:iam-identity::a/3ag0e9402tyfd5d29761c3e97696b71n::serviceid:ServiceId-540a4a41-7322-4fdd-a9e7-e0cb7ab760f9\\\","+
			"\\n         \\\"resource_instance_id\\\": \\\"crn:v1:bluemix:public:cloud-object-storage:global:a/3ag0e9402tyfd5d29761c3e97696b71n:d6f74k03-6k4f-4a82-b165-697354o63903::\\\""+
			"\\n    }\\n"+			
			"\\n"+	        
			"\\n+++ 2) IBM COS (IAM) authentication with application configuration\\n"+
			"\\n"+
			"**Create IBM Cloud Object Storage Credentials**\\n" +
    		"\\nA service credential provides the necessary information to connect an application to Object Storage packaged in a JSON document. Service credentials are always associated with a Service ID, and new Service IDs can be created along with a new credential.\\n" +
    		"\\nUse the following steps to create a service credential:\\n" +
    		"\\n" + 
    		" 1. Log in to the IBM Cloud console and navigate to your instance of Object Storage.\\n" +
    		" 2. In the side navigation, click Service Credentials.\\n" +
    		" 3. Click New credential and provide the necessary information.\\n" +
    		" 4. Click Add to generate service credential.\\n" +
    		" 5. Click View credentials and copy JSON into clipboard.\\n" +
			"\\n"+
			"This is an example of a service credential:\\n"+    		
			"\\n    {"+
			"\\n         \\\"apikey\\\": \\\"0viPHOY7LbLNa9eLftrtHPpTjoGv6hbLD1QalRXikliJ\\\","+
			"\\n         \\\"endpoints\\\": \\\"https://cos-service.bluemix.net/endpoints\\\","+
			"\\n         \\\"iam_apikey_description\\\": \\\"Auto generated apikey during resource-key operation for Instance - crn:v1:bluemix:public:cloud-object-storage:global:a/3ag0e9402tyfd5d29761c3e97696b71n:d6f74k03-6k4f-4a82-b165-697354o63903::\\\","+
			"\\n         \\\"iam_apikey_name\\\": \\\"auto-generated-apikey-f9274b63-ef0b-4b4e-a00b-b3bf9023f9dd\\\","+
			"\\n         \\\"iam_role_crn\\\": \\\"crn:v1:bluemix:public:iam::::serviceRole:Manager\\\","+
			"\\n         \\\"iam_serviceid_crn\\\": \\\"crn:v1:bluemix:public:iam-identity::a/3ag0e9402tyfd5d29761c3e97696b71n::serviceid:ServiceId-540a4a41-7322-4fdd-a9e7-e0cb7ab760f9\\\","+
			"\\n         \\\"resource_instance_id\\\": \\\"crn:v1:bluemix:public:cloud-object-storage:global:a/3ag0e9402tyfd5d29761c3e97696b71n:d6f74k03-6k4f-4a82-b165-697354o63903::\\\""+
			"\\n    }\\n"+  		
			"\\n"+			
			"**Save Credentials in Application Configuration Property**\\n" + 
    		"\\n" + 
    		"With this option, users can copy their IBM Cloud Object Storage Credentials JSON from the IBM Cloud Object Storage service and "
    		+ "store it in an application configuration property called `cos.creds`. When the operator starts, "
    		+ "it will look for that property and extract the information needed to connect. "
    		+ "The following steps outline how this can be done: \\n" + 
    		"\\n" + 
    		" 1. Create an application configuration called `cos`.\\n" + 
    		" 2. Create a property in the `cos` application configuration *named* `cos.creds`.\\n" + 
    		"   * The *value* of the property should be the raw IBM Cloud Object Storage Credentials JSON\\n" +
    		"   * The *value* of the property could be pasted from the clipboard if you have done the *Create IBM Cloud Object Storage Credentials* steps above. \\n" +
    		" 3. The operator will automatically look for an application configuration named `cos` and will extract "
    		+ "the information needed to connect.\\n" +
    		"\\nThis is an example of an application configuration in Streams Console:\\n"+
    		"{../../doc/images/appConfig.png}\\n" +
    		"\\nFrom the `cos.creds` JSON the `apikey` (**IAMApiKey**) and `resource_instance_id` (**IAMServiceInstanceId**) are extracted by the operator."+
    		"\\nThe auth endpoint value (**IAMTokenEndpoint**) is extracted from the `endpoints` URL provided as part of the service credentials. With the `endpoints` URL a JSON is retrieved and /oidc/token is added to end of the iam-token URL to construct the **IAMTokenEndpoint**.\\n"+
			"\\n    {"+
			"\\n       \\\"identity-endpoints\\\": {"+
			"\\n          \\\"iam-token\\\": \\\"iam.bluemix.net\\\","+
			"\\n          \\\"iam-policy\\\": \\\"iampap.bluemix.net\\\""+
			"\\n    },"+
			"\\n    ..."+
			"\\n"+
			"\\n++ HMAC authentication\\n"+
			"\\nHMAC authentication can be configured with operator parameters or application configuration."+
			"\\n"+
			"\\nThe priority of the HMAC authentication options is\\n"
	        + "1. application configuration with property called `cos.creds` \\n"
	        + "1. application configuration with properties called `accessKeyID` and `secretAccessKey` (ignored if option above is set)\\n"
	        + "1. `objectStorageUser` (`accessKeyID`), `objectStoragePassword` (`secretAccessKey`) operator parameters (ignored if options above are set)\\n"
	        + "\\n"+			
			"\\nFor S3-compliant COS use **AccessKeyID** for 'objectStorageUser' and **SecretAccessKey** for 'objectStoragePassword'.\\n"+
	        "\\nThe value of the application configuration property `cos.creds` should be a JSON in the format below:"+
			"\\n    { \\\"access_key_id\\\": \\\"7exampledonotusea6440da12685eee02\\\", \\\"secret_access_key\\\": \\\"8ddbece407exampledonotuse43r2d2586\\\" }"+
			"\\n"			
	        ;
	
}
