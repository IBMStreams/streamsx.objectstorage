/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.objectstorage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streamsx.objectstorage.client.IObjectStorageClient;
import com.ibm.streamsx.objectstorage.client.ObjectStorageClientFactory;

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

	// Other variables
	protected Thread processThread = null;
	protected boolean shutdownRequested = false;

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
	    
		// set up operator specific configuration
		setOpConfig(config);
		
		fObjectStorageClient = createObjectStorageClient(context, config);
		
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
			String formattedPropertyName = Utils.formatProperty(Constants.S3_SERVICE_ENDPOINT_CONFIG_NAME, Utils.getProtocol(fObjectStorageURI));
			String endpoint = config.get(formattedPropertyName);
			String errMsg = Messages.getString("OBJECTSTORAGE_SINK_AUTH_CONNECT", endpoint);
			
	    	if (TRACE.isLoggable(TraceLevel.ERROR)) {
				TRACE.log(TraceLevel.ERROR,	errMsg); 
				TRACE.log(TraceLevel.ERROR,	"Failed to connect to cloud object storage with endpoint '" + endpoint + "'. Exception: " + ioe.getMessage());
			}
	    	LOGGER.log(TraceLevel.ERROR, Messages.getString("OBJECTSTORAGE_SINK_AUTH_CONNECT", endpoint));
	    	throw new Exception(ioe);
	    }
	}
	

	protected abstract void setOpConfig(Configuration config) throws IOException, URISyntaxException ;

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

	protected IObjectStorageClient createObjectStorageClient(OperatorContext opContext, Configuration config) throws Exception {
		
		
		return ObjectStorageClientFactory.getObjectStorageClient(fObjectStorageURI, opContext, config);
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
	
	public String getBucketName() {
		return fBucketName;				
	}
	
	public String genServiceExtendedURI()  {
		String protocol = Utils.getProtocol(fObjectStorageURI);
		String authority = Utils.getBucket(fObjectStorageURI);
		if (protocol.equals(Constants.COS) &&  !authority.endsWith("." + Constants.DEFAULT_SERVICE_NAME)) {
			authority += "." + Constants.DEFAULT_SERVICE_NAME;
		}
				
		return protocol + Constants.PROTOCOL_URI_DELIM +  authority + Constants.URI_DELIM ;
	}

	@ContextCheck(compile = true)
	public static void checkCompileParameters(OperatorContextChecker checker)
			throws Exception {
		
		// there are two sets of authentication parameters
		// group 1: username + password 
		// group 2: IAMAPIKey + IAMServiceInstanceId + IAMTokenEndpoint
		
		
		checker.checkDependentParameters(IObjectStorageConstants.PARAM_OS_USER, 
										 IObjectStorageConstants.PARAM_OS_PASSWORD);
		
		checker.checkDependentParameters(IObjectStorageConstants.PARAM_IAM_APIKEY, 
										 IObjectStorageConstants.PARAM_IAM_SERVICE_INSTANCE_ID);
		
		// checks that there is no cross-correlation between parameters from different groups
		checker.checkExcludedParameters(IObjectStorageConstants.PARAM_OS_USER, IObjectStorageConstants.PARAM_IAM_APIKEY);
		checker.checkExcludedParameters(IObjectStorageConstants.PARAM_OS_USER, IObjectStorageConstants.PARAM_IAM_SERVICE_INSTANCE_ID);
		checker.checkExcludedParameters(IObjectStorageConstants.PARAM_OS_USER, IObjectStorageConstants.PARAM_IAM_TOKEN_ENDPOINT);
		checker.checkExcludedParameters(IObjectStorageConstants.PARAM_OS_PASSWORD, IObjectStorageConstants.PARAM_IAM_APIKEY);
		checker.checkExcludedParameters(IObjectStorageConstants.PARAM_OS_PASSWORD, IObjectStorageConstants.PARAM_IAM_SERVICE_INSTANCE_ID);
		checker.checkExcludedParameters(IObjectStorageConstants.PARAM_OS_PASSWORD, IObjectStorageConstants.PARAM_IAM_TOKEN_ENDPOINT);
	}
	
	public static final String AUTHENTICATION_DESC =
			"\\n"+
			"\\n+ Supported Authentication Schemes" +
			"\\n"+
			"\\nThe operator supports IBM Cloud Identity and Access Management (IAM) and HMAC for authentication."+
			"\\n"+
			"\\n# IAM authentication\\n"+
			"\\nFor IAM authentication the following authentication parameters should be used:"+
			"\\n* IAMApiKey\\n"+
			"\\n* IAMServiceInstanceId\\n"+
			"\\n* IAMTokenEndpoint - IAM token endpoint. The default is `https://iam.bluemix.net/oidc/token`.\\n"+			
		    "\\n"+
			"\\nThe following diagram demonstrates how `IAMApiKey` and `IAMServiceInstanceId` can be extracted "+ 
			"from the COS service credentials:\\n"+ 
			"\\n{../../doc/images/COSCredentialsOnCOSOperatorMapping.png}"+
		    "\\n"+	
		    "\\n# HMAC authentication\\n"+
		    "\\nFor HMAC authentication the following authentication parameters should be used:\\n"+
			"\\n* objectStorageUser\\n"+
			"\\n* objectStoragePassword\\n"+
			"\\n For S3-compliant COS use **AccessKeyID** for 'objectStorageUser' and **SecretAccessKey** for 'objectStoragePassword'." 
	        ;
}
