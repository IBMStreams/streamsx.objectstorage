/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/
package com.ibm.streamsx.objectstorage.client;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.Utils;
import com.ibm.streamsx.objectstorage.auth.OSAuthenticationHelper;


public class ObjectStorageFileClient extends ObjectStorageAbstractClient   {

	private static Logger TRACE = Logger.getLogger(ObjectStorageFileClient.class.getName());

	public ObjectStorageFileClient(String objectStorageURI, OperatorContext opContext) throws Exception {
		super(objectStorageURI, opContext);
	}

	public ObjectStorageFileClient(String objectStorageURI, OperatorContext opContext, Configuration config) throws Exception {
		super(objectStorageURI, opContext, config);
	}
	
	
	@Override
	public void initClientConfig() throws IOException, URISyntaxException {

		String protocol = Utils.getProtocol(fObjectStorageURI);

		// configure authentication related properties
		OSAuthenticationHelper.configAuthProperties(protocol, fOpContext, fConnectionProperties);
		
		// initialize COS specific properties
		fConnectionProperties.set(Constants.S3A_IMPL_CONFIG_NAME, Constants.LOCAL_DEFAULT_FS_IMPL);

//		fConnectionProperties.setIfUnset(Constants.COS_CLIENT_EXECUTION_TIMEOUT_CONFIG_NAME, Constants.COS_CLIENT_EXECUTION_TIMEOUT);		
//		fConnectionProperties.setIfUnset(Constants.COS_CLIENT_IMPL_CONFIG_NAME,  Constants.COS_SERVICE_CLIENT);
//		fConnectionProperties.setIfUnset(Constants.COS_SCHEME_CONFIG_NAME, protocol);
//		
//		fConnectionProperties.setIfUnset(Utils.formatProperty(Constants.S3_MULTIPART_CONFIG_NAME, protocol), Constants.S3_MULTIPATH_SIZE);
//		// the default setting for this param is not numeric (100M) whhich causes to parser failure.
//		// keeps its value numeric for parameter fallback working.
//		fConnectionProperties.set(Constants.S3A_MULTIPART_CONFIG_NAME, Constants.S3_MULTIPATH_SIZE);
//		
//		fConnectionProperties.setIfUnset(Constants.SOCKET_TIMEOUT_CONFIG_NAME, Constants.S3_DEFAULT_SOCKET_TIMEOUT);
//		fConnectionProperties.setIfUnset(Constants.REQ_LEVEL_CONNECT_TIMEOUT_CONFIG_NAME, Constants.S3_REQ_LEVEL_DEFAULT_SOCKET_TIMEOUT);
//		fConnectionProperties.setIfUnset(Constants.CONNECTION_TIMEOUT_CONFIG_NAME, Constants.S3_CONNECTION_TIMEOUT);
//		fConnectionProperties.setIfUnset(Constants.REQ_SOCKET_TIMEOUT_CONFIG_NAME, Constants.S3_REQ_SOCKET_TIMEOUT);
	}

	
	@Override
	public void connect() throws Exception {
		initClientConfig();
		
	    fFileSystem = new org.apache.hadoop.fs.LocalFileSystem();
//		String formattedPropertyName = Utils.formatProperty(Constants.S3_SERVICE_ENDPOINT_CONFIG_NAME, Utils.getProtocol(fObjectStorageURI));
//		String endpoint = fConnectionProperties.get(formattedPropertyName);
//		TRACE.log(TraceLevel.INFO, "About to initialize object storage file system with endpoint '" + endpoint  + "'. Use configuration property '" + formattedPropertyName + "' to update it if required.");
	    fFileSystem.initialize(new URI(fObjectStorageURI), fConnectionProperties);	
	    
	    TRACE.log(TraceLevel.INFO, "Object storage client initialized with configuration: \n");
	    for (Map.Entry<String, String> entry : fConnectionProperties) {
            TRACE.log(TraceLevel.INFO, entry.getKey() + " = " + entry.getValue());
        }
	}

}
