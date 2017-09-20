/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/
package com.ibm.streamsx.objectstorage.client;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;

import com.ibm.streamsx.objectstorage.Utils;


public class ObjectStorageCOSClient extends ObjectStorageAbstractClient {

	
	public ObjectStorageCOSClient(String objectStorageURI, String objectStorageUser, String objectStoragePassword) throws Exception {
		super(objectStorageURI, objectStorageUser, objectStoragePassword);
	}

	public ObjectStorageCOSClient(String objectStorageURI, String objectStorageUser, String objectStoragePassword, Configuration config) throws Exception {
		super(objectStorageURI, objectStorageUser, objectStoragePassword, config);
	}
	
	
	@Override
	public void initClientConfig() throws IOException, URISyntaxException {
				
		String protocol = Utils.getProtocol(fObjectStorageURI);
		
		// initialize COS specific properties
		fConnectionProperties.set(Constants.COS_FS_IMPL_CONFIG_NAME, Constants.STOCATOR_DEFAULT_FS_IMPL);
		fConnectionProperties.setIfUnset(Constants.COS_SERVICE_ACCESS_KEY_CONFIG_NAME, fObjectStorageUser);
		fConnectionProperties.setIfUnset(Constants.COS_SERVICE_SECRET_KEY_CONFIG_NAME, fObjectStoragePassword);			
		fConnectionProperties.setIfUnset(Constants.COS_CLIENT_EXECUTION_TIMEOUT_CONFIG_NAME, Constants.COS_CLIENT_EXECUTION_TIMEOUT);		
		fConnectionProperties.setIfUnset(Constants.COS_CLIENT_IMPL_CONFIG_NAME,  Constants.COS_SERVICE_CLIENT);
		fConnectionProperties.setIfUnset(Constants.COS_SCHEME_CONFIG_NAME, protocol);
		
		fConnectionProperties.setIfUnset(Constants.S3_SERVICE_ENDPOINT_CONFIG_NAME, Constants.S3_DEFAULT_ENDPOINT);
		fConnectionProperties.setIfUnset(Utils.formatProperty(Constants.S3_MULTIPART_CONFIG_NAME, protocol), Constants.S3_MULTIPATH_SIZE);
		// the default setting for this param is not numeric (100M) whhich causes to parser failure.
		// keeps its value numeric for parameter fallback working.
		fConnectionProperties.set(Constants.S3A_MULTIPART_CONFIG_NAME, Constants.S3_MULTIPATH_SIZE);
		
		fConnectionProperties.setIfUnset(Constants.SOCKET_TIMEOUT_CONFIG_NAME, Constants.S3_DEFAULT_SOCKET_TIMEOUT);
		fConnectionProperties.setIfUnset(Constants.REQ_LEVEL_CONNECT_TIMEOUT_CONFIG_NAME, Constants.S3_REQ_LEVEL_DEFAULT_SOCKET_TIMEOUT);
		fConnectionProperties.setIfUnset(Constants.CONNECTION_TIMEOUT_CONFIG_NAME, Constants.S3_CONNECTION_TIMEOUT);
		fConnectionProperties.setIfUnset(Constants.REQ_SOCKET_TIMEOUT_CONFIG_NAME, Constants.S3_REQ_SOCKET_TIMEOUT);
	}


}
