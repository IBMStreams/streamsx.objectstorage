/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/
package com.ibm.streamsx.objectstorage.client;

import org.apache.hadoop.conf.Configuration;

import com.ibm.streamsx.objectstorage.Utils;


public class ObjectStorageClientFactory {

	
	public static IObjectStorageClient getObjectStorageClient(String objectStorageURI, String objectStorageUser,
			String objectStoragePassword, String objectStorageProjectID, Configuration config) throws Exception {

		String protocol = Utils.getProtocol(objectStorageURI);

		switch (protocol.toLowerCase()) {
		case Constants.SWIFT2D:
			return new ObjectStorageSwiftClient(objectStorageURI, objectStorageUser, objectStoragePassword,
					objectStorageProjectID, config);
		case Constants.S3A:
			return new ObjectStorageS3AClient(objectStorageURI, objectStorageUser, objectStoragePassword, config);
		case Constants.COS:
			return new ObjectStorageCOSClient(objectStorageURI, objectStorageUser, objectStoragePassword, config);
		default:
			throw new IllegalArgumentException(
					"No Object Storage client implementation found for protocol '" + protocol.toLowerCase() + "'");
		}
	}
	
}
