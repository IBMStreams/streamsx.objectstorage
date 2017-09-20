/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.objectstorage.client;

public class Constants {	
	
	// Generic configuration options
	public static final String HADOOP_HOME_DIR_CONFIG_NAME = "hadoop.home.dir";
	public static final String HADOOP_HOME_DIR_DEFAULT = "/tmp";
	public static final String STOCATOR_DEFAULT_FS_IMPL = "com.ibm.stocator.fs.ObjectStoreFileSystem";
	
	// Swift specific stocator configuration options
	public static final String SWIFT_IS_PUBLIC_CONFIG_NAME = "fs.swift2d.service." + Constants.DEFAULT_SERVICE_NAME + ".public";
	public static final String SWIFT_AUTH_URL_CONFIG_NAME = "fs.swift2d.service." + Constants.DEFAULT_SERVICE_NAME + ".auth.url";
	public static final String SWIFT_PROJECT_ID_CONFIG_NAME = "fs.swift2d.service." + Constants.DEFAULT_SERVICE_NAME + ".tenant";
	public static final String SWIFT_PASSWORD_CONFIG_NAME = "fs.swift2d.service." + Constants.DEFAULT_SERVICE_NAME + ".password";
	public static final String SWIFT_USERNAME_CONFIG_NAME = "fs.swift2d.service." + Constants.DEFAULT_SERVICE_NAME + ".username";
	public static final String SWIFT_AUTH_METHOD_CONFIG_NAME = "fs.swift2d.service." + Constants.DEFAULT_SERVICE_NAME + ".auth.method";
	public static final String SWIFT_REGION_CONFIG_NAME = "fs.swift2d.service." + Constants.DEFAULT_SERVICE_NAME + ".region";
	public static final String SWIFT_NON_STREAMING_UPLOAD_CONFIG_NAME = "fs.swift2d.service." + Constants.DEFAULT_SERVICE_NAME + ".nonstreaming.upload";
	public static final String SWIFT_DEFAULT_SOCKET_TIMEOUT = "60000";
	public static final String SWIFT_REQ_LEVEL_DEFAULT_SOCKET_TIMEOUT = "60000";
	public static final String SWIFT_CONNECTION_TIMEOUT = "60000";
	public static final String SWIFT_REQ_SOCKET_TIMEOUT = "60000";
	public static final String SWIFT_AUTH_URL = "https://identity.open.softlayer.com/v3/auth/tokens";
	public static final String SWIFT_AUTH_METHOD = "keystoneV3";
	public static final String SWIFT_DEFAULT_REGION = "dallas";
	public static final String SWIFT_DEFAULT_ENDPOINT = "dal.objectstorage.open.softlayer.com";
	public static final String SWIFT2D = "swift2d";
	public static final String SWIFT_FS_IMPL_CONFIG_NAME = "fs." + SWIFT2D + ".impl";

	// COS specific  configuration options
	public static final String COS_SERVICE_ACCESS_KEY_CONFIG_NAME = "fs.cos." + Constants.DEFAULT_SERVICE_NAME +  ".access.key"; 
	public static final String COS_SERVICE_SECRET_KEY_CONFIG_NAME = "fs.cos." + Constants.DEFAULT_SERVICE_NAME + ".secret.key";
	public static final String COS_SERVICE_CLIENT = "com.ibm.stocator.fs.cos.COSAPIClient"; 
	public static final String COS_CLIENT_EXECUTION_TIMEOUT_CONFIG_NAME = "fs.cos.client.execution.timeout";
	public static final String COS_CLIENT_IMPL_CONFIG_NAME = "fs.stocator.cos.impl";
	public static final String COS_FS_IMPL_CONFIG_NAME = "fs.cos.impl";
	public static final String COS_SCHEME_CONFIG_NAME = "fs.stocator.cos.scheme";		
	public static final String COS_CLIENT_EXECUTION_TIMEOUT = "600000";
	public static final String COS = "cos";

	// S3A specific  configuration options
	public static final String S3A_SERVICE_ACCESS_KEY_CONFIG_NAME = "fs.s3a.access.key"; 
	public static final String S3A_SERVICE_SECRET_KEY_CONFIG_NAME = "fs.s3a.secret.key";	
	public static final String S3A_PATH_STYLE_ACCESS_CONFIG_NAME = "fs.s3a.path.style.access";
	public static final String S3A_FAST_UPLOAD_ENABLE_CONFIG_NAME = "fs.s3a.fast.upload";
	public static final String S3A_FAST_UPLOAD_BUFFER_CONFIG_NAME = "fs.s3a.fast.upload.buffer";
	public static final String S3A_SIGNING_ALGORITHM_CONFIG_NAME = "fs.s3a.signing-algorithm";
	public static final String S3A_MULTIPART_CONFIG_NAME = "fs.s3a.multipart.size";
	public static final String S3A_IMPL_CONFIG_NAME = "fs.s3a.impl";	
	public static final String S3A = "s3a";
	
	// S3 multiprotocol configuration options
	public static final String S3_SERVICE_ENDPOINT_CONFIG_NAME = "fs.%s.streams-service.endpoint"; 
	public static final String S3_ENDPOINT_CONFIG_NAME = "fs.%s.endpoint";
	public static final String S3_SERVICE_SIGNER_TYPE_CONFIG_NAME = "fs.%s.v2.streams-service.signer.type";
	public static final String S3_MULTIPART_CONFIG_NAME = "fs.%s.multipart.size";
	public static final String S3_SERVICE_CREATE_BUCKET_CONFIG_NAME = "fs.%s.streams-service.create.bucket";
	public static final String S3_DEFAULT_ENDPOINT = "s3-api.us-geo.objectstorage.softlayer.net";
	public static final String S3_DEFAULT_PROTOCOL = S3A;
	public static final String S3_MULTIPATH_SIZE = "104857600";			
	public static final String S3_DEFAULT_SOCKET_TIMEOUT = "600000";
	public static final String S3_REQ_LEVEL_DEFAULT_SOCKET_TIMEOUT = "600000";
	public static final String S3_CONNECTION_TIMEOUT = "600000";
	public static final String S3_REQ_SOCKET_TIMEOUT = "600000";
	public static final String S3A_DEFAULT_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem";
	
	// low level socket timeout in milliseconds
	public static final String SOCKET_TIMEOUT_CONFIG_NAME = "fs.stocator.SoTimeout";
	public static final String REQ_LEVEL_CONNECT_TIMEOUT_CONFIG_NAME = "fs.stocator.ReqConnectTimeout";
	public static final String CONNECTION_TIMEOUT_CONFIG_NAME = "fs.stocator.ReqConnectionRequestTimeout";
	public static final String REQ_SOCKET_TIMEOUT_CONFIG_NAME = "fs.stocator.ReqSocketTimeout";
	public static final String EXECUTION_COUNT_NAME =  "fs.stocator.executionCount";
	
	// authentication related param names
	public final static String AUTH_PRINCIPAL = "authPrincipal";
	public final static String AUTH_KEYTAB = "authKeytab";
	public final static String CRED_FILE = "credFile";
	
	// generic (vs. protocol specific) params	
	public static final String SUPPORTED_SCHEME_LIST_CONFIG_NAME = "fs.stocator.scheme.list";
	public static final String[] SUPPORTED_SCHEME_LIST  =  {SWIFT2D, COS, S3A};
	
	public static final int DATA_OUTPUT_STREAM_BUFFER_SIZE = 1024 * 256;
	
	public static final String PROTOCOL_URI_DELIM = "://";
	public static final String URI_DELIM = "/";
	public static final String DEFAULT_SERVICE_NAME  = "streams-service";
}
