package com.ibm.streamsx.objectstorage.test;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;

public class Constants {
	
	public static final String OBJECT_STORAGE_TEST_TOPOLOGY_NAME = "ObjectStorageSinkTestTopology";



	public static final java.util.logging.Level TRACE_LEVEL = java.util.logging.Level.FINEST;

	
	/**
	 * Object storage operator related constants
	 */
	public static final String OBJECT_STORAGE_TOOLKIT_NAME = "com.ibm.streamsx.objectstorage";
	public static final String OBJECT_STORAGE_SINK_OP_NS = "com.ibm.streamsx.objectstorage";
	public static final String OBJECT_STORAGE_SINK_OP_NAME = "ObjectStorageSink";
	public static final String INET_TOOLKIT_NAME = "com.ibm.streamsx.inet";
	public static final String INET_TOOLKIT_VERSION = "2.7.4";
	public static final String JSON_TOOLKIT_NAME = "com.ibm.streamsx.json";
	public static final String JSON_TOOLKIT_VERSION = "1.2.1";	
	public static final String TEST_DATA_FILE_DELIMITER = ",";
	
	public static final String STUDIO_TEST_PROPERTIES = "studio.test.properties";

	/**
	 * Object storage sink related constants
	 */
	public static final StreamSchema OS_SINK_OUT_SCHEMA = Type.Factory.getStreamSchema("tuple<rstring objectname, uint64 size>");

	/**
	 * The pathes are relative to the repository root
	 */
	public static final String PROJECT_TEST_ROOT_RELPATH = "/test/java/com.ibm.streamsx.objectstorage.test";	
	public static final String OS_REMOTE_CLIENT_CRED_DIR_RELPATH = PROJECT_TEST_ROOT_RELPATH + "/etc";
	public static final String S3_REMOTE_CLIENT_CRED_FILENAME = "s3_objectstore_credentials.json";
	public static final String SWIFT_REMOTE_CLIENT_CRED_FILENAME = "swift_objectstore_credentials.json";
	public static final String OS_SINGLE_ATTR_TEST_OBJECT_NAME = "singleAttr.csv";
	public static final String OS_MULTI_ATTR_TEST_OBJECT_NAME = "multiAttr.csv";
	public static final String OS_TEST_LINE_DEL = ",";
	public static final String OS_DEFAULT_TEST_DATA_FILE_RELPATH = PROJECT_TEST_ROOT_RELPATH + "/data";

	/**
	 * Test life cycle composite names - mostly used during test configuration/initialization stage
	 */
	public static final String DATA_INJECTION_STEP = "InjectionStep";
	
	/**
	 * Topology tester types
	 */
	public static final String STANDALONE = "STANDALONE_TESTER";
	public static final String DISTRIBUTED = "DISTRIBUTED_TESTER";
	public static final String EMBEDDED = "EMBEDDED_TESTER";


	public static final String CREDENTIALS_FILE_SUFFIX = "objectstore_credentials.json";
	
	/**
	 * Protocols to test
	 */
	public static final String SWIFT2D = "swift2d";
	public static final String COS = "cos";
	public static final String S3A = "s3a";
	public static final String FILE = "file";

	/**
	 * Default test bucket/container name
	 */
	public static final String DEFAULT_BUCKET_NAME = "ost-test-bucket3";
	public static final String DEFAULT_CONTAINER_NAME = "ost-test-container";
	public static final String DEFAULT_IAM_BUCKET_NAME = "ostiam-test-bucket3";
	public static final String FILE_DEFAULT_BUCKET_NAME = "";	
	

	/**
	 * Sink operator parameter names
	 */
	public static final String SINK_STORAGE_FORMAT_PARAM_NAME = "storageFormat";
	public static final String RAW_STORAGE_FORMAT = "raw";
	public static final String PARQUET_STORAGE_FORMAT = "parquet";
	public static final String DEFAULT_STORAGE_FORMAT = Constants.RAW_STORAGE_FORMAT;
	
}


