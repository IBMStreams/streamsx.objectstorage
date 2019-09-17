package com.ibm.streamsx.objectstorage.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.tester.Tester;
import com.ibm.streamsx.topology.tester.junit.AbstractTestClass;
import com.ibm.streamsx.topology.tester.junit.TestProperties;



/**
 * Abstract object storage toolkit test case class 
 * @author streamsadmin
 *
 */
public abstract class AbstractObjectStorageTest extends AbstractTestClass {
	
	
	private Credentials _credentials;	
	protected Topology _testTopology;	
	protected Tester _tester;
	protected String _protocol;
	protected String _bucket;	
	
	/**
	 * Pathes
	 */
	protected String _projectRootAbsPath = null; // absolute test project root path
	protected String _remoteClientCredAbsPath = null; // absolute remote client credentials path
	protected String _osDefaultTestDataFileAbsPath = null; // absolute default test data file path
	protected String _testDataFileName = Constants.OS_SINGLE_ATTR_TEST_OBJECT_NAME; // test data file name
	protected String _projectTestRootAbsPath = null; // absolute test project path
	
	/**
	 * Test configuration
	 */
	protected Map<String, Object> _testConfiguration = new HashMap<String, Object>();
	
	public AbstractObjectStorageTest() {
		// initialize test topology		
		_testTopology = new Topology();
		_tester = _testTopology.getTester();
		// "hadoop.home.dir" must be defined to avoid exception
		System.setProperty("hadoop.home.dir", "/tmp");
	}

	public void build(String testName, TraceLevel logLevel, String topologyType, String protocol, AuthenticationMode authMode, String bucket) throws Exception {		
		
		// initializes absolute paths for a test
		_projectRootAbsPath = Utils.getTestRoot() + ""; // current folder
		_projectTestRootAbsPath = _projectRootAbsPath + Constants.PROJECT_TEST_ROOT_RELPATH; // test project path
		_osDefaultTestDataFileAbsPath =  _projectRootAbsPath + Constants.OS_DEFAULT_TEST_DATA_FILE_RELPATH; // default input file path
		_remoteClientCredAbsPath = _projectRootAbsPath + Constants.OS_REMOTE_CLIENT_CRED_DIR_RELPATH; // remote os credentials file path		
		_protocol = protocol;
		_bucket = bucket;
		
		
		// adds object storage toolkit to the test topology
		String tk = System.getProperty("objectstorage.toolkit.release");
		if (null == tk) {
			SPL.addToolkit(_testTopology, new File(_projectRootAbsPath, Constants.OBJECT_STORAGE_TOOLKIT_NAME));
		}
		else {
			SPL.addToolkit(_testTopology, new File(tk)); 
		}
		setTopologyType(topologyType); // set test topology type
		setLoggerLevel(logLevel); // set test logger level
	 
		genObjectStorageConnectionParams(_testConfiguration, authMode); // set object storage connection params
		//genTestSpecificParams(_testConfiguration); // set test specific params	
	}
	

	/**
	 * Contains test configuration
	 * @throws UnsupportedEncodingException 
	 * @throws IOException 
	 */	
	public abstract void genTestSpecificParams(Map<String, Object> params) throws UnsupportedEncodingException;
	public abstract int getTestTimeout(); // test runtime
	public abstract void validateResults(SPLStream osSink, String protocol, String storageFormat, Integer expectedCnt) throws Exception;
	
	public String getEndpoint() {
		return _credentials.getEndpoint();
	}

	public String getUserId() {
		return _credentials.getUserId();
	}

	public String getPassword() {
		return _credentials.getPassword();
	}

	public String getProjectId() {
		return _credentials.getProjectId();
	}
	
	/**
	 * Sets test logger level
	 * @param level
	 */
	public void setLoggerLevel(TraceLevel level) {
		//Topology.STREAMS_LOGGER.setLevel(level);		
		//Topology.TOPOLOGY_LOGGER.setLevel(level);  
		getConfig().put(ContextProperties.TRACING_LEVEL, java.util.logging.Level.FINE);		
		System.out.println("Streams topology logger level: " + Topology.TOPOLOGY_LOGGER.getLevel());
		
	}
	
	/**
	 * Sets topology type the test uses 
	 * (distributed, standalone or embedded)
	 * @param topologyType
	 */
	public void setTopologyType(String topologyType) {
		System.setProperty(TestProperties.TESTER_TYPE, topologyType);
		setTesterType();
	}
	
	public void genObjectStorageConnectionParams(Map<String, Object> params, AuthenticationMode authMode) throws Exception {				
		String osUri = Utils.buildBaseURI(_protocol, _bucket);
		params.put("objectStorageURI", osUri);

		// load credentials relevant for the test (by protocol)
		String protocol = Utils.getProtocol(osUri);
		initCredentials(protocol, authMode);
		
		params.put("endpoint", getEndpoint());
		switch (authMode) {
			case BASIC: 
				params.put("objectStorageUser", getUserId());
				params.put("objectStoragePassword", getPassword());
				params.put("objectStorageProjectID", getProjectId());			
				break;
			case IAM:		
				break;
			case NONE:	
				break;
			default:
				throw new Exception("Can't populate operator authentication parametrization for authentication mode '" + authMode + "'");
		}
			
		
	}

	private String getRemoteCredFileName(String protocol, AuthenticationMode authMode) {
		return "etc/" + protocol + "_" + authMode.toString().toLowerCase() + "_" + Constants.CREDENTIALS_FILE_SUFFIX;
	}
	
	
	private void initCredentials(String protocol, AuthenticationMode authMode) throws Exception {

		Gson gson = new Gson();
		String credentialsFile = getRemoteCredFileName(protocol, authMode);
		
		switch (authMode) {
		case BASIC:
			_credentials = protocol.equals(Constants.SWIFT2D) ?
					   gson.fromJson(new JsonReader(new FileReader(credentialsFile)), SwiftBasicCredentials.class) :
		  			   gson.fromJson(new JsonReader(new FileReader(credentialsFile)), COSBasicCredentials.class);														
			System.out.println("Credentials loaded from file '" + credentialsFile + "' for protocol '" + protocol + "' and authentication mode '" + authMode + "' are " +   gson.toJson(_credentials));
			break;
		case IAM: 
			if (protocol.equals(Constants.SWIFT2D)) {
				throw new Exception("IAM authentication method is not supported for swift protocol");
			}			
			_credentials =  gson.fromJson(new JsonReader(new FileReader(credentialsFile)), COSIAMCredentials.class);														
			System.out.println("Credentials loaded from file '" + credentialsFile + "' for protocol '" + protocol + "' and authentication mode '" + authMode + "' are " +   gson.toJson(_credentials));
			break;
		case NONE: // empty credentials
			_credentials = new COSIAMCredentials("");
			System.out.println("Empty credentials are used for protocol '" + protocol + "' and authentication mode '" + authMode + ": '" +   gson.toJson(_credentials) + "'");
			break;
		default:
			throw new Exception("Can't find credentials for authentication mode '" + authMode + "'");
		}
	}
	

	  
}
