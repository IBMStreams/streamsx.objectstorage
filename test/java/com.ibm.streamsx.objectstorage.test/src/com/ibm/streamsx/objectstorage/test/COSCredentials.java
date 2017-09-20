package com.ibm.streamsx.objectstorage.test;

/**
 * COS Credentials POJO 
 * @author streamsadmin
 *
 */
public class COSCredentials implements Credentials {	
	private String endpoint;
	private String userId;
	private String password;
	
	private static final String MOCK_ENDPOINT = "localhost:" + Constants.MOCK_PORT;
	private static final String MOCK_USERID = "myuserid";
	private static final String MOCK_PASSWD = "mypwd";
	
	public COSCredentials(String endpoint, String userId, String password) {
		this.endpoint = endpoint;
		this.userId = userId;
		this.password = password;
	}
	
	public String getEndpoint() {
		return endpoint;
	}
	
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public String getProjectId() {
		return "";
	}

//	public static Credentials getMockCredentials() {
//		return new COSCredentials(MOCK_ENDPOINT, MOCK_USERID, MOCK_PASSWD);
//	}	
}
