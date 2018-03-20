package com.ibm.streamsx.objectstorage.test;

/**
 * COS Credentials POJO 
 * @author streamsadmin
 *
 */
public class COSBasicCredentials implements Credentials {	
	private String endpoint;
	private String userId;
	private String password;
		
	public COSBasicCredentials(String endpoint, String userId, String password) {
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

	@Override
	public String getIAMApiKey() {
		return null;
	}

	@Override
	public String getIAMServiceInstanceId() {
		return null;
	}

	@Override
	public String getIAMTokenEndpoint() {
		return null;
	}

//	public static Credentials getMockCredentials() {
//		return new COSCredentials(MOCK_ENDPOINT, MOCK_USERID, MOCK_PASSWD);
//	}	
}
