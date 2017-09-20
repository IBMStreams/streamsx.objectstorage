package com.ibm.streamsx.objectstorage.test;

/**
 * Swift Credentials POJO
 * @author streamsadmin
 *
 */
public class SwiftCredentials implements Credentials {	
	private String auth_url;
	private String userId;
	private String password;
	private String projectId;
	private String region;
	
	private final static String SWIFT_ENDPOINT = "dal.objectstorage.open.softlayer.com"; 
	
	
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
	
	public String getProjectId() {
		return projectId;
	}
	
	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getAuthUrl() {
		return auth_url;
	}
	public void setAuthUrl(String auth_url) {
		this.auth_url = auth_url;
	}

	public String getEndpoint() {
		return SWIFT_ENDPOINT;
	}

	//	public static Credentials getMockCredentials() {
//		// TODO Auto-generated method stub
//		return null;
//	}

}
