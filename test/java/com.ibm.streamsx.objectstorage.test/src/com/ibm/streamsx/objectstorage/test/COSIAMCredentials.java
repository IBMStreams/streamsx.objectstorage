package com.ibm.streamsx.objectstorage.test;

/**
 * COS Credentials POJO 
 * @author streamsadmin
 *
 */
public class COSIAMCredentials implements Credentials {	
	private String endpoint;
	
	
	public COSIAMCredentials(String endpoint) {
		this.endpoint = endpoint;
	}
	
	public String getEndpoint() {
		return endpoint;
	}
	
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	
	public String getUserId() {
		return null;
	}
	
	public String getPassword() {
		return null;
	}

	public String getProjectId() {
		return "";
	}

}
