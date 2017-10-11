package com.ibm.streamsx.objectstorage.test;

/**
 * COS Credentials POJO 
 * @author streamsadmin
 *
 */
public class COSIAMCredentials implements Credentials {	
	private String endpoint;
	private String apikey;
	private String serviceInstanceId;
	private String tokenEndpoint;
	
	
	public COSIAMCredentials(String endpoint, String apikey, String serviceInstanceId, String tokenEndpoint) {
		this.endpoint = endpoint;
		this.apikey = apikey;
		this.serviceInstanceId = serviceInstanceId;
		this.tokenEndpoint = tokenEndpoint;		
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

	@Override
	public String getIAMApiKey() {
		return this.apikey;
	}

	@Override
	public String getIAMServiceInstanceId() {
		return this.serviceInstanceId;
	}

	@Override
	public String getIAMTokenEndpoint() {
		return this.tokenEndpoint;
	}
}
