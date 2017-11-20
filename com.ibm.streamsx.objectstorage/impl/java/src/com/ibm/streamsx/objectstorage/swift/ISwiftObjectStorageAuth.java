package com.ibm.streamsx.objectstorage.swift;

/**
 * Swift-specific authentication scheme interface
 * @author streamsadmin
 *
 */
public interface ISwiftObjectStorageAuth {

	public void setUserID(String userID);
		
	public void setPassword(String password);
	
	public void setAccessPoint(String accessPoint);

	public void setProjectID(String projectID);
	
	public String getProjectID();	

	public void setContainer(String container);
	
	public String getContainer();
	
}
