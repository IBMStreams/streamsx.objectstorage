package com.ibm.streamsx.objectstorage;

import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

@PrimitiveOperator(name = "ObjectStorageScan", namespace = "com.ibm.streamsx.objectstorage", description = "Java Operator ObjectScan")
@OutputPorts({
		@OutputPortSet(description = "Port that produces tuples", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating),
		@OutputPortSet(description = "Optional output ports", optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Libraries({"opt/*","opt/downloaded/*" })
@SharedLoader
public class ObjectStorageScan extends BaseObjectStorageScan implements IObjectStorageAuth {

	@Parameter(optional=true, description = "Specifies username for connection to a cloud object storage (AKA 'AccessKeyID' for S3-compliant COS).")
	public void setObjectStorageUser(String objectStorageUser) {
		super.setUserID(objectStorageUser);
	}
	
	public String getObjectStorageUser() {
		return super.getUserID();
	}
	
	@Parameter(optional=true, description = "Specifies password for connection to a cloud object storage (AKA 'SecretAccessKey' for S3-compliant COS).")
	public void setObjectStoragePassword(String objectStoragePassword) {
		super.setPassword(objectStoragePassword);
	}
	
	public String getObjectStoragePassword() {
		return super.getPassword();
	}
	
	
	@Parameter(optional=true, description = "Specifies project id for connection to object storage. The parameters is mandatory for Swift-compliant COS only.")
	public void setObjectStorageProjectID(String objectStorageProjectID) {
		super.setProjectID(objectStorageProjectID);
	}
	
	public String getObjectStorageProjectID() {
		return super.getProjectID();
	}
	
	@Parameter(optional=false, description = "Specifies URI for connection to object storage. For Swift-compliant COS the URI should be in 'swift2d://containerName/' format. For S3-compiant COS the URI should be in  's3d://bucket/ or s3a://bucket/' format.")
	public void setObjectStorageURI(String objectStorageURI) {
		super.setURI(objectStorageURI);;
	}
	
	public String getObjectStorageURI() {
		return super.getURI();
	}

	@Parameter(optional=false, description = "Specifies endpoint for connection to object storage. For example, for S3 the endpoint might be 's3.amazonaws.com'.")
	public void setEndpoint(String endpoint) {
		super.setEndpoint(endpoint);
	}

	@Parameter(optional=true, description = "Specifies IAM API Key. Relevant for IAM authentication case only.")
	public void setIAMApiKey(String iamApiKey) {
		super.setIAMApiKey(iamApiKey);
	}
	
	public String getIAMApiKey() {
		return super.getIAMApiKey();
	}
	
	@Parameter(optional=true, description = "Specifies IAM token endpoint. Relevant for IAM authentication case only.")
	public void setIAMTokenEndpoint(String iamTokenEndpoint) {
		super.setIAMTokenEndpoint(iamTokenEndpoint);;
	}
	
	public String getIAMTokenEndpoint() {
		return super.getIAMTokenEndpoint();
	}
	
	@Parameter(optional=true, description = "Specifies endpoint for connection to object storage. For example, for S3 the endpoint might be 's3.amazonaws.com'.")
	public void setIAMServiceInstanceId(String iamServiceInstanceId) {
		super.setIAMServiceInstanceId(iamServiceInstanceId);
	}
	
	public String getIAMServiceInstanceId() {
		return super.getIAMServiceInstanceId();
	}
}
