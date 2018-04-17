package com.ibm.streamsx.objectstorage;

import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

@PrimitiveOperator(name = "ObjectStorageScan", namespace = "com.ibm.streamsx.objectstorage",
description=ObjectStorageScan.DESC+ObjectStorageScan.BASIC_DESC)
@InputPorts({@InputPortSet(description="Port that ingests control tuples to set the directory to be scanned", cardinality=1, optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({
		@OutputPortSet(description = "Port that produces tuples", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating),
		@OutputPortSet(description = "Optional output ports", optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Libraries({"opt/*","opt/downloaded/*" })
@SharedLoader
public class ObjectStorageScan extends BaseObjectStorageScan implements IObjectStorageAuth {

	public static final String DESC = 
			"Operator scans for specified key name pattern on a object storage. The operator supports basic (user/password) and IAM authentication.\\n" +
			"\\nThe `ObjectStorageScan` is similar to the `DirectoryScan` operator. "+
			"The `ObjectStorageScan` operator repeatedly scans an object storage directory and writes the names of new or modified files " +
			"that are found in the directory to the output port. The operator sleeps between scans.";
	
	public static final String BASIC_DESC = 					
			"\\n"+
			"\\n# Behavior in a consistent region\\n" +
			"\\nThe operator can participate in a consistent region. " +
			"The operator can be at the start of a consistent region if there is no input port.\\n" +
			"\\nThe operator supports periodic and operator-driven consistent region policies. " +
			"If consistent region policy is set as operator driven, the operator initiates a drain after each tuple is submitted. " +
			"\\nThis allows for a consistent state to be established after a object is fully processed.\\n" +
			"If the consistent region policy is set as periodic, the operator respects the period setting and establishes consistent states accordingly. " +
			"This means that multiple objects can be processed before a consistent state is established.\\n" +
			"\\nAt checkpoint, the operator saves the last submitted object name and its modification timestamp to the checkpoint." +
			"\\nUpon application failures, the operator resubmits all objects that are newer than the last submitted object at checkpoint."
		   	;	
	
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
	
	
	public String getObjectStorageProjectID() {
		return super.getProjectID();
	}
	
	@Parameter(optional=false, description = "Specifies URI for connection to object storage. For S3-compliant COS the URI should be in 'cos://bucket/ or s3a://bucket/' format.")
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
