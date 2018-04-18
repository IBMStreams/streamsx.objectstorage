package com.ibm.streamsx.objectstorage;

import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

@PrimitiveOperator(name="ObjectStorageSink", namespace="com.ibm.streamsx.objectstorage",
description=ObjectStorageSink.DESC+ObjectStorageSink.BASIC_DESC)
@InputPorts({@InputPortSet(description="The `ObjectStorageSink` operator has one input port, which writes the contents of the input stream to the object that you specified. The `ObjectStorageSink` supports writing data into object storage in two formats. For line format, the schema of the input port is tuple<rstring line>, which specifies a single rstring attribute that represents a line to be written to the object. For binary format, the schema of the input port is tuple<blob data>, which specifies a block of data to be written to the object.", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="The `ObjectStorageSink` operator is configurable with an optional output port. The schema of the output port is <rstring objectName, uint64 objectSize>, which specifies the name and size of objects that are written to object storage.", cardinality=1, optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
@Libraries({"opt/*","opt/downloaded/*" })
public class ObjectStorageSink extends BaseObjectStorageSink implements IObjectStorageAuth {
	
	public static final String DESC = 
			"Operator writes objects to S3 compliant object storage. The operator supports basic (user/password) and IAM authentication.";
	
	public static final String BASIC_DESC =
			"\\n"+
			"\\nThis operator writes tuples that arrive on its input port to the output object that is named by the **objectName** parameter. "+
			"You can optionally control whether the operator closes the current output object and creates a new object for writing based on the size"+ 
			"of the object in bytes, the number of tuples that are written to the object, or the time in seconds that the object is open for writing, "+
			"or when the operator receives a punctuation marker."+
			"\\n"
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
	
	@Parameter(optional=false, description = "Specifies URI for connection to object storage. For S3-compliant COS the URI should be in  'cos://bucket/ or s3a://bucket/' format.")
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
