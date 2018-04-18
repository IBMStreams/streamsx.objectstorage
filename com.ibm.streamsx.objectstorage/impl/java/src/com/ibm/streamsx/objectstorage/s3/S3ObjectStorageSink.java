package com.ibm.streamsx.objectstorage.s3;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.objectstorage.BaseObjectStorageSink;
import com.ibm.streamsx.objectstorage.Utils;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streamsx.objectstorage.ObjectStorageSink;

@PrimitiveOperator(name="S3ObjectStorageSink", namespace="com.ibm.streamsx.objectstorage.s3",
description=S3ObjectStorageSink.DESC+ObjectStorageSink.BASIC_DESC)
@InputPorts({@InputPortSet(description="The `S3ObjectStorageSink` operator has one input port, which writes the contents of the input stream to the object that you specified. The `S3ObjectStorageSink` supports writing data into object storage in two formats. For line format, the schema of the input port is tuple<rstring line>, which specifies a single rstring attribute that represents a line to be written to the object. For binary format, the schema of the input port is tuple<blob data>, which specifies a block of data to be written to the object.", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="The `S3ObjectStorageSink` operator is configurable with an optional output port. The schema of the output port is <rstring objectName, uint64 objectSize>, which specifies the name and size of objects that are written to object storage.", cardinality=1, optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
@Libraries({"opt/*","opt/downloaded/*" })
@SharedLoader
public class S3ObjectStorageSink extends BaseObjectStorageSink implements IS3ObjectStorageAuth {
	
	public static final String DESC = 
			"Operator writes objects to S3 compliant object storage.";
	
	private String fAccessKeyID;
	private String fsecretAccessKey;
	private String fBucket;
	private S3Protocol fProtocol = S3Protocol.s3a;	
	
	@Override
	public void initialize(OperatorContext context) throws Exception {
		setURI(Utils.getObjectStorageS3URI(getProtocol(), getBucket()));
		setUserID(getAccessKeyID());
		setPassword(getSecretAccessKey());
		setEndpoint((getEndpoint() == null) ? Constants.S3_DEFAULT_ENDPOINT : getEndpoint());
		super.initialize(context);
	}
	
	@Parameter
	public void setAccessKeyID(String accessKeyID) {
		fAccessKeyID = accessKeyID;
	}

	
	public String getAccessKeyID() {
		return fAccessKeyID;
	}

	@Parameter
	public void setSecretAccessKey(String secretAccessKey) {
		fsecretAccessKey = secretAccessKey;
	}

	
	public String getSecretAccessKey() {
		return fsecretAccessKey;
	}

	@Parameter
	public void setBucket(String bucket) {
		fBucket = bucket;
		
	}

	
	public String getBucket() {
		return fBucket;
	}

	@Parameter(optional = true, description = "Specifies protocol to use for communication with COS. Supported values are s3a and cos. The default value is s3a.")
	public void setProtocol(S3Protocol protocol) {
		fProtocol = protocol;		
	}
	
	public S3Protocol getProtocol() {
		return fProtocol;
	}

	@Parameter(optional=true)
	public void setEndpoint(String endpoint) {
		super.setEndpoint(endpoint);
	}

}
