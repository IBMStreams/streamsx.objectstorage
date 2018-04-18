package com.ibm.streamsx.objectstorage.s3;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streamsx.objectstorage.BaseObjectStorageSource;
import com.ibm.streamsx.objectstorage.ObjectStorageSource;
import com.ibm.streamsx.objectstorage.Utils;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;


@PrimitiveOperator(name="S3ObjectStorageSource", namespace="com.ibm.streamsx.objectstorage.s3",
description=S3ObjectStorageSource.DESC+ObjectStorageSource.BASIC_DESC)
@InputPorts({@InputPortSet(description="The `S3ObjectStorageSource` operator has one optional input port. If an input port is specified, the operator expects an input tuple with a single attribute of type rstring. The input tuples contain the object names that the operator opens for reading. The input port is non-mutating.", cardinality=1, optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="The `S3ObjectStorageSource` operator has one output port. The tuples on the output port contain the data that is read from the objects. The operator supports two modes of reading.  To read an object line-by-line, the expected output schema of the output port is tuple<rstring line>. To read an object as binary, the expected output schema of the output port is tuple<blob data>. Use the blockSize parameter to control how much data to retrieve on each read. The operator includes a punctuation marker at the conclusion of each object.", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
@Libraries({"opt/*","opt/downloaded/*" })
@SharedLoader
public class S3ObjectStorageSource extends BaseObjectStorageSource  implements IS3ObjectStorageAuth {

	public static final String DESC = 
			"Operator reads objects from S3 compliant object storage.";	
	
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
