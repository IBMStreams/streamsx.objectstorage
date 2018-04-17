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
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streamsx.objectstorage.BaseObjectStorageScan;
import com.ibm.streamsx.objectstorage.ObjectStorageScan;
import com.ibm.streamsx.objectstorage.Utils;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

@PrimitiveOperator(name = "S3ObjectStorageScan", namespace = "com.ibm.streamsx.objectstorage.s3",
description=S3ObjectStorageScan.DESC+ObjectStorageScan.BASIC_DESC)
@InputPorts({@InputPortSet(description="Port that ingests control tuples to set the directory to be scanned", cardinality=1, optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({
		@OutputPortSet(description = "Port that produces tuples", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating),
		@OutputPortSet(description = "Optional output ports", optional = true, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
@Libraries({"opt/*","opt/downloaded/*" })
@SharedLoader
public class S3ObjectStorageScan extends BaseObjectStorageScan implements IS3ObjectStorageAuth {

	public static final String DESC = 
			"Operator scans for specified key name pattern on a object storage.\\n" +
			"\\nThe `S3ObjectStorageScan` is similar to the `DirectoryScan` operator. "+
			"The `S3ObjectStorageScan` operator repeatedly scans an object storage directory and writes the names of new or modified files " +
			"that are found in the directory to the output port. The operator sleeps between scans.";	
	
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
	
	@Parameter(optional=false, description = "Specifies accessKeyID for S3 Account")
	public void setAccessKeyID(String accessKeyID) {
		fAccessKeyID = accessKeyID;
	}

	
	public String getAccessKeyID() {
		return fAccessKeyID;
	}

	@Parameter(optional=false, description = "Specifies secret access key for S3 Account.")
	public void setSecretAccessKey(String secretAccessKey) {
		fsecretAccessKey = secretAccessKey;
	}

	
	public String getSecretAccessKey() {
		return fsecretAccessKey;
	}

	@Parameter(optional=false, description = "Specifies a bucket to use for scanning.")
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
