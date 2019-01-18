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
import com.ibm.streamsx.objectstorage.AbstractObjectStorageOperator;
import com.ibm.streamsx.objectstorage.BaseObjectStorageSink;
import com.ibm.streamsx.objectstorage.Utils;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streamsx.objectstorage.ObjectStorageSink;

@PrimitiveOperator(name="S3ObjectStorageSink", namespace="com.ibm.streamsx.objectstorage.s3",
description=S3ObjectStorageSink.DESC+ObjectStorageSink.BASIC_DESC+ObjectStorageSink.STORAGE_FORMATS_DESC+ObjectStorageSink.ROLLING_POLICY_DESC+S3ObjectStorageSink.EXAMPLES_DESC)
@InputPorts({@InputPortSet(description="The `S3ObjectStorageSink` operator has one input port, which writes the contents of the input stream to the object that you specified. The `S3ObjectStorageSink` supports writing data into object storage in two formats. For line format, the schema of the input port is tuple<rstring line>, which specifies a single rstring attribute that represents a line to be written to the object. For binary format, the schema of the input port is tuple<blob data>, which specifies a block of data to be written to the object.", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="The `S3ObjectStorageSink` operator is configurable with an optional output port. The schema of the output port is <rstring objectName, uint64 objectSize>, which specifies the name and size of objects that are written to object storage. Note, that the tuple is generated on the object upload completion.", cardinality=1, optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Free)})
@Libraries({"opt/*","opt/downloaded/*" })
public class S3ObjectStorageSink extends BaseObjectStorageSink implements IS3ObjectStorageAuth {
	
	public static final String DESC = 
			"Operator writes objects to S3 compliant object storage.";
	
	public static final String EXAMPLES_DESC =
			"\\n"+
			"\\n+ Examples\\n"+
			"\\n"+
			"\\nThese examples use the `S3ObjectStorageSink` operator.\\n"+
			"\\n"+
			"\\n**a)** S3ObjectStorageSink creating objects of size 200 bytes with incremented number in object name.\\n"+
			"As endpoint is the public **us-geo** (CROSS REGION) the default value of the `os-endpoint` submission parameter.\\n"+			
			"\\n    composite Main {"+
			"\\n        param"+
			"\\n            expression<rstring> $accessKeyID : getSubmissionTimeValue(\\\"os-access-key-id\\\");"+
			"\\n            expression<rstring> $secretAccessKey : getSubmissionTimeValue(\\\"os-secret-access-key\\\");"+
			"\\n            expression<rstring> $bucket: getSubmissionTimeValue(\\\"os-bucket\\\");"+
			"\\n            expression<rstring> $endpoint: getSubmissionTimeValue(\\\"os-endpoint\\\", \\\"s3.us.cloud-object-storage.appdomain.cloud\\\");"+
			"\\n        graph"+
			"\\n            stream<rstring i> SampleData = Beacon()  {"+
			"\\n                param"+
			"\\n                    period: 0.1;"+
			"\\n                output SampleData: i = (rstring)IterationCount();"+
			"\\n            }"+
			"\\n"+
			"\\n            () as osSink = com.ibm.streamsx.objectstorage.s3::S3ObjectStorageSink(SampleData) {"+
			"\\n                param"+
			"\\n                    accessKeyID : $accessKeyID;"+
			"\\n                    secretAccessKey : $secretAccessKey;"+
			"\\n                    bucket : $bucket;"+
			"\\n                    objectName : \\\"%OBJECTNUM.txt\\\";"+
			"\\n                    endpoint : $endpoint;"+
			"\\n                    bytesPerObject: 200l;"+
			"\\n            }"+
			"\\n    }\\n"+
			"\\n"+
			"\\n**b)** S3ObjectStorageSink creating objects in parquet format.\\n"+
			"Objects are created in parquet format after $timePerObject in seconds\\n"+
		    "\\n    composite Main {"+
		    "\\n        param"+
			"\\n            expression<rstring> $accessKeyID : getSubmissionTimeValue(\\\"os-access-key-id\\\");"+
			"\\n            expression<rstring> $secretAccessKey : getSubmissionTimeValue(\\\"os-secret-access-key\\\");"+
			"\\n            expression<rstring> $bucket: getSubmissionTimeValue(\\\"os-bucket\\\");"+
			"\\n            expression<rstring> $endpoint: getSubmissionTimeValue(\\\"os-endpoint\\\", \\\"s3.us.cloud-object-storage.appdomain.cloud\\\");"+
			"\\n            expression<float64> $timePerObject: 10.0;"+
			"\\n    "+
			"\\n        type"+
			"\\n            S3ObjectStorageSinkOut_t = tuple<rstring objectName, uint64 size>;"+
			"\\n    "+
			"\\n        graph"+
			"\\n    "+
			"\\n            stream<rstring username, uint64 id> SampleData = Beacon() {"+
			"\\n                param"+
			"\\n                   period: 0.1;"+
			"\\n                output"+
			"\\n                    SampleData : username = \\\"Test\\\"+(rstring) IterationCount(), id = IterationCount();"+
			"\\n            }"+
			"\\n    "+
			"\\n            stream<S3ObjectStorageSinkOut_t> ObjStSink = com.ibm.streamsx.objectstorage.s3::S3ObjectStorageSink(SampleData) {"+
			"\\n                param"+
			"\\n                    accessKeyID : $accessKeyID;"+
			"\\n                    secretAccessKey : $secretAccessKey;"+
			"\\n                    bucket : $bucket;"+
			"\\n                    endpoint : $endpoint;"+
			"\\n                    objectName: \\\"sample_%TIME.snappy.parquet\\\";"+
			"\\n                    timePerObject : $timePerObject;"+
			"\\n                    storageFormat: \\\"parquet\\\";"+
			"\\n                    parquetCompression: \\\"SNAPPY\\\";"+
			"\\n            }"+
			"\\n    "+
			"\\n            () as SampleSink = Custom(ObjStSink as I) {"+
			"\\n                logic"+
			"\\n                    onTuple I: {"+
			"\\n                         printStringLn(\\\"Object with name '\\\" + I.objectName + \\\"' of size '\\\" + (rstring)I.size + \\\"' has been created.\\\");"+
			"\\n                    }"+
			"\\n            }"+
			"\\n    }"+	
			"\\n"			
			;		
	
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
	
	@Parameter(optional=false, description = "Specifies the Access Key ID for S3 account.")
	public void setAccessKeyID(String accessKeyID) {
		fAccessKeyID = accessKeyID;
	}
	
	public String getAccessKeyID() {
		return fAccessKeyID;
	}

	@Parameter(optional=false, description = "Specifies the Secret Access Key for S3 account.")
	public void setSecretAccessKey(String secretAccessKey) {
		fsecretAccessKey = secretAccessKey;
	}
	
	public String getSecretAccessKey() {
		return fsecretAccessKey;
	}

	@Parameter(optional=false, description = "Specifies a bucket to use for writing objects. The bucket must exist. The operator does not create a bucket.")
	public void setBucket(String bucket) {
		fBucket = bucket;		
	}
	
	public String getBucket() {
		return fBucket;
	}

	@Parameter(optional = true, description = "Specifies the protocol to use for communication with object storage. Supported values are s3a and cos. The default value is s3a.")
	public void setProtocol(S3Protocol protocol) {
		fProtocol = protocol;		
	}
	
	public S3Protocol getProtocol() {
		return fProtocol;
	}

	@Parameter(optional=true, description = "Specifies endpoint for connection to object storage. For example, for S3 the endpoint might be 's3.amazonaws.com'. The default value is the IBM Cloud Object Storage (COS) public endpoint 's3.us.cloud-object-storage.appdomain.cloud'.")
	public void setEndpoint(String endpoint) {
		super.setEndpoint(endpoint);
	}

}
