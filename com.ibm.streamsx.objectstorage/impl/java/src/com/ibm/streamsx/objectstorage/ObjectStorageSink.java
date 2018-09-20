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
description=ObjectStorageSink.DESC+ObjectStorageSink.BASIC_DESC+AbstractObjectStorageOperator.AUTHENTICATION_DESC+ObjectStorageSink.STORAGE_FORMATS_DESC+ObjectStorageSink.ROLLING_POLICY_DESC+ObjectStorageSink.EXAMPLES_DESC)
@InputPorts({@InputPortSet(description="The `ObjectStorageSink` operator has one input port, which writes the contents of the input stream to the object that you specified. The `ObjectStorageSink` supports writing data into object storage in two formats `parquet` and `raw`. The storage format `raw` supports line format and blob format. For line format, the schema of the input port is tuple<rstring line>, which specifies a single rstring attribute that represents a line to be written to the object. For binary format, the schema of the input port is tuple<blob data>, which specifies a block of data to be written to the object.", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="The `ObjectStorageSink` operator is configurable with an optional output port. The schema of the output port is <rstring objectName, uint64 objectSize>, which specifies the name and size of objects that are written to object storage. Note, that the tuple is generated on the object upload completion.", cardinality=1, optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Free)})
@Libraries({"opt/*","opt/downloaded/*" })
public class ObjectStorageSink extends BaseObjectStorageSink implements IObjectStorageAuth {
	
	public static final String DESC = 
			"Operator writes objects to S3 compliant object storage. The operator supports basic (HMAC) and IAM authentication.";
	
	public static final String BASIC_DESC =
			"\\n"+
			"\\nThis operator writes tuples that arrive on its input port to the output object that is named by the **objectName** parameter. "+
			"You can optionally control whether the operator closes the current output object and creates a new object for writing based on the size"+ 
			"of the object in bytes, the number of tuples that are written to the object, or the time in seconds that the object is open for writing, "+
			"or when the operator receives a punctuation marker."+
			"\\n"+ObjectStorageSink.CR_DESC+ObjectStorageSink.BUFFER_DESC;;
			
	public static final String CR_DESC =
			"\\n"+
			"\\n+ Behavior in a consistent region\\n"+
			"\\n"+
			"\\nThe operator can participate in a consistent region. " +
			"The operator can be part of a consistent region, but cannot be at the start of a consistent region.\\n" +
			"\\nWhile consistent region supports that tuples are processed at least once, the operator creates objects in object storage with exactly once semantics.\\n" +
			"\\nFailures during tuple processing or drain are handled by the operator and consistent region support:\\n" +
			"* Object is not visible before it is finally closed\\n" +
			"* Consistent region replays tuples in case of failures\\n" +
			"* Object with the same name is overwritten on object storage\\n" +
			"\\n" +
			"\\n{../../doc/images/Sink_CRSupport.png}\\n\\n" +
			"\\nOn drain, the operator flushes its internal buffer and uploads the object to the object storage.\\n" +
			"On checkpoint, the operator stores the current object number to the checkpoint.\\n"+
			"If the region became inconsistent (e.g. failure causes PE restart), then `reset()` is called and the operator reads the checkpointed data.\\n" +
			"In the case the region is resetted after objects have been already closed on object storage, the affected objects are deleted on object storage.\\n"+
			"\\n# Restrictions\\n"+
			"\\nThe close mode can not be configured when running in a consistent region. The parameters `bytesPerObject`, `closeOnPunct`, `timePerObject` and `tuplesPerObject` are ignored.\\n"+
			"\\nThere is a limited set of variables for the object name supported when running consistent region. The variable `%OBJECTNUM` is mandatory, `%PARTITIONS` is optional, all other variables are not supported. The object number is incrementend after objects are uploaded at end of drain.\\n"+
			"\\n# Metrics\\n"+
			"\\nThe following metrics are available when operator is part of a consistent region only:\\n"+
			"* **`drainTime`** - Drain time of this operator in milliseconds\\n" +
			"* **`drainTimeMax`** - Maximum drain time of this operator in milliseconds\\n" +
			"* **`processingRate`** - Number of input data processed in KB/sec.\\n" +
			"* **`nDeletedObjects`** - Number of objects deleted on reset after objects are closed.\\n" +
			"\\nThe metric **`processingRate`** is calculated by the number of bytes received until `drain()` and the time from first tuple arrival until end of `checkpoint()`\\n" +			
			"\\n{../../doc/images/processingTime.png}\\n\\n"
		   	;
	
	public static final String BUFFER_DESC =
			"\\n"+
			"\\n+ Buffering mechanism\\n"+
			"\\n"+
			"\\nThe operator buffers the object data prior upload to object storage depending on the selected protocol (client) differently." +
			"\\n"+
			"\\n * cos - disk buffering is used"+
			"\\n * s3a - memory buffering is used per default. You can change the buffering mechanism with the parameter `s3aFastUploadBuffer`.\\n"+
			"\\nThe toolkit user may easily switch from hadoop-aws to stocator client by specifying appropriate protocol in the `objectStorageURI` parameter of the ObjectStorageSink operator or in the `protocol` parameter of the S3ObjectStorageSink operator. Concretely, when `s3a` protocol is specified the toolkit uses hadoop-aws client. When `cos` protocol is specified the toolkit uses stocator client."
		   	;	

	public static final String EXAMPLES_DESC =
			"\\n"+
			"\\n+ Examples\\n"+
			"\\n"+
			"\\nThese examples use the `ObjectStorageSink` operator.\\n"+
			"\\n"+
			"\\n**a)** ObjectStorageSink with static object name closed on window marker.\\n"+
			"\\nBeacon operator sends 5000 tuples and window marker afterwards.	ObjectStorageSink operator writes 5000 tuples to the object and closes the object on window marker.\\n"+		
			"\\nSample is using `cos` **application configuration** with property `cos.creds` to specify the IAM credentials:\\n"+
			"Set the **objectStorageURI** either in format \\\"cos://<bucket-name>/\\\" or \\\"s3a://<bucket-name>/\\\".\\n"+	
			"As endpoint is the public **us-geo** (CROSS REGION) the default value of the `os-endpoint` submission parameter.\\n"+
			"\\n    composite Main {"+
			"\\n        param"+
			"\\n            expression<rstring> $objectStorageURI: getSubmissionTimeValue(\\\"os-uri\\\");"+
			"\\n            expression<rstring> $endpoint: getSubmissionTimeValue(\\\"os-endpoint\\\", \\\"s3-api.us-geo.objectstorage.softlayer.net\\\");"+
			"\\n        graph"+
			"\\n            stream<rstring i> SampleData = Beacon()  {"+
			"\\n                param"+
			"\\n                    iterations: 5000;"+
			"\\n                    period: 0.1;"+
			"\\n                output SampleData: i = (rstring)IterationCount();"+
			"\\n            }"+
			"\\n"+
			"\\n            () as osSink = com.ibm.streamsx.objectstorage::ObjectStorageSink(SampleData) {"+
			"\\n                param"+
			"\\n                    objectStorageURI: $objectStorageURI;"+
			"\\n                    objectName : \\\"static_name.txt\\\";"+
			"\\n                    endpoint : $endpoint;"+
			"\\n            }"+
			"\\n    }\\n"+
			"\\n"+
			"\\n**b)** ObjectStorageSink creating objects of size 200 bytes with incremented number in object name.\\n"+
			"\\nSample is using parameters to specify the IAM credentials.\\n"+
			"Set the **objectStorageURI** either in format \\\"cos://<bucket-name>/\\\" or \\\"s3a://<bucket-name>/\\\".\\n"+
			"As endpoint is the public **us-geo** (CROSS REGION) the default value of the `os-endpoint` submission parameter.\\n"+			
			"\\n    composite Main {"+
			"\\n        param"+
			"\\n            expression<rstring> $IAMApiKey: getSubmissionTimeValue(\\\"os-iam-api-key\\\");"+
			"\\n            expression<rstring> $IAMServiceInstanceId: getSubmissionTimeValue(\\\"os-iam-service-instance\\\");"+
			"\\n            expression<rstring> $objectStorageURI: getSubmissionTimeValue(\\\"os-uri\\\");"+
			"\\n            expression<rstring> $endpoint: getSubmissionTimeValue(\\\"os-endpoint\\\", \\\"s3-api.us-geo.objectstorage.softlayer.net\\\");"+
			"\\n        graph"+
			"\\n            stream<rstring i> SampleData = Beacon()  {"+
			"\\n                param"+
			"\\n                    period: 0.1;"+
			"\\n                output SampleData: i = (rstring)IterationCount();"+
			"\\n            }"+
			"\\n"+
			"\\n            () as osSink = com.ibm.streamsx.objectstorage::ObjectStorageSink(SampleData) {"+
			"\\n                param"+
			"\\n                    IAMApiKey: $IAMApiKey;"+
			"\\n                    IAMServiceInstanceId: $IAMServiceInstanceId;"+
			"\\n                    objectStorageURI: $objectStorageURI;"+
			"\\n                    objectName : \\\"%OBJECTNUM.txt\\\";"+
			"\\n                    endpoint : $endpoint;"+
			"\\n                    bytesPerObject: 200l;"+
			"\\n            }"+
			"\\n    }\\n"+
			"\\n"+
			"\\n**c)** ObjectStorageSink creating objects in parquet format.\\n"+
			"\\nOperator reads IAM credentials from application configuration.\\n"+
			"Ensure that cos application configuration with property cos.creds has been created.\\n"+
			"Objects are created in parquet format after $timePerObject in seconds\\n"+
		    "\\n    composite Main {"+
		    "\\n        param"+
		    "\\n            expression<rstring> $objectStorageURI: getSubmissionTimeValue(\\\"os-uri\\\", \\\"cos://streams-sample-001/\\\");"+
			"\\n            expression<rstring> $endpoint: getSubmissionTimeValue(\\\"os-endpoint\\\", \\\"s3-api.us-geo.objectstorage.softlayer.net\\\");"+
			"\\n            expression<float64> $timePerObject: 10.0;"+
			"\\n    "+
			"\\n        type"+
			"\\n            S3ObjectStorageSinkOut_t = tuple<rstring objectName, uint64 size>;"+
			"\\n    "+
			"\\n        graph"+
			"\\n    "+
			"\\n            stream<rstring username, uint64 id> SampleData = Beacon() {"+
			"\\n                param"+
			"\\n                    period: 0.1;"+
			"\\n                output"+
			"\\n                    SampleData : username = \\\"Test\\\"+(rstring) IterationCount(), id = IterationCount();"+
			"\\n            }"+
			"\\n    "+
			"\\n            stream<S3ObjectStorageSinkOut_t> ObjStSink = com.ibm.streamsx.objectstorage::ObjectStorageSink(SampleData) {"+
			"\\n                param"+
			"\\n                    objectStorageURI: $objectStorageURI;"+
			"\\n                    endpoint : $endpoint;"+
			"\\n                    objectName: \\\"sample_%TIME.snappy.parquet\\\";"+
			"\\n                    timePerObject : $timePerObject;"+
			"\\n                    storageFormat: \\\"parquet\\\";"+
			"\\n                    parquetCompression: \\\"SNAPPY\\\";"+
			"\\n            }"+
			"\\n    "+
			"\\n            () as SampleSink = Custom(ObjStSink as I) {"+
			"\\n                 logic"+
			"\\n                    onTuple I: {"+
			"\\n                        printStringLn(\\\"Object with name '\\\" + I.objectName + \\\"' of size '\\\" + (rstring)I.size + \\\"' has been created.\\\");"+
			"\\n                    }"+
			"\\n            }"+
			"\\n    }"+	
			"\\n"			
			;	

	
	public static final String STORAGE_FORMATS_DESC =
			"\\n"+
			"\\n+ Supported Storage Formats\\n"+ 
			"\\nThe operator support two storage formats:\\n"+
			"\\n* `parquet` - when output object is generated in parquet format\\n"+
			"\\n* `raw` - when output object is generated in the raw format\\n"+
			"\\nThe storage format can be configured with the `storageFormat` parameter.\\n"+
			"\\nThe `storageFormat` parameter supports two values: `parquet` and `raw`.\\n"+
			"\\n# Parquet Storage Format\\n"+
			"\\n"+
			"\\nParquet output schema is derived from the tuple structure. Note, that parquet format is supported "+
			"\\nfor tuples with the flat SPL schema only.\\n"+
			"\\n"+
			"\\nThe following table summarizes primitive SPL to Parquet types mapping:\\n"+
			"\\n"+
			"|:------------------------------------------|:------------:|\\n"+
			"| SPL Type                                  | Parquet Type |\\n"+
			"|:==========================================|:============:|\\n"+
			"| BOOLEAN                                   | boolean      |\\n"+
			"|-------------------------------------------|--------------|\\n"+
			"| INT8, UINT8, INT16, UINT16, INT32, UINT32 | int32        |\\n"+
			"|-------------------------------------------|--------------|\\n"+
			"| INT64, UINT64                             | int64        |\\n"+
			"|-------------------------------------------|--------------|\\n"+
			"| FLOAT32                                   | float        |\\n"+
			"|-------------------------------------------|--------------|\\n"+
			"| FLOAT64                                   | double       |\\n"+
			"|-------------------------------------------|--------------|\\n"+
			"| RSTRING, USTRING, BLOB                    | binary       |\\n"+
			"|-------------------------------------------|--------------|\\n"+
			"| TIMESTAMP                                 | int64        |\\n"+
			"|-------------------------------------------|--------------|\\n"+
			"| ALL OTHER SPL PRIMITIVE TYPES             | binary       |\\n"+
			"----------------------------------------------------------\\n"+
			"\\n"+
			"\\n"+
			"\\nThe following table summarizes collection SPL to Parquet types mapping:\\n"+
			"\\n"+
			"|:------------:|:---------------------------------------------------------------------:|\\n"+
			"| SPL Type     | Parquet Type                                                          |\\n"+
			"|:============:|:======================================================================|\\n"+
			"| LIST, SET    | optional group my_list (LIST) (repeated group of list/set elements)   |\\n"+
			"|--------------|-----------------------------------------------------------------------|\\n"+
			"| MAP          | repeated group of key/value                                           |\\n"+
			"---------------------------------------------------------------------------------------\\n"+
			"\\n"+			
			"\\n\\nParameters relevant for `parquet` storage format\\n"+
			"\\n* `nullPartitionDefaultValue` - Specifies default for partitions with null values.\\n"+
			"\\n* `parquetBlockSize` - Specifies the block size which is the size of a row group being buffered in memory. The default is 128M.\\n"+
			"\\n* `parquetCompression` - Enum specifying support compressions for parquet storage format. Supported compression types are 'UNCOMPRESSED','SNAPPY','GZIP'\\n"+
			"\\n* `parquetDictPageSize` - There is one dictionary page per column per row group when dictionary encoding is used. The dictionary page size works like the page size but for dictionary.\\n"+
			"\\n* `parquetEnableDict` - Specifies if parquet dictionary should be enabled.\\n"+
			"\\n* `parquetEnableSchemaValidation` - Specifies of schema validation should be enabled.\\n"+
			"\\n* `parquetPageSize` - Specifies the page size is for compression. A block is composed of pages. The page is the smallest unit that must be read fully to access a single record. If this value is too small, the compression will deteriorate. The default is 1M.\\n"+
			"\\n* `parquetWriterVersion` - Specifies parquet writer version. Supported versions are `1.0` and `2.0`\\n"+
			"\\n* `skipPartitionAttributes` - Avoids writing of attributes used as partition columns in data files.\\n"+
			"\\n* `partitionValueAttributes` - Specifies the list of attributes to be used for partition column values. Please note,"+ 
			"that its strongly recommended not to use attributes with continuous values per rolling policy unit of measure"+ 
			"to avoid operator performance degradation. "+
			"The following examples demonstrates recommended and non-recommended partitioning approaches."+
			"**Recommended**: /YEAR=YYYY/MONTH=MM/DAY=DD/HOUR=HH "+
			"**Non-recommended**: /latutide=DD.DDDD/longitude=DD.DDDD/\\n"+
			"\\n"+
			"\\n**Parquet storage format - preferred practices for partitions design**\\n"+
			"\\n1. Think about what kind of queries you will need. For example, you might need to build monthly reports or sales by product line.\\n"+
			"\\n2. Do not partition on an attribute with high cardinality per rolling policy window that you end up with too many simultaneously"+
			"active partitions. Reducing the number of  simultaneously active partitions can greatly improve performance and operator's resource consumption.\\n"+
			"\\n3. Do not partition on attribute with high cardinality per rolling policy window so you end up with many small-sized objects.\\n"+
			"\\n"+
			"\\n\\n# Raw Storage Format\\n"+
			"\\nIf the input tuple schema for the `raw` storage format has more than one input attribute the operators expect `dataAttribute` parameter "+
			"to be specified. The attribute specified as `dataAttribute` value should be of `rstring` or `blob` type.\\n"+
			"\\nParameters relevant for the `raw` storage format:\\n"+
			"\\n* `dataAttribute` - Required when input tuple has more than one attribute. Specifies the name of the attribute which "+
			"content is about to be written to the output object. The attribute should has `rstring` or `blob` SPL type.\\n"+
			"Mandatory parameter for the case when input tuple has more than one attribute and the storage format is set to `raw`.\\n"+
			"\\n* `objectNameAttribute` - If set, it points to the attribute containing an object name. The operator will close the object when value "+
			"of this attribute changes and will open the new object with an updated name.\\n"+
			"\\n* `encoding` - Specifies the character encoding that is used in the output object.\\n"+
			"\\n* `headerRow` - If specified the header line with the parameter content will be generated in each output object.\\n"			
		   	;
	
	
	public static final String ROLLING_POLICY_DESC =
			"\\n"+
			"\\n+ Rolling Policy\\n"+
			"\\nRolling policy specifies the window size managed by operator per output object."+ 
			"\\nWhen window is closed the current output object is closed and a new object is opened."+
			"\\nThe operator supports three rolling policy types:\\n"+
			"\\n* Size-based (parameter `bytesPerObject`)\\n"+
			"\\n* Time-based (parameter `timePerObject`)\\n"+
			"\\n* Tuple count-based (parameter `tuplesPerObject`)\\n"+
			"\\n"+
			"\\n# Object name\\n"+
			"\\nThe `objectName` parameter can optionally contain the following variables, which the operator evaluates at runtime "+
			"to generate the object name:\\n"+
			"\\n"+
			"\\n**%TIME** is the time when the COS object is created. The default time format is yyyyMMdd_HHmmss.\\n"+ 
			"\\n"+ 
			"\\nThe variable %TIME can be added anywhere in the path after the bucket name. The variable is typically used to "+ 
			"make dynamic object names when you expect the application to create multiple objects.\\n"+ 
			"\\nHere are some examples of valid file paths with %TIME:\\n"+
			"\\n * `event%TIME.parquet`\\n"+
			"\\n * `%TIME_event.parquet`\\n"+
			"\\n * `/my_new_folder/my_new_file_%TIME.csv`\\n"+
			"\\n"+ 			
			"\\n**%OBJECTNUM** is an object number, starting at 0, when a new object is created for writing.\\n"+ 
			"\\nObjects with the same name will be overwritten. Typically, %OBJECTNUM is added after the file name.\\n"+
			"\\nHere are some examples of valid file paths with %OBJECTNUM:\\n"+
			"\\n * `event_%OBJECTNUM.parquet`\\n"+
			"\\n * `/geo/uk/geo_%OBJECTNUM.parquet`\\n"+
			"\\n * `%OBJECTNUM_event.csv`\\n"+
			"\\n * `%OBJECTNUM_%TIME.csv`\\n"+
			"\\n"+ 
			"\\nNote: If partitioning is used, %OBJECTNUM is managed globally for all partitions in the COS object,"+ 
			"rather than independently for each partition."+
			"\\n"+ 
			"\\n**%PARTITIONS** place partitions anywhere in the object name.  By default, partitions are placed immediately before the last part of the object name.\\n"+
			"\\nHere's an example of default position of partitions in an object name: \\n"+
			"\\nSuppose that the file path is `/GeoData/test_%TIME.parquet`. Partitions are defined as YEAR, MONTH, DAY, and HOUR.\\n"+ 
			"\\nThe object in COS would be `/GeoData/YEAR=2014/MONTH=7/DAY=29/HOUR=36/test_20171022_124948.parquet` \\n"+
			"\\n"+ 
			"\\nWith %PARTITIONS, you can change the placement of partitions in the object name from the default. \\n"+
			"\\nLet's see how the partition placement changes by using %PARTITIONS:\\n"+
			"\\nSuppose that the file path now is `/GeoData/Asia/%PARTITIONS/test_%TIME.parquet.` \\n"+
			"\\nThe object name in COS would be `/GeoData/Asia/YEAR=2014/MONTH=7/DAY=29/HOUR=36/test_20171022_124948.parquet`\\n"+
			"\\n"+ 
			"\\n**Empty partition values** \\n"+
			"\\nIf a value in a partition is not valid, the invalid values are replaced by the string `__HIVE_DEFAULT_PARTITION__` in the COS object name.\\n"+ 
			"\\nFor example, `/GeoData/Asia/YEAR=2014/MONTH=7/DAY=29/HOUR=__HIVE_DEFAULT_PARTITION__/test_20171022_124948.parquet`\\n"+
			"\\n"+
			"\\n**Further variables for the object name**\\n"+
			"\\n"+
			"\\n* `%HOST` the host that is running the processing element (PE) of this operator.\\n"+
			"\\n* `%PROCID` the process ID of the processing element running the this operator.\\n"+
			"\\n* `%PEID` the processing element ID.\\n"+
			"\\n* `%PELAUNCHNUM` the PE launch count.\\n"+
			"\\n"
			;
	
	@Parameter(optional=true, description = "Specifies username for connection to a Cloud Object Storage (COS), also known as 'AccessKeyID' for S3-compliant COS.")
	public void setObjectStorageUser(String objectStorageUser) {
		super.setUserID(objectStorageUser);
	}
	
	public String getObjectStorageUser() {
		return super.getUserID();
	}
	
	@Parameter(optional=true, description = "Specifies password for connection to a Cloud Object Storage (COS), also known as 'SecretAccessKey' for S3-compliant COS.")
	public void setObjectStoragePassword(String objectStoragePassword) {
		super.setPassword(objectStoragePassword);
	}
	
	public String getObjectStoragePassword() {
		return super.getPassword();
	}
		
	@Parameter(optional=false, description = "Specifies URI for connection to Cloud Object Storage (COS). For S3-compliant COS the URI should be in  'cos://bucket/ or s3a://bucket/' format. The bucket or container must exist. The operator does not create a bucket or container.")
	public void setObjectStorageURI(String objectStorageURI) {
		super.setURI(objectStorageURI);
	}
	
	public String getObjectStorageURI() {
		return super.getURI();
	}

	@Parameter(optional=false, description = "Specifies endpoint for connection to Cloud Object Storage (COS). For example, for S3 the endpoint might be 's3.amazonaws.com'.")
	public void setEndpoint(String endpoint) {
		super.setEndpoint(endpoint);
	}


	@Parameter(optional=true, description = "Specifies IAM API Key. Relevant for IAM authentication case only. If `cos` application configuration contains property `cos.creds`, then this parameter is ignored.")
	public void setIAMApiKey(String iamApiKey) {
		super.setIAMApiKey(iamApiKey);
	}
	
	public String getIAMApiKey() {
		return super.getIAMApiKey();
	}
	
	@Parameter(optional=true, description = "Specifies IAM token endpoint. Relevant for IAM authentication case only. Default value is 'https://iam.bluemix.net/oidc/token'.")
	public void setIAMTokenEndpoint(String iamTokenEndpoint) {
		super.setIAMTokenEndpoint(iamTokenEndpoint);;
	}
	
	public String getIAMTokenEndpoint() {
		return super.getIAMTokenEndpoint();
	}
	
	@Parameter(optional=true, description = "Specifies IAM service instance ID for connection to Cloud Object Storage (COS). Relevant for IAM authentication case only. If `cos` application configuration contains property `cos.creds`, then this parameter is ignored.")
	public void setIAMServiceInstanceId(String iamServiceInstanceId) {
		super.setIAMServiceInstanceId(iamServiceInstanceId);
	}
	
	public String getIAMServiceInstanceId() {
		return super.getIAMServiceInstanceId();
	}
	
	@Parameter(optional=true, description = "Specifies the name of the application configuration containing IBM Cloud Object Storage (COS) IAM credentials. If not set the default application configuration name is `cos`. Create a property in the `cos` application configuration *named* `cos.creds`. The *value* of the property `cos.creds` should be the raw IBM Cloud Object Storage Credentials JSON.")
	public void setAppConfigName(String appConfigName) {
		super.setAppConfigName(appConfigName);
	}
	
	public String getAppConfigName() {
		return super.getAppConfigName();
	}
	
}
