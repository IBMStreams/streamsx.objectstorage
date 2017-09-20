package com.ibm.streamsx.objectstorage;

import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streamsx.objectstorage.client.IObjectStorageClient;

/**
 * Contains object creation
 * logic based on operator parameters
 * 
 * @author streamsadmin
 *
 */
public class OSObjectFactory {

	private String fEncoding = null;
	private String fStorageFormat = null;
	private Integer fTimePerObject  = 0;
	private Integer fDataBytesPerObject = 0;
	private Integer fTuplesPerObject = 0;
	private List<String> fPartitionAttributeNames = null;
	
	private static final String CLASS_NAME = OSObjectFactory.class.getName(); 
	private static Logger TRACE = Logger.getLogger(CLASS_NAME);

	/**
	 * Ctor
	 * @param context
	 */
	public OSObjectFactory(OperatorContext context) {
		
		// Parameters relevant for OSObject creation	 
		fEncoding = Utils.getParamSingleStringValue(context, IObjectStorageConstants.PARAM_ENCODING, "UTF-8");
		fStorageFormat = Utils.getParamSingleStringValue(context, IObjectStorageConstants.PARAM_STORAGE_FORMAT, StorageFormat.raw.name());
		fTimePerObject = Utils.getParamSingleIntValue(context, IObjectStorageConstants.PARAM_TIME_PER_OBJECT, 0);
		fDataBytesPerObject = Utils.getParamSingleIntValue(context, IObjectStorageConstants.PARAM_BYTES_PER_OBJECT, 0);
		fTuplesPerObject = Utils.getParamSingleIntValue(context, IObjectStorageConstants.PARAM_TUPLES_PER_OBJECT, 0);
		fPartitionAttributeNames  = Utils.getParamListValue(context, IObjectStorageConstants.PARAM_PARTITION_VALUE_ATTRIBUTES, null);
	}
	
	
	public OSObject createObject(final OperatorContext operatorContext, 
			                     String objectname, 
			                     final String fHeaderRow, 
			                     final int dataIndex, 
			                     final MetaType dataType, 
			                     final IObjectStorageClient objectStorageClient, 
			                     final Tuple tuple) {
		if (TRACE.isLoggable(TraceLevel.DEBUG)) {
			TRACE.log(TraceLevel.DEBUG,	"Partition attribute names: '" + fPartitionAttributeNames  + "'"); 
		}

		String partitionPath = getPartitionPath(tuple);
		objectname = partitionPath  + objectname; 
		if (TRACE.isLoggable(TraceLevel.DEBUG)) {
			TRACE.log(TraceLevel.DEBUG,	"Adding partition path '" + partitionPath  + "' to object name '" + objectname + "'"); 
		}
		
		OSObject res = new OSObject(operatorContext, 		  
		  objectname,
		  fHeaderRow, 
		  objectStorageClient, 
		  fEncoding, 
		  dataIndex, 
		  dataType,
		  fStorageFormat);

		if (fTimePerObject > 0) {
			res.setExpPolicy(EnumFileExpirationPolicy.TIME);
			// time in parameter specified in seconds, need to convert to miliseconds
			res.setTimePerObject(fTimePerObject * 1000);
			res.createObjectTimer(fTimePerObject * 1000);
		} else if (fDataBytesPerObject > 0) {
		    res.setExpPolicy(EnumFileExpirationPolicy.SIZE);
		    res.setSizePerObject(fDataBytesPerObject);
		} else if (fTuplesPerObject > 0) {
		    res.setExpPolicy(EnumFileExpirationPolicy.TUPLECNT);
		    res.setTuplesPerObject(fTuplesPerObject);
		} 

		res.setPartitionPath(partitionPath.toString());
		
		return res;
	}
	
	public String getPartitionPath(Tuple tuple) {
		StringBuffer res = new StringBuffer();
		if (fPartitionAttributeNames != null && !fPartitionAttributeNames.isEmpty() && tuple != null) {
			// concatenate object name with partition attributes.
			// This will automatically create path if not exists
			for (String attrName: fPartitionAttributeNames) {
				// cast tuple value to string
				res.append(attrName + "=" + tuple.getObject(attrName).toString() + Constants.URI_DELIM);				
			}			
		}
		
		return res.toString();
	}
}
