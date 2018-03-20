package com.ibm.streamsx.objectstorage.unitest.sink.standalone;

import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.objectstorage.client.Constants;
import com.ibm.streamsx.objectstorage.client.IObjectStorageClient;
import com.ibm.streamsx.objectstorage.client.ObjectStorageClientFactory;
import com.ibm.streamsx.objectstorage.internal.sink.OSObject;
import com.ibm.streamsx.objectstorage.internal.sink.RollingPolicyType;
import com.ibm.streamsx.objectstorage.internal.sink.StorageFormat;

public class Utils {
	/**
	 * Creates new OSObject. Each OSObject represents entity (i.e. object) that
	 * about to be written to object storage.
	 */
	public static OSObject osObjectFactory(final String partitionPath, final String objectname, final String fHeaderRow,
			final int dataIndex, final MetaType dataType, final Tuple tuple, StorageFormat storageFormat) {

		RollingPolicyType rollingPolicyType = getRollingPolicyType(0, 0, 1000);

		OSObject res = new OSObject(objectname, fHeaderRow, "UTF-8", dataIndex, storageFormat.name());

		res.setPartitionPath(partitionPath != null ? partitionPath : "");
		res.setRollingPolicyType(rollingPolicyType.toString());

		return res;

	}
	
	private static RollingPolicyType getRollingPolicyType(Integer timePerObject, Integer dataBytesPerObject, Integer tuplesPerObject) {
		if (timePerObject > 0) return RollingPolicyType.TIME;
		if (dataBytesPerObject > 0) return RollingPolicyType.SIZE;
		if (tuplesPerObject > 0) return RollingPolicyType.TUPLES_NUM;
		
		return RollingPolicyType.UNDEFINED;
	}
	
	public static String genBuffer() throws UnsupportedEncodingException {
		return genBuffer(1000);
	}
	
	public static String genBuffer(int bufferSize) throws UnsupportedEncodingException {
		String res = "";
		int size = 0;
		while (size < bufferSize) {	
			res +=  UUID.randomUUID().toString();
		    size += res.getBytes("UTF-8").length;
		}
		
		return res;
	}

	public static IObjectStorageClient createClient(OperatorContext opContext, String objectStorageURI, String endpoint) throws Exception {
		Configuration config = new Configuration();	
		System.setProperty(Constants.HADOOP_HOME_DIR_CONFIG_NAME, Constants.HADOOP_HOME_DIR_DEFAULT);
		config.set(com.ibm.streamsx.objectstorage.Utils.formatProperty(Constants.S3_SERVICE_ENDPOINT_CONFIG_NAME, com.ibm.streamsx.objectstorage.Utils.getProtocol(objectStorageURI)), endpoint);
		config.set(com.ibm.streamsx.objectstorage.Utils.formatProperty(Constants.S3_ENDPOINT_CONFIG_NAME, com.ibm.streamsx.objectstorage.Utils.getProtocol(objectStorageURI)), endpoint);

		return ObjectStorageClientFactory.getObjectStorageClient(objectStorageURI, opContext, config);
	}
	
	public static String getCurrentTimestamp() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Timestamp(System.currentTimeMillis()));
	}
	
	public static double genRandomLat(Random r) { 
		return (r.nextDouble() * -180.0) + 90.0;
	}
	
	public static double genRandomLon(Random r) { 
		return (r.nextDouble() * -360.0) + 180.0;
	}
	
	public static double genRandomTemperature(Random r) { 
		return Math.random() * 40;
	}

	public static double genRandomHumidity(Random r) { 
		return Math.random() * 10;
	}

	public static double genRandomBaromin(Random r) { 
		return Math.random() * 100;
	}

	public static double genRandomRainin(Random r) { 
		return Math.random();
	}

	
}
