package com.ibm.streamsx.objectstorage.unitest.sink.standalone;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.objectstorage.IObjectStorageConstants;
import com.ibm.streamsx.objectstorage.client.IObjectStorageClient;
import com.ibm.streamsx.objectstorage.internal.sink.OSObject;
import com.ibm.streamsx.objectstorage.internal.sink.OSWritableObject;
import com.ibm.streamsx.objectstorage.internal.sink.RollingPolicyType;
import com.ibm.streamsx.objectstorage.internal.sink.StorageFormat;

public class TestWritePerformance {

	@Rule public TestName name = new TestName();
	
	/**
	 * Creates new OSObject. Each OSObject represents 
	 * entity (i.e. object) that about to be written to object storage.
	 */
	public static OSObject osObjectFactory(final String partitionPath,
         final String objectname, 
         final String fHeaderRow, 
         final int dataIndex, 
         final MetaType dataType,			                     
         final Tuple tuple) {
		
		RollingPolicyType rollingPolicyType = getRollingPolicyType(0, 0, 1000);
		
		OSObject res = new OSObject(
				objectname,
				fHeaderRow, 
				"UTF-8", 
				dataIndex,
				StorageFormat.raw.name());
		
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

	
	
	
	@Before
	public void init() {
		MockitoAnnotations.initMocks(this);
	}

	private static StreamSchema genTupleSchema(ArrayList<TestSchemaAttribute> testSchemaAttrList)  {
		
		StreamSchema res = mock(StreamSchema.class);	
		for (int i = 0; i < testSchemaAttrList.size(); i++) {
			Attribute attrMock = mock(Attribute.class);
			Type attrType = mock(Type.class);
			when(attrType.getMetaType()).thenReturn(testSchemaAttrList.get(i).getType());
			when(res.getAttribute(i)).thenReturn(attrMock);
			when(attrMock.getType()).thenReturn(attrType);
			when(attrMock.getName()).thenReturn(testSchemaAttrList.get(i).getName());
			when(res.getAttribute(i)).thenReturn(attrMock);
		}
		
		when(res.getAttributeCount()).thenReturn(testSchemaAttrList.size());
		
		
		return res;
	}
	
	private static String getCurrentTimestamp() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Timestamp(System.currentTimeMillis()));
	}
	
	private static StreamSchema genDataHistoranSchema() {
		
		ArrayList<TestSchemaAttribute> res = new ArrayList<TestSchemaAttribute>();
		Random r = new Random();
		
		double lat = (r.nextDouble() * -180.0) + 90.0;
		double lon = (r.nextDouble() * -360.0) + 180.0;
		res.add(new TestSchemaAttribute("id", "I90580453", MetaType.RSTRING));
		res.add(new TestSchemaAttribute("tz", "America/New_York", MetaType.RSTRING));
		res.add(new TestSchemaAttribute("dateutc", getCurrentTimestamp(), MetaType.RSTRING));
		res.add(new TestSchemaAttribute("time_stamp", getCurrentTimestamp(), MetaType.RSTRING));
		res.add(new TestSchemaAttribute("longitude",  String.valueOf(lat), MetaType.FLOAT64));
		res.add(new TestSchemaAttribute("latitude", String.valueOf(lon), MetaType.FLOAT64));
		res.add(new TestSchemaAttribute("temperature", String.valueOf(34.900), MetaType.FLOAT64));
		res.add(new TestSchemaAttribute("baromin", String.valueOf(27.7230), MetaType.FLOAT64));
		res.add(new TestSchemaAttribute("humidity", String.valueOf(91.527), MetaType.FLOAT64));
		res.add(new TestSchemaAttribute("rainin", String.valueOf(0.5), MetaType.FLOAT64));
		
		
		return genTupleSchema(res);
		
	}
	
	private static OutputTuple genDataHistorianTuple(StreamSchema tupleSchema, StorageFormat storageFormat) {
		OutputTuple res = mock(OutputTuple.class);
		Random r = new Random();
		
		when(res.getStreamSchema()).thenReturn(tupleSchema);
		double lat = (r.nextDouble() * -180.0) + 90.0;
		double lon = (r.nextDouble() * -360.0) + 180.0;
		if (storageFormat.equals(StorageFormat.parquet)) {
			when(res.getObject(0)).thenReturn("I90580453" + (int)(Math.random() * 10));
			when(res.getObject(1)).thenReturn("America/New_York");
			when(res.getObject(2)).thenReturn(getCurrentTimestamp());
			when(res.getObject(3)).thenReturn(getCurrentTimestamp());
			when(res.getObject(4)).thenReturn(lat);
			when(res.getObject(5)).thenReturn(lon);
			when(res.getObject(6)).thenReturn(Math.random() * 40);
			when(res.getObject(7)).thenReturn(Math.random() * 10);
			when(res.getObject(8)).thenReturn(Math.random() * 100);
			when(res.getObject(9)).thenReturn(Math.random());
		} else {
			String tupleStr = "{ " +
                    "id: " + "I90580453, " +
                    "tz : " + "America/New_York, " + 
                    "dateutc : " + "2017-01-24 01:54:20, " +  
                    "time_stamp : " + "2017-08-09 02:18:43, " + 
                    "longitude : " + "-79.7267227172852, " +
                    "latitude : " + "43.6024017333984, " + 
                    "temperature : " + "34.9000015258789, " + 
                    "baromin : " + "27.7230415344238, " + 
                    "humidity : " + "91.5271301269531, " + 
                    "rainin : " + "0" + " }";
			
			
			
			when(res.getObject(0)).thenReturn(new RString(tupleStr)); 			  
		}
		
		return res;
	}
	
	private void setCredentials(OperatorContext opContext) {
		LinkedList<String> iamAPIKey = new LinkedList<String>();
		iamAPIKey.add("");

		LinkedList<String> IAMServiceInstanceId = new LinkedList<String>();
		IAMServiceInstanceId.add("");

		LinkedList<String> IAMTokenEndpoint = new LinkedList<String>();
		IAMTokenEndpoint.add("https://iam.ng.bluemix.net/oidc/token");
		
		Set<String> paramNames = new HashSet<String>();
		paramNames.add(IObjectStorageConstants.PARAM_IAM_APIKEY);
		paramNames.add(IObjectStorageConstants.PARAM_IAM_SERVICE_INSTANCE_ID);
		paramNames.add(IObjectStorageConstants.PARAM_IAM_TOKEN_ENDPOINT);
		when(opContext.getParameterNames()).thenReturn(paramNames);
		when(opContext.getParameterValues(IObjectStorageConstants.PARAM_IAM_APIKEY)).thenReturn(iamAPIKey);
		when(opContext.getParameterValues(IObjectStorageConstants.PARAM_IAM_SERVICE_INSTANCE_ID)).thenReturn(IAMServiceInstanceId);
		when(opContext.getParameterValues(IObjectStorageConstants.PARAM_IAM_TOKEN_ENDPOINT)).thenReturn(IAMTokenEndpoint);

	}
	
	/**
	 * The test uses mock listener that does nothing 
	 * to measure replace operation for growing object 
	 * in EHCache
	 * @throws Exception 
	 */
	@Test	
	public void testParquetWritePerformanceLocal() throws Exception { 
		
		testtWritePerformanceLocal(StorageFormat.parquet);
	
	}
	
	@Test	
	public void testRawWritePerformanceLocal() throws Exception { 
		
		testtWritePerformanceLocal(StorageFormat.raw);
	
	}

	
	public void testtWritePerformanceLocal(StorageFormat storageFormat) throws Exception { 
		int tupleCount = 10000;
		
		DescriptiveStatistics stats = new DescriptiveStatistics();
		
		// run locally
		String objectStorageURI = "file:///tmp/";
		String endpoint = "s3-api.us-geo.objectstorage.softlayer.net";

		// set context mock
		OperatorContext opContext = mock(OperatorContext.class, Mockito.RETURNS_DEEP_STUBS);
		
		// set connection credentials
		setCredentials(opContext);
		
		// create data historian schema
		StreamSchema tupleSchema = TestWritePerformance.genDataHistoranSchema();

		StreamingInput<Tuple> inTupleMock = mock(StreamingInput.class);			
		List<StreamingInput<Tuple>> inTuplesList = new LinkedList<StreamingInput<Tuple>>();
		inTuplesList.add(inTupleMock);					
		when(opContext.getStreamingInputs()).thenReturn(inTuplesList);		
		when(inTupleMock.getStreamSchema()).thenReturn(tupleSchema);
		
		// create OSObject and populates it with data
		OSObject osObject = com.ibm.streamsx.objectstorage.unitest.sink.standalone.Utils.osObjectFactory("", 
								name.getMethodName(), null, 0, MetaType.BOOLEAN, null, storageFormat);		
		
		for (int i = 0; i < tupleCount; i++) {
			// create tuple with data historian data
			OutputTuple tupleMock = TestWritePerformance.genDataHistorianTuple(tupleSchema, storageFormat);
			osObject.getDataBuffer().add(tupleMock);
		}
		
		// create writable OSObject
		IObjectStorageClient objectStorageClient = com.ibm.streamsx.objectstorage.unitest.sink.standalone.Utils.createClient(opContext, objectStorageURI, endpoint);
		objectStorageClient.connect();
		 
		long start = System.currentTimeMillis();
		OSWritableObject writableObject = new OSWritableObject(osObject, opContext, objectStorageClient);		
		// flush buffer
		writableObject.flushBuffer();
		// close object
		writableObject.close();
		long operationTime = System.currentTimeMillis() - start;
		stats.addValue(operationTime);
		
		System.out.println(name.getMethodName() + " -> mean: " + stats.getMean() + " ms, std: " + stats.getStandardDeviation() + " ms, median: " + stats.getPercentile(90) + "ms");
		System.out.println(name.getMethodName() + " -> mean tuples/sec: " + (tupleCount/stats.getMean())*1000 + " ms,  median tuples/sec: " + (tupleCount/stats.getPercentile(90))*1000 + "ms");
		//assertTrue("Replace operation average performance  is '" + stats.getMean() + "' is slower than 5 microseconds", stats.getMean() < 5000);
	}
	
}
	