package com.ibm.streamsx.objectstorage.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Assert;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.spl.SPLStream;
import com.ibm.streamsx.topology.spl.SPLStreams;
import com.ibm.streamsx.topology.streams.BeaconStreams;


public class Utils {
	  
	/**
	 * Generates endless test injection stream	
	 */
	public static SPLStream getEndlessStringStream(Topology topology, int tupleRate) {
		
		TStream<String> strings = topology.endlessSource(new Supplier<String>() {
			int counter = 0;
			private static final long serialVersionUID = 1L;

				@Override
	            public String get() {
	                return "" + counter++; 
	            }});
		
		TStream<String> throttledStrings = strings.throttle(1000/tupleRate, TimeUnit.MILLISECONDS);
		
        return SPLStreams.stringToSPLStream(throttledStrings);        
    }

	
	/**
	 * Generates test injection stream	
	 */
	public static SPLStream getTestStreamWithEmptyStr(Topology topology, Tuple[] testData, String testSchemaStr, int tupleRate) {
	        TStream<Long> beacon = BeaconStreams.longBeacon(topology, testData.length);	        				        
	        TStream<Long> throttledBeacon = beacon.throttle(1000/tupleRate, TimeUnit.MILLISECONDS);
	        StreamSchema testSchema = Type.Factory.getStreamSchema(testSchemaStr);
	        
	        return SPLStreams.convertStream(throttledBeacon, new BiFunction<Long, OutputTuple, OutputTuple>() {
	            private static final long serialVersionUID = 1L;

	            @Override
	            public OutputTuple apply(Long v1, OutputTuple v2) {
	            	int index  = (int)((long) v1);
	                v2.assign(testData[index]);
	                // periodically set customer id to be emptpy string 
	                if (index%2 == 0) {
	                	v2.setString("customerId", "");
	                } 
	                else if (index%3 == 0) {
	                	v2.setString("customerId", "custid 3");
	                }
	                return v2;
	            }
	        }, testSchema);        
	    }
	
	/**
	 * Generates test injection stream	
	 */
	public static SPLStream getTestStream(Topology topology, Tuple[] testData, String testSchemaStr, int tupleRate) {
	        TStream<Long> beacon = BeaconStreams.longBeacon(topology, testData.length);	        				        
	        TStream<Long> throttledBeacon = beacon.throttle(1000/tupleRate, TimeUnit.MILLISECONDS);
	        StreamSchema testSchema = Type.Factory.getStreamSchema(testSchemaStr);
	        
	        return SPLStreams.convertStream(throttledBeacon, new BiFunction<Long, OutputTuple, OutputTuple>() {
	            private static final long serialVersionUID = 1L;

	            @Override
	            public OutputTuple apply(Long v1, OutputTuple v2) {
	                v2.assign(testData[(int)((long) v1)]);
	                return v2;
	            }
	        }, testSchema);        
	    }
	
	
	/**
	 * Generates tuples list from file lines 
	 * @throws ParseException 
	 */
	public static Tuple[] genTuplesFromFile(String inFilePath, String testDataFileName, String inFileDelimiter, String schemaStr) throws IOException, ParseException {		
		List<String> tuplesStrList = OSTRawFileUtils.getInstance().readFileLineByLine(inFilePath + "/" + testDataFileName);		
		Tuple[] res = new Tuple[tuplesStrList.size()];	
		StreamSchema schema = Type.Factory.getStreamSchema(schemaStr);

		
		int i = 0;
		String[] tupleStrTokens;
		Type.MetaType attrType;
		int attrCount = schema.getAttributeCount();
		Object[] tupleObjects = new Object[attrCount];
		for (String tupleStr: tuplesStrList) {
			tupleStrTokens = tupleStr.split(";");
			for (int j = 0; j < attrCount; j++) {
				attrType = schema.getAttribute(j).getType().getMetaType();
				tupleObjects[j] = valueFactory(attrType, tupleStrTokens[j]);
			}
			res[i] = schema.getTuple(tupleObjects);
			i++;
		}
		
		return res;
	}

	public static Object valueFactory(Type.MetaType type, String value) throws ParseException {
		switch (type) {
		case BLOB: {
			return ValueFactory.newBlob(new RString(value));
		}
		case RSTRING: {
			return new RString(value);
		}
		case BOOLEAN: {
			return new Boolean(value);
		}		
		case FLOAT32: {
			return new Float(value);
		}
		case FLOAT64: {
			return new Double(value);
		}
		case TIMESTAMP: 
			String[] tsTokens = value.substring(1, value.length() - 1).split(",");
			return new Timestamp(Long.parseLong(tsTokens[0]), Integer.parseInt(tsTokens[1]));
		default: 
			return new RString(value);
		}
	}	
	
	public static String joinString(List<String> data, String delimiter) {
		return data.stream().limit(data.size()).collect(Collectors.joining(delimiter));
	}

	public static String joinString(List<String> data, String delimiter, int dataItemsCount) {
		return data.stream().limit(dataItemsCount).collect(Collectors.joining(delimiter));
	}
	
	public static String inputStreamToStr(InputStream is) throws IOException {

		BufferedReader bin = null;
		StringBuilder sin = new StringBuilder();

		String line;
		try {
			bin = new BufferedReader(new InputStreamReader(is));
			while ((line = bin.readLine()) != null) {
				sin.append(line + "\n");
			}

		} finally {
			if (bin != null) bin.close();			
		}

		
		return sin.toString();

	}
	
	public static String removeWhiteSpaces(String str) {
	    return str.replace("\\s+", "");
	}
	

	public static boolean isEclipse() {
	    return System.getProperty("java.class.path").contains("eclipse");
	}
	
	public static File getTestRoot() {	
		File res = isEclipse() ? 
				new File(System.getProperty("user.dir") + "/../../.."): 
				new File(System.getProperty("user.dir") + "/../../../../..");
				
		Assert.assertTrue(res.getPath(), res.isAbsolute());
		Assert.assertTrue(res.getPath(), res.exists());			

		return res;		
	}
	
	public static boolean fileExists(String  filePath) {
		return new File(filePath).exists();
	}
	    
	/**
	 * Extracts protocol from object storage URI
	 */
	public static final String getProtocol(String objectStorageURI) {
		return objectStorageURI.substring(0, objectStorageURI.toString().indexOf("://"));
	}
	
	public static final String buildBaseURI(String protocol, String bucket) {
		return protocol + "://" + bucket + "/";
	}
}
