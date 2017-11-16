package com.ibm.streamsx.objectstorage;

import static com.ibm.streamsx.objectstorage.client.Constants.PROTOCOL_URI_DELIM;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.s3.S3Protocol;
import com.ibm.streamsx.objectstorage.swift.SwiftProtocol;



public class Utils {

	/**
	 * Create a logger specific to this class
	 */
	private static final String CLASS_NAME = "com.ibm.streamsx.objectstorage.Utils";

	private static Logger TRACE = Logger.getLogger(CLASS_NAME);


	public static String getObjectStorageS3URI(S3Protocol protocol, String bucket) {
		return protocol + "://" + bucket + "/";
	}

	public static String getObjectStorageSwiftURI(SwiftProtocol protocol, String container) {
		return protocol + "://" + container + "/";
	}

	public static URI genObjectURI(URI objectStorageURI, String objectName) throws URISyntaxException {
		if (objectName.startsWith(objectStorageURI.getScheme() + PROTOCOL_URI_DELIM)) {			
			return new URI(objectName);
		} else {
			String objectValue = objectName.startsWith("/") ? objectName : "/" + objectName;
			URI res = new URI(objectStorageURI.getScheme(), objectStorageURI.getAuthority(), objectValue, null, null);
			return res;
		}
	}

	/**
	 * Encode URI if not encoded yet
	 * 
	 * @param objectStorageUriStr
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static URI getEncodedURI(String objectStorageUriStr) throws IOException, URISyntaxException {
		String scheme = getProtocol(objectStorageUriStr);
		@SuppressWarnings("deprecation")
		String host = URLEncoder.encode(Utils.getHost(URLDecoder.decode(objectStorageUriStr)));		
		@SuppressWarnings("deprecation")
		String path = Utils.getPath(URLDecoder.decode(objectStorageUriStr));

		// this specific URI ctor encodes the result
		// return new URI(scheme, host, path, null, null);
		return new URI(scheme + "://" + host + "/" + path);
	}

	public static String getEncodedURIStr(String objectStorageUriStr) throws IOException, URISyntaxException {
		return getEncodedURI(objectStorageUriStr).toString();
	}

	public static String getHost(String uriStr) {
		int sInd = uriStr.indexOf("//") + 2;
		String host = uriStr.substring(sInd);
		int eInd = host.indexOf("/");

		return host.substring(0, eInd);
	}

	public static String getPath(String uriStr) {
		return uriStr.substring(getProtocol(uriStr).length() + getHost(uriStr).length() + 3, uriStr.length());
	}

	/**
	 * Extracts protocol from object storage URI
	 */
	public static final String getProtocol(String objectStorageURI) {
		return objectStorageURI.substring(0, objectStorageURI.toString().indexOf(PROTOCOL_URI_DELIM));
	}

	public static boolean isValidObjectStorageUser(String user) {
		if (user == null || user.trim().isEmpty())
			return false;

		return true;
	}
	
	
	
	/**
	 * Stocator expects property names to be container dependent. The method populates 
	 * template with container name configured on operator level.
	 * @throws URISyntaxException 
	 */
	public static String formatProperty(String propTemplate, String templateValue) throws IOException, URISyntaxException {
		String res = String.format(propTemplate, templateValue);
		if (TRACE.isLoggable(TraceLevel.DEBUG)) {
			TRACE.log(TraceLevel.DEBUG,	"Formatted property template '" + propTemplate + "' with value '" + templateValue + "': " + res); 
		}	
		return res;
	}

	/**
	 * Gets param single String value from context
	 * @param opContext operator context
	 * @param paramName parameter name
	 * @return single parameter value
	 */
	public static String getParamSingleStringValue(OperatorContext opContext, String paramName, String paramDefault) {
		String res = paramDefault;
		
		if (opContext.getParameterNames().contains(paramName)) {
			res = opContext.getParameterValues(paramName).get(0);
		}
		
		return res;
	}
	
	
	/**
	 * Gets param single int value from context
	 * @param opContext operator context
	 * @param paramName parameter name
	 * @return single parameter value
	 */
	public static int getParamSingleIntValue(OperatorContext opContext, String paramName, int paramDefault) {
		return Integer.parseInt(getParamSingleStringValue(opContext, paramName, String.valueOf(paramDefault)));
	}
	
	/**
	 * Gets param boolean value from context
	 * @param opContext operator context
	 * @param paramName parameter name
	 * @return single parameter value
	 */
	public static boolean getParamSingleBoolValue(OperatorContext opContext, String paramName, boolean paramDefault) {
		return Boolean.parseBoolean(getParamSingleStringValue(opContext, paramName, String.valueOf(paramDefault)));
	}

	/**
	 * Gets param list value from context
	 * @param opContext operator context
	 * @param paramName parameter name
	 * @param paramDefault parameter default value
	 * @return parameter value list
	 */
	public static List<String> getParamListValue(OperatorContext opContext, String paramName, List<String> paramDefault) {
		List<String> res = paramDefault;
		if (opContext.getParameterNames().contains(paramName)) {
			res = opContext.getParameterValues(paramName);
		}
		
		return res;
	}
	

}
