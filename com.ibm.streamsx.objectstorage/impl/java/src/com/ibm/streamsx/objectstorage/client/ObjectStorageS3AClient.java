package com.ibm.streamsx.objectstorage.client;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.Utils;
import com.ibm.streamsx.objectstorage.auth.OSAuthenticationHelper;

public class ObjectStorageS3AClient extends ObjectStorageAbstractClient  {

	private static Logger TRACE = Logger.getLogger(ObjectStorageS3AClient.class.getName());


	public ObjectStorageS3AClient(String objectStorageURI, OperatorContext opContext) throws Exception {
		super(objectStorageURI, opContext);
	}

	public ObjectStorageS3AClient(String objectStorageURI, OperatorContext opContext, Configuration config) throws Exception {
		super(objectStorageURI, opContext, config);
	}

	@Override
	public void connect() throws Exception {
		initClientConfig();
		
	    fFileSystem = new org.apache.hadoop.fs.s3a.S3AFileSystem();	
		String formattedPropertyName = Utils.formatProperty(Constants.S3_SERVICE_ENDPOINT_CONFIG_NAME, Utils.getProtocol(fObjectStorageURI));
		String endpoint = fConnectionProperties.get(formattedPropertyName);
		if (TRACE.isLoggable(TraceLevel.INFO)) {
			TRACE.log(TraceLevel.INFO, "About to initialize object storage file system with endpoint '" + endpoint  + "'. Use configuration property '" + formattedPropertyName + "' to update it if required.");
		}
	    fFileSystem.initialize(new URI(fObjectStorageURI), fConnectionProperties);	
	}

	
	@Override
	public void initClientConfig() throws IOException, URISyntaxException {
		
		String protocol = Utils.getProtocol(fObjectStorageURI);

		// config authentication related properties
		OSAuthenticationHelper.configAuthProperties(protocol, fOpContext, fConnectionProperties);
		
		fConnectionProperties.set(Constants.S3A_IMPL_CONFIG_NAME, Constants.S3A_DEFAULT_IMPL);
		//fConnectionProperties.set(Utils.formatProperty(Constants.S3A_SERVICE_ACCESS_KEY_CONFIG_NAME, protocol), fObjectStorageUser);
		//fConnectionProperties.set(Utils.formatProperty(Constants.S3A_SERVICE_SECRET_KEY_CONFIG_NAME, protocol), fObjectStoragePassword);			
		//fConnectionProperties.set(Utils.formatProperty(Constants.S3_ENDPOINT_CONFIG_NAME, protocol), Constants.S3_DEFAULT_ENDPOINT);
		fConnectionProperties.setIfUnset(Utils.formatProperty(Constants.S3_MULTIPART_CONFIG_NAME, protocol), Constants.S3_MULTIPATH_SIZE);
	
		fConnectionProperties.set(Constants.S3A_SIGNING_ALGORITHM_CONFIG_NAME, "S3SignerType");
		
		// Enable S3 path style access ie disabling the default virtual hosting behaviour.
		fConnectionProperties.set(Constants.S3A_PATH_STYLE_ACCESS_CONFIG_NAME, Boolean.TRUE.toString());			

		// -------------- Enable Streaming on output ----------------------------
		// Enable fast upload mechanism
		fConnectionProperties.set(Constants.S3A_FAST_UPLOAD_ENABLE_CONFIG_NAME, Boolean.TRUE.toString());
		
		// When fs.s3a.fast.upload.buffer is set to bytebuffer, all data is buffered in “Direct” ByteBuffers prior to upload. 
		// This may be faster than buffering to disk, and, if disk space is small. "bytebuffer" uses off-heap memory within the JVM.
	    // fConnectionProperties.set(Constants.S3A_FAST_UPLOAD_BUFFER_CONFIG_NAME, "bytebuffer");
	    fConnectionProperties.set(Constants.S3A_FAST_UPLOAD_BUFFER_CONFIG_NAME, "disk");
	    //fConnectionProperties.set(Constants.S3A_FAST_UPLOAD_BUFFER_CONFIG_NAME, "array");
	    fConnectionProperties.set(Constants.S3A_DISK_BUFFER_DIR_CONFIG_NAME, Constants.S3A_DISK_BUFFER_DIR);			     
	}

	@Override
	public OutputStream getOutputStream(String filePath, boolean append)
			throws IOException {
		
		if (TRACE.isLoggable(TraceLevel.DEBUG)) {
			TRACE.log(TraceLevel.DEBUG,	"Get output stream for file path '" + filePath + "' in object storage with url '" + fObjectStorageURI + "'"); 
		}

		if (fIsDisconnected)
			return null;

		if (!append) {
			
			Path objPath = new Path(fObjectStorageURI, filePath);			
			boolean overwrite = true; // the current default behavior is
									  // to overwrite object if exists
			return fFileSystem.create(objPath, overwrite);
		} else {
			Path path = new Path(fObjectStorageURI, filePath);
			// if file exist, create output stream to append to file
			if (fFileSystem.exists(path)) {
				return fFileSystem.append(path);
			} else {
				OutputStream stream = fFileSystem.create(new Path(fObjectStorageURI, path));
				return stream;
			}
		}
	}
}
