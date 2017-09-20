/*******************************************************************************
* Copyright (C) 2014, International Business Machines Corporation
* All Rights Reserved
*******************************************************************************/

package com.ibm.streamsx.objectstorage.client.auth;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.Messages;

import static com.ibm.streamsx.objectstorage.Utils.isValidObjectStorageUser;

public abstract class BaseAuthenticationHelper implements IAuthenticationHelper {
	
	enum AuthType {
		SIMPLE, KERBEROS
	}

	private static final String CLASS_NAME = "com.ibm.streamsx.objectstorage.client.auth"; 

	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME); 
	
	public static final String SIMPLE_AUTH = "simple";
	public static final String KERBEROS_AUTH = "kerberos";

	protected Configuration fConfiguration;
	protected URI fObjectStorageUri;
	
	public BaseAuthenticationHelper() {
		fConfiguration = new Configuration();

	}
	
	public BaseAuthenticationHelper(String configPath) {
		fConfiguration = new Configuration();
		
		if(configPath != null && !configPath.isEmpty()) {
			try {
				URL url = new URL("file", null, configPath + "/core-site.xml");
				fConfiguration.addResource(url);
			} catch (MalformedURLException e) {
				LOGGER.log(TraceLevel.ERROR, e.getMessage(), e);
			}
		}
	}

	@Override
	public FileSystem connect(String fileSystemUri, String hdfsUser,
			Map<String, String> connectionProperties) throws Exception {
		if (fileSystemUri != null && !fileSystemUri.isEmpty()) {
			fConfiguration.set("fs.defaultFS", fileSystemUri);
		}

		// if fs.defaultFS is not specified, try the deprecated name
		String uri = fConfiguration.get("fs.defaultFS");
		if (uri == null || uri.toString().isEmpty()) {
			uri = fConfiguration.get("fs.default.name");
		}

		if (uri == null) {
			throw new Exception("Unable to find a URI to connect to.");
		}

		setObjectStorageUri(new URI(uri));
		LOGGER.log(TraceLevel.DEBUG, Messages.getString("OBJECTSTORAGE_CLIENT_AUTH_ATTEMPTING_CONNECT" , getObjectStorageUri().toString()));
		
		return null;
	}
	
	protected FileSystem internalGetFileSystem(URI osUri, String osUser) throws Exception {
		FileSystem fs = null;
		
		if (isValidObjectStorageUser(osUser)) {
			LOGGER.log(TraceLevel.DEBUG, Messages.getString("OBJECTSTORAGE_CLIENT_AUTH_CONNECT", osUri + " " + osUser));
			fs = FileSystem.get(osUri, fConfiguration, osUser);
		} else {
			LOGGER.log(TraceLevel.DEBUG, Messages.getString("OBJECTSTORAGE_CLIENT_AUTH_CONNECT", osUri));
			fs = FileSystem.get(osUri, fConfiguration);
		}
		
		return fs;
	}
	
	protected UserGroupInformation authenticateWithKerberos(final String hdfsUser,
			final String kerberosPrincipal, final String kerberosKeytab)
			throws Exception {
		UserGroupInformation.setConfiguration(fConfiguration);
		UserGroupInformation ugi;
		if (isValidObjectStorageUser(hdfsUser)) {
			UserGroupInformation.loginUserFromKeytab(kerberosPrincipal,
					kerberosKeytab);
			ugi = UserGroupInformation.createProxyUser(hdfsUser,
					UserGroupInformation.getLoginUser());
			LOGGER.log(TraceLevel.DEBUG, Messages.getString("OBJECTSTORAGE_CLIENT_AUTH_PROXY_CONNECT" , hdfsUser));
		} else {
			ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
					kerberosPrincipal, kerberosKeytab);
			LOGGER.log(TraceLevel.DEBUG, Messages.getString("OBJECTSTORAGE_CLIENT_AUTH_USING_KERBOSER" , kerberosPrincipal));
		}

		return ugi;
	}
	
	public URI getObjectStorageUri() {
		return fObjectStorageUri;
	}
	
	public void setObjectStorageUri(URI objectStorageURI) {
		fObjectStorageUri = objectStorageURI;
	}
	
	public Configuration getConfiguration() {
		return fConfiguration;
	}

	public AuthType getAuthType() {
		String auth = fConfiguration.get("hadoop.security.authentication");

		if (auth != null && auth.toLowerCase().equals("kerberos")) {
			return AuthType.KERBEROS;
		} else {
			return AuthType.SIMPLE;
		}
	}
}
