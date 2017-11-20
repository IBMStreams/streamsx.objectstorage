//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.objectstorage.swift;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.logging.Logger;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import com.ibm.stocator.fs.swift.auth.PasswordScopeAccessProvider;
import com.ibm.streams.function.model.Function;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.toolkit.model.ToolkitLibraries;

/**
 * Class for implementing SPL Java native function.
 */
@ToolkitLibraries({ "opt/downloaded/*","opt/*"})
public class FunctionsImpl {

	static Logger TRACER = Logger.getLogger("com.ibm.streamsx.objectstorage.swift");

	private static Account fAccount = null;
	private static Access fAccess = null;

	private static DateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

	@Function(namespace = "com.ibm.streamsx.objectstorage.swift", name = "initialize", description = "Initialize Swift client. This method must be called first.", stateful = false)
	public static boolean initialize(String objectStorageUserID, String objectStoragePassword,
			String objectStorageProjectID) {
		try {
			if (null == fAccount) {
				AccountConfig config = new AccountConfig();				
		        config.setAuthenticationMethod(AuthenticationMethod.EXTERNAL);
				config.setAuthUrl(Constants.OBJECT_STORAGE_AUTH_URL);				
				config.setPreferredRegion(Constants.OBJECT_STORAGE_DEFAULT_REGION);
		        PasswordScopeAccessProvider psap = new PasswordScopeAccessProvider(objectStorageUserID,
		        		objectStoragePassword, objectStorageProjectID, config.getAuthUrl(), config.getPreferredRegion());
		        config.setAccessProvider(psap);
				fAccount = new AccountFactory(config).createAccount();
				// reauthentication is allowed by default - no special actions required
				fAccess = fAccount.authenticate();
			}
		} catch (Exception e) {
			TRACER.log(TraceLevel.ERROR,
					"Failed to initialize connection to object storage using Swift protocol. ERROR: " + e.getMessage());
			return false;
		}
		return true;
	}

	@Function(namespace = "com.ibm.streamsx.objectstorage.swift", name = "createContainer", description = "Creates a container if it doesn't exist.", stateful = false)
	public static boolean createContainer(String containerName) {
		try {
			Container container = fAccount.getContainer(containerName);
			container.create();
			container.makePublic();

			return true;
		} catch (Exception e) {
			TRACER.log(TraceLevel.ERROR,
					"Failed to create container '" + containerName + "'. ERROR: " + e.getMessage());
			return false;
		}

	}

	@Function(namespace = "com.ibm.streamsx.objectstorage.swift", name = "listContainers", description = "Lists all container names.", stateful = false)
	public static String[] listContainers() {
		try {
			String[] resultList = null;
			Collection<Container> containers = fAccount.list();
			resultList = new String[containers.size()];
			int i = 0;
			for (Container currentContainer : containers) {
				resultList[i++] = currentContainer.getName();
			}

			return resultList;
		} catch (Exception e) {
			TRACER.log(TraceLevel.ERROR, "Failed to get containers list. ERROR: " + e.getMessage());
			return null;
		}
	}

	@Function(namespace = "com.ibm.streamsx.objectstorage.swift", name = "deleteContainer", description = "Deletes a container.", stateful = false)
	public static boolean deleteContainer(String containerName) {
		boolean result = true;
		try {
			TRACER.log(TraceLevel.TRACE, "deleteContainer '" + containerName + "'");
			Container container = fAccount.getContainer(containerName);
			container.delete();
		} catch (Exception e) {
			result = false;
			TRACER.log(TraceLevel.ERROR, "Failed to delete container '" + containerName + "'.ERROR: " + e.getMessage());
		}
		return result;
	}

	@Function(namespace = "com.ibm.streamsx.objectstorage.swift", name = "listObjects", description = "Lists all object names in a container.", stateful = false)
	public static String[] listObjects(String containerName) {
		String[] resultList = null;
		try {
			Container container = fAccount.getContainer(containerName);
			Collection<StoredObject> objects = container.list();
			resultList = new String[objects.size()];
			int i = 0;
			for (StoredObject currentObject : objects) {
				resultList[i++] = currentObject.getName();
			}
		} catch (Exception e) {
			TRACER.log(TraceLevel.ERROR,
					"Failed to get list of objects from container '" + containerName + "'.ERROR: " + e.getMessage());
			return null;
		}
		return resultList;
	}

   @Function(namespace="com.ibm.streamsx.objectstorage.swift", name="getObjectMetadata", description="Get object metadata.", stateful=false)
    public static String[] getObjectMetadata(String containerName, String objectName) {
    	String[] resultList = null;
        try {
        	Container container = fAccount.getContainer(containerName);
			StoredObject obj = container.getObject(objectName);
			
        	return new String[] {String.valueOf(obj.getContentLength()), DATE_FORMAT.format(obj.getLastModified())};
        }
        catch (Exception e) {
        	resultList = null;
        	TRACER.log(TraceLevel.ERROR,
					"Failed to get metadata for object '" + objectName  + "'  from container '" + containerName + "'.ERROR: " + e.getMessage());
        }
    	return resultList;
    }    

	
	@Function(namespace = "com.ibm.streamsx.objectstorage.swift", name = "deleteAllObjects", description = "Deletes all objects in a container.", stateful = false)
	public static boolean deleteAllObjects(String containerName) {
		boolean result = true;
		try {
			Container container = fAccount.getContainer(containerName);
			Collection<StoredObject> objects = container.list();
			for (StoredObject currentObject : objects) {
				currentObject.delete();
			}
		} catch (Exception e) {
			result = false;
			TRACER.log(TraceLevel.ERROR,
					"Failed to delete all objects from container '" + containerName + "'.ERROR: " + e.getMessage());
		}
		return result;
	}

	@Function(namespace = "com.ibm.streamsx.objectstorage.swift", name = "deleteObject", description = "Deletes an object from a container.", stateful = false)
	public static boolean deleteObject(String objectName, String containerName) {
		boolean result = true;
		try {
			TRACER.log(TraceLevel.TRACE, "deleteObject " + objectName + " from container " + containerName);
			Container container = fAccount.getContainer(containerName);
			StoredObject object = container.getObject(objectName);
			object.delete();
		} catch (Exception e) {
			result = false;
			TRACER.log(TraceLevel.ERROR, "Failed to remove object '" + objectName + "' from container '" + containerName + "'.ERROR: " + e.getMessage());			
		}
		return result;
	}

}
