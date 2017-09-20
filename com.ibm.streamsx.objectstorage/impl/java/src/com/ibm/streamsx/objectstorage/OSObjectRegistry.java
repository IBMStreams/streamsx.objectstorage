package com.ibm.streamsx.objectstorage;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.ibm.streams.operator.logging.LoggerNames;

/**
 * Manages registry of open objects
 * per partition.
 * @author streamsadmin
 *
 */
public class OSObjectRegistry {
	
	private static final String CLASS_NAME = OSObjectRegistry.class.getName(); 
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME); 

	
	// represents object registry: partition is a key, object is a value
	private ConcurrentHashMap<String, OSObject> fOSObjectRegistry = new ConcurrentHashMap<String, OSObject>();
	
	/**
	 * Finds object in registry
	 * @param key partition
	 * @return object if exists, null otherwise
	 */
	public OSObject find(String key) {
		if (fOSObjectRegistry.containsKey(key))
			return fOSObjectRegistry.get(key);
		return null;
	}

	/**
	 * Registers new object 
	 * @param key object partition
	 * @param value object to register
	 */
	public void register(String key, OSObject value) {			
		fOSObjectRegistry.put(key, value);
	}

	/**
	 * Removed object from regitsry
	 * @param key partition
	 */
	public void remove(String key) {
		if (fOSObjectRegistry.contains(key)) fOSObjectRegistry.remove(key);
	}
	
	public ConcurrentHashMap<String, OSObject> getRegistry() {
		return fOSObjectRegistry;
	}
	
	public String toString() {
		StringBuffer res = new StringBuffer();
		
		for (Entry<String, OSObject> entry : fOSObjectRegistry.entrySet()) {
		    String key = entry.getKey().toString();
		    OSObject value = entry.getValue();
		    res.append("key=" + key + ", object=" + value.getPath() + "\n");		    
		}
		
		return res.toString();				
	}

	/**
	 * Closes all active objects
	 */
	public void closeAll() {
		OSObject activeObject = null;
		for (Entry<String, OSObject> entry : fOSObjectRegistry.entrySet()) {		    
		    try {
		    	activeObject = entry.getValue();
		    	if (!activeObject.isClosed())
		    		activeObject.close();
		    } catch (Exception e) {
		    	LOGGER.warning("Failed to close object '" + entry.getValue().getPath() + "'");
			} finally {
			    fOSObjectRegistry.remove(entry.getKey());
		    }
		}		
		
		
	}

	
}
