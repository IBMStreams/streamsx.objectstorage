package com.ibm.streamsx.objectstorage.internal.sink;

import java.util.logging.Logger;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;

import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.BaseObjectStorageSink;
import com.ibm.streamsx.objectstorage.Messages;

/**
 * Listener for cache lifecycle events
 * @author streamsadmin
 *
 */
public class OSObjectRegistryListener implements CacheEventListener<String, OSObject> {
	
	private BaseObjectStorageSink fParent;
	
	private static final String CLASS_NAME = OSObjectRegistryListener.class.getName();
	
	private static Logger TRACE = Logger.getLogger(CLASS_NAME);
		
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME);

	public OSObjectRegistryListener(BaseObjectStorageSink parent) {
		fParent = parent;
	}

	@Override
	public void onEvent(CacheEvent<? extends String, ? extends OSObject> event)  {
		System.out.println("OSObjectRegistryListener.onevent: " + Thread.currentThread().getId() + ": thread name " + Thread.currentThread().getName() + "");
		OSObject osObject = event.getOldValue();
		if (TRACE.isLoggable(TraceLevel.DEBUG)) {			
			TRACE.log(TraceLevel.DEBUG,	"Event received for partition '" + event.getKey() + "' of type '" + event.getType() + "'");
			if (osObject != null) TRACE.log(TraceLevel.DEBUG,	"About to process OSObject: \n  '" + osObject.toString() + "'");
		}
		switch (event.getType()) {
		case CREATED: // new entry added
			fParent.getActiveObjectsMetric().increment();
			break;
		
		case REMOVED: // entry has been removed
			writeObject(osObject);
			break;
		
		case EXPIRED: // OSObject is expired according to Expiry
			          // derived from operator rolling policy 
			writeObject(osObject);
			if (TRACE.isLoggable(TraceLevel.WARNING)) {
				
			}
			break;
		case EVICTED: // no space left for new entries
			writeObject(osObject);
			break;
			
		default:
			String errMsg = "Unknown event type '" + event.getType() + "' for key '" + event.getKey() + "' has been received.";
			LOGGER.log(TraceLevel.ERROR, Messages.getString(errMsg));
			break;
		}
	}
	
	private void writeObject(OSObject osObject) {
		try {
			// create writable OSObject
			OSWritableObject writableObject = osObject.isWritable() ? 
															(OSWritableObject)osObject : 
															new OSWritableObject(osObject, fParent.getOperatorContext(), fParent.getObjectStorageClient());
			// flush buffer
			writableObject.flushBuffer();
			// close object
			writableObject.close();
			
			// update metrics
			fParent.getActiveObjectsMetric().incrementValue(-1);
			fParent.getCloseObjectsMetric().increment();
			
			// submit output 
			fParent.submitOnOutputPort(osObject.getPath(), osObject.getDataSize());	
		} 
		catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		}
		catch (Exception e) {
			if (TRACE.isLoggable(TraceLevel.ERROR)) {
				TRACE.log(TraceLevel.ERROR,	"Failed to close OSObject with path '" + osObject.getPath() + "'. Error message: " + e.getMessage()); 		
			}
			throw new RuntimeException(e);
		}
		
	}
	
}
