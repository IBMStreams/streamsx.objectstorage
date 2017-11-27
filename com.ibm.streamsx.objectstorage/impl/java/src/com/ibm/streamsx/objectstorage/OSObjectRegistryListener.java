package com.ibm.streamsx.objectstorage;

import java.util.logging.Logger;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;

import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;

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
			try {
				// create writable OSObject
				OSWritableObject writableObject = new OSWritableObject(osObject, fParent.getOperatorContext(), fParent.getObjectStorageClient());
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
			break;
		case EXPIRED: // OSObject is expired according to Expiry
			          // derived from operator rolling policy 
			if (TRACE.isLoggable(TraceLevel.DEBUG)) {
				TRACE.log(TraceLevel.DEBUG,	"OSObject   '" + osObject.getPath()  + "' has been expired. Closing it."); 
			}
			try {
				// create writable OSObject
				OSWritableObject writableObject = new OSWritableObject(osObject, fParent.getOperatorContext(), fParent.getObjectStorageClient());
				// flush buffer
				writableObject.flushBuffer();
				// close object
				writableObject.close();
				
				// update metrics
				fParent.getActiveObjectsMetric().incrementValue(-1);
				fParent.getCloseObjectsMetric().increment();
				fParent.getExpiredObjectsMetric().increment();

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
					throw new RuntimeException(e);
				}
			}
			
			break;
		case EVICTED: // no space left for new entries
			if (TRACE.isLoggable(TraceLevel.DEBUG)) {
				TRACE.log(TraceLevel.DEBUG,	"Entry with key '" + event.getKey() + "' has been evicted ahead of time due to cache resource problem. Consider to increase cache size."); 				
			}
			// close to avoid data loss
			try {
				// create writable OSObject
				OSWritableObject writableObject = new OSWritableObject(osObject, fParent.getOperatorContext(), fParent.getObjectStorageClient());
				// flush buffer
				writableObject.flushBuffer();
				// close object
				writableObject.close();
				
				// update metrics
				fParent.getActiveObjectsMetric().incrementValue(-1);
				fParent.getCloseObjectsMetric().increment();
				fParent.getExpiredObjectsMetric().increment();

				// submit output 
				fParent.submitOnOutputPort(osObject.getPath(), osObject.getDataSize());	
			} 
			catch (InterruptedException ie) {
				// the thread was interrupted 
				Thread.currentThread().interrupt();
				throw new RuntimeException(ie);
			}
			catch (Exception e) {
				if (TRACE.isLoggable(TraceLevel.ERROR)) {
					TRACE.log(TraceLevel.ERROR,	"Failed to close OSObject with path '" + osObject.getPath() + "'. Error message: " + e.getMessage()); 
				}
				throw new RuntimeException(e);
			}
			break;
		default:
			String errMsg = "Unknown event type '" + event.getType() + "' for key '" + event.getKey() + "' has been received.";
			LOGGER.log(TraceLevel.ERROR, Messages.getString(errMsg));
			break;
		}
	}
	
}
