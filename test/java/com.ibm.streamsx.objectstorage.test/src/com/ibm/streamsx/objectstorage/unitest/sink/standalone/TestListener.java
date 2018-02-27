package com.ibm.streamsx.objectstorage.unitest.sink.standalone;

import java.util.logging.Logger;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;

import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.BaseObjectStorageSink;
import com.ibm.streamsx.objectstorage.Messages;
import com.ibm.streamsx.objectstorage.internal.sink.OSObject;

/**
 * Listener for cache lifecycle events
 * @author streamsadmin
 *
 */
public class TestListener implements CacheEventListener<String, OSObject> {
	
	private BaseObjectStorageSink fParent;
	
	private static final String CLASS_NAME = TestListener.class.getName();
	
	private static Logger TRACE = Logger.getLogger(CLASS_NAME);
		
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME);

	public TestListener() {		
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
			break;
		
		case REMOVED: // entry has been removed
			writeObject(osObject);
			break;
		
		case EXPIRED: // OSObject is expired according to Expiry
			          // derived from operator rolling policy 
			writeObject(osObject);
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
		//(new WriteObjectThread()).start();
		
	}
	

	public class WriteObjectThread extends Thread {

	    public void run() {
	    	System.out.println("WriteObject stub called");
	    }
	}

}

