package com.ibm.streamsx.objectstorage.internal.sink;

import org.ehcache.core.internal.events.EventListenerWrapper;
import org.ehcache.event.CacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.BaseObjectStorageSink;
import com.ibm.streamsx.objectstorage.Messages;

public class OSObjectEventDispatchTask<K, V> implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(OSObjectEventDispatchTask.class);
	private final CacheEvent<K, V> cacheEvent;
	private final Iterable<EventListenerWrapper<K, V>> listenerWrappers;
	private BaseObjectStorageSink parent;

	public OSObjectEventDispatchTask(CacheEvent<K, V> cacheEvent, Iterable<EventListenerWrapper<K, V>> listener,
			BaseObjectStorageSink parent) {
		if (cacheEvent == null) {
			throw new NullPointerException("cache event cannot be null");
		}
		if (listener == null) {
			throw new NullPointerException("listener cannot be null");
		}
		this.cacheEvent = cacheEvent;
		this.listenerWrappers = listener;
		this.parent = parent;
	}

	@Override
	public void run() {
		for (EventListenerWrapper<K, V> listenerWrapper : listenerWrappers) {
			if (listenerWrapper.isForEventType(cacheEvent.getType())) {
				try {
					// listenerWrapper.onEvent(cacheEvent);
					switch (cacheEvent.getType()) {
					case CREATED: // new entry added
						parent.getActiveObjectsMetric().increment();
						break;

					case REMOVED: // entry has been removed
						writeObject((OSObject) cacheEvent.getOldValue());
						break;

					case EXPIRED: // OSObject is expired according to Expiry
									// derived from operator rolling policy
						writeObject((OSObject) cacheEvent.getOldValue());
						break;
					case EVICTED: // no space left for new entries
						writeObject((OSObject) cacheEvent.getOldValue());
						break;

					default:
						String errMsg = "Unknown event type '" + cacheEvent.getType() + "' for key '"
								+ cacheEvent.getKey() + "' has been received.";
						// LOGGER.log(TraceLevel.ERROR,
						// Messages.getString(errMsg));
						break;
					}
				} catch (Exception e) {
					LOGGER.warn(listenerWrapper.getListener() + " Failed to fire Event due to ", e);
				}
			}
		}
	}

	private void writeObject(OSObject osObject) {
		try {
			System.out.println("writeObject invoked for object " + osObject.fPath);
			// create writable OSObject
			OSWritableObject writableObject = new OSWritableObject(osObject, parent.getOperatorContext(),
					parent.getObjectStorageClient());
			// flush buffer
			writableObject.flushBuffer();
			// close object
			writableObject.close();

			// update metrics
			parent.getActiveObjectsMetric().incrementValue(-1);
			parent.getCloseObjectsMetric().increment();

			// submit output
			parent.submitOnOutputPort(osObject.getPath(), osObject.getDataSize());
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(ie);
		} catch (Exception e) {
			// if (TRACE.isLoggable(TraceLevel.ERROR)) {
			// TRACE.log(TraceLevel.ERROR, "Failed to close OSObject with path
			// '" + osObject.getPath() + "'. Error message: " + e.getMessage());
			// }
			throw new RuntimeException(e);
		}

	}

}