package com.ibm.streamsx.objectstorage.internal.sink;


import org.ehcache.core.internal.events.EventListenerWrapper;
import org.ehcache.event.CacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OSObjectEventDispatchTask <K, V> implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OSObjectEventDispatchTask.class);
  private final CacheEvent<K, V> cacheEvent;
  private final Iterable<EventListenerWrapper<K, V>> listenerWrappers;

  OSObjectEventDispatchTask(CacheEvent<K, V> cacheEvent, Iterable<EventListenerWrapper<K, V>> listener) {
    if (cacheEvent == null) {
      throw new NullPointerException("cache event cannot be null");
    }
    if (listener == null) {
      throw new NullPointerException("listener cannot be null");
    }
    this.cacheEvent = cacheEvent;
    this.listenerWrappers = listener;
  }

  @Override
  public void run() {
    for(EventListenerWrapper<K, V> listenerWrapper : listenerWrappers) {
      if (listenerWrapper.isForEventType(cacheEvent.getType())) {
        try {
          listenerWrapper.onEvent(cacheEvent);
        } catch (Exception e) {
          LOGGER.warn(listenerWrapper.getListener() + " Failed to fire Event due to ", e);
        }
      }
    }
  }
}