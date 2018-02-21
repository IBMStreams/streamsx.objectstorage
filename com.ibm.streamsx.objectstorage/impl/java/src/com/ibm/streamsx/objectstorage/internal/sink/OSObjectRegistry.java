package com.ibm.streamsx.objectstorage.internal.sink;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.UserManagedCache;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.event.EventType;
import org.ehcache.expiry.Expiry;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.LoggerNames;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.objectstorage.BaseObjectStorageSink;
import com.ibm.streamsx.objectstorage.IObjectStorageConstants;
import com.ibm.streamsx.objectstorage.Utils;

/**
 * Manages registry of open objects
 * per partition.
 * @author streamsadmin
 *
 */
public class OSObjectRegistry {
	
	private static final String CLASS_NAME = OSObjectRegistry.class.getName(); 
	private static Logger LOGGER = Logger.getLogger(LoggerNames.LOG_FACILITY + "." + CLASS_NAME); 

	/**
	 * EHCache configuration
	 */
	
	private static final String OS_OBJECT_CACHE_NAME_PREFIX = "OSObjectCache";
	
	
	// dispatcher concurrency
	private static final int CACHE_DISPATCHER_CONCURRENCY = 1;
	private static final long SIZE_OF_MAX_OBJECT_GRAPH = 1024 * 512;
	
	// event listener pool settings
	private static final String EVENT_LISTENER_THREADPOOL_NAME = "EventListenerThreadPool";
	private static final int EVENT_LISTENER_THREADPOOL_MIN_SIZE = 3;
	private static final int EVENT_LISTENER_THREADPOOL_MAX_SIZE = 10;
	
	// disk tier settings
	private static final int DISK_HEAP_MAX_CACHE_SIZE_GB = 1;
	private static final String DISK_CACHE_DIR = "/tmp/objectStorageRegistryCache";
	private static final String DISK_STORE_THREADPOOL_NAME = "DiskStoreThreadPool";
	private static final int DISK_STORE_THREADPOOL_MIN_SIZE = 0;
	private static final int DISK_STORE_THREADPOOL_MAX_SIZE = 1;
	
	// default thread pool settings
	private static final String DEFAULT_THREADPOOL_NAME = "OSRegistryDefaultThreadPool";
	private static final int DEFAULT_THREADPOOL_MIN_SIZE = 0;
	private static final int DEFAULT_THREADPOOL_MAX_SIZE = 2;

	
	// OSRegistry allows to utilize 70% of operator's JVM heap
	private static final double OSREGISTRY_MEMORY_PORTION = 0.7;
	
	// represents object registry: partition is a key, object is a value	
	private Cache<String, OSObject>  fCache = null;
	
	
	CacheManager fCacheManager = null;
	private String fCacheName = null;
	private OSObjectRegistryListener fOSObjectRegistryListener = null;
	
	private Integer fTimePerObject  = 0;
	private Integer fDataBytesPerObject = 0;
	private Integer fTuplesPerObject = 0;
	private boolean fCloseOnPunct = false;
	
	private long osRegistryMaxMemory = 0;
	private BaseObjectStorageSink fParent;
	
	
	private static Logger TRACE = Logger.getLogger(CLASS_NAME);
	
	public OSObjectRegistry(OperatorContext opContext, BaseObjectStorageSink parent) {

		fParent = parent;
		
		fOSObjectRegistryListener = new OSObjectRegistryListener(parent);
				
		fTimePerObject = Utils.getParamSingleIntValue(opContext, IObjectStorageConstants.PARAM_TIME_PER_OBJECT, 0);
		fDataBytesPerObject = Utils.getParamSingleIntValue(opContext, IObjectStorageConstants.PARAM_BYTES_PER_OBJECT, 0);
		fTuplesPerObject = Utils.getParamSingleIntValue(opContext, IObjectStorageConstants.PARAM_TUPLES_PER_OBJECT, 0);
		fCloseOnPunct = Utils.getParamSingleBoolValue(opContext, IObjectStorageConstants.PARAM_CLOSE_ON_PUNCT, false);
		
		fCacheName = Utils.genCacheName(OS_OBJECT_CACHE_NAME_PREFIX, opContext);

		Expiry<Object, Object> expiry = null;
		if (fTimePerObject > 0) {
			if (TRACE.isLoggable(TraceLevel.DEBUG)) {
				TRACE.log(TraceLevel.DEBUG,	"Set expiration policy for cache '" + fCacheName  + "' on '" + fTimePerObject + "' seconds"); 
			}
			expiry = new TimePerObjectExpiry(fTimePerObject);
		} 
		else if (fDataBytesPerObject > 0) {
			expiry = new DataBytesPerObjectExpiry(fDataBytesPerObject);
		} 
		else if (fTuplesPerObject > 0) {
			expiry = new TuplesPerObjectExpiry(fTuplesPerObject);
		} else if (fCloseOnPunct) {
			expiry = new OnPunctExpiry();
		}

		// defines event listeners pool
		CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder().
				using(PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder().
						defaultPool(DEFAULT_THREADPOOL_NAME, 
								    DEFAULT_THREADPOOL_MIN_SIZE, 
								    DEFAULT_THREADPOOL_MAX_SIZE).
//						pool(DISK_STORE_THREADPOOL_NAME, 
//								 DISK_STORE_THREADPOOL_MIN_SIZE, 
//								 DISK_STORE_THREADPOOL_MAX_SIZE).
						pool(EVENT_LISTENER_THREADPOOL_NAME, 
							 EVENT_LISTENER_THREADPOOL_MIN_SIZE, 
							 EVENT_LISTENER_THREADPOOL_MAX_SIZE).build()); 
		
		// upper heap limit for OSRegistry
		osRegistryMaxMemory = (long) (OSREGISTRY_MEMORY_PORTION * Runtime.getRuntime().maxMemory());
		
		if (TRACE.isLoggable(TraceLevel.DEBUG)) {
			TRACE.log(TraceLevel.DEBUG,	"OSObject registry memory limit '" + osRegistryMaxMemory + "'");
		}
			
		
		CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
    			.newEventListenerConfiguration(fOSObjectRegistryListener,EventType.CREATED , EventType.REMOVED, EventType.EVICTED, EventType.EXPIRED
    			 ) 
    			.ordered().asynchronous();

		
//		CacheConfigurationBuilder<String, OSObject> cacheConfigBuilder = 
//				CacheConfigurationBuilder.
//					newCacheConfigurationBuilder(String.class, OSObject.class, ResourcePoolsBuilder.newResourcePoolsBuilder().
//				    heap(fTuplesPerObject, EntryUnit.ENTRIES)).					
////					heap(osRegistryMaxMemory, MemoryUnit.B)).
////					disk(DISK_HEAP_MAX_CACHE_SIZE_GB, MemoryUnit.GB)).											
////					withValueSerializer(OSObjectSerializer.class). // use custom serializer for disk			
//					withDispatcherConcurrency(CACHE_DISPATCHER_CONCURRENCY).
//					withEventListenersThreadPool(EVENT_LISTENER_THREADPOOL_NAME).							
//					withExpiry(expiry).										
//					withSizeOfMaxObjectGraph(SIZE_OF_MAX_OBJECT_GRAPH).add(cacheEventListenerConfiguration);
//
//		// bypass trying to load the Agent entirely 
//		System.setProperty(AgentSizeOf.BYPASS_LOADING, "true");
//		fCacheManager = cacheManagerBuilder.
//	//				with(CacheManagerBuilder.persistence(DISK_CACHE_DIR)).
//					withCache(fCacheName, cacheConfigBuilder).build(true);
//		
//		if (TRACE.isLoggable(TraceLevel.DEBUG)) {
//			TRACE.log(TraceLevel.DEBUG,	"Creating  '" + fCacheName  + "' cache"); 
//		}
//		
//		
//		// creates OSRegistry cache
//		fCache = fCacheManager.getCache(fCacheName, String.class, OSObject.class);
		
//		fCache.getRuntimeConfiguration().registerCacheEventListener(fOSObjectRegistryListener, EventOrdering.ORDERED,
//				EventFiring.ASYNCHRONOUS, EnumSet.of(EventType.CREATED, EventType.REMOVED, EventType.EVICTED, EventType.EXPIRED));
 

//		CacheEventDispatcherImpl<String, OSObject> eventDispatcher = new CacheEventDispatcherImpl<String, OSObject>(null, null);
		OSObjectCacheEventDispatcher<String, OSObject> eventDispatcher = new OSObjectCacheEventDispatcher<String, OSObject>(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5));
		
		UserManagedCacheBuilder<String, OSObject, UserManagedCache<String, OSObject>> umcb = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, OSObject.class)
				.withEventExecutors(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5))
				//.withEventExecutors(Executors.newFixedThreadPool(2), Executors.newFixedThreadPool(5))
				.withEventListeners(cacheEventListenerConfiguration)
				.withEventDispatcher(eventDispatcher)
				.withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
				.withDispatcherConcurrency(CACHE_DISPATCHER_CONCURRENCY)
				.withExpiry(expiry)									
				.withSizeOfMaxObjectGraph(SIZE_OF_MAX_OBJECT_GRAPH);
		
		fCache = umcb.build(true);
		
		if (TRACE.isLoggable(TraceLevel.DEBUG)) {
			TRACE.log(TraceLevel.DEBUG,	"Using '" + fCacheName  + "' cache as internal objects registry"); 
		}
	}
	
	/**
	 * Finds object in registry
	 * @param key partition
	 * @return object if exists, null otherwise
	 */
	public OSObject find(String key) {
		return fCache.get(key);			
	}

	/**
	 * Registers new object 
	 * @param key object partition
	 * @param value object to register
	 * @param nActivePartitions 
	 */
	public void register(String key, OSObject value) {
		fCache.put(key, value);
	}
	
	/**
	 * Removed object from regitsry
	 * @param key partition
	 * @param nActivePartitions 
	 */
	public void remove(String key) {
		if (fCache.containsKey(key)) {
			fCache.remove(key);
		}
	}
		

	
	public String toString() {
		StringBuffer res = new StringBuffer();
		org.ehcache.Cache.Entry<String, OSObject> cacheEntry = null;
		Iterator<org.ehcache.Cache.Entry<String, OSObject>> cacheIterator = fCache.iterator();
		int cacheEntryCount = 0;
		while (cacheIterator.hasNext()) {
			cacheEntry = ((org.ehcache.Cache.Entry<String, OSObject>)cacheIterator.next());
		    res.append("key=" + cacheEntry.getKey() + ", object=" + cacheEntry.getValue().getPath() + "\n");		    
		    cacheEntryCount++;
		}
		
		res.append("Cache stats: entries number='" + cacheEntryCount + "'");
		
		return res.toString();				
	}

	
	/**
	 * Closes all active objects
	 */
	public void closeAll() {
		Iterator<org.ehcache.Cache.Entry<String, OSObject>> cacheIterator = fCache.iterator();
		org.ehcache.Cache.Entry<String, OSObject> cacheEntry = null;
		String cacheKey = null;
		while (cacheIterator.hasNext()) {
			cacheEntry = ((org.ehcache.Cache.Entry<String, OSObject>)cacheIterator.next());
			cacheKey = cacheEntry.getKey();
			remove(cacheKey); // triggers REMOVED event responsible for object closing and metrics update	
		}
	}

	
	public void shutdownCache() {
		if (fCacheManager != null) {
			fCacheManager.removeCache(fCacheName);
			fCacheManager.close();
		}
	}

	public void update(String key, OSObject osObject) {		
		fCache.replace(key, osObject);		
		// replace equivalent to get + put
		// so, required to update expiration if time used
		
	}

	public void expireAll() {
		Iterator<org.ehcache.Cache.Entry<String, OSObject>> cacheIterator = fCache.iterator();
		org.ehcache.Cache.Entry<String, OSObject> cacheEntry = null;
		OSObject osObject = null;
		while (cacheIterator.hasNext()) {
			cacheEntry = ((org.ehcache.Cache.Entry<String, OSObject>)cacheIterator.next());
			osObject = cacheEntry.getValue();
			osObject.setExpired();
			fCache.replace(cacheEntry.getKey(), osObject);
		}		
	}

}
