package com.ibm.streamsx.objectstorage.unitest.sink.cache;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.UserManagedCache;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.PooledExecutionServiceConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.sizeof.impl.AgentSizeOf;
import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.objectstorage.internal.sink.OSObject;
import com.ibm.streamsx.objectstorage.internal.sink.OSObjectCacheEventDispatcher;
import com.ibm.streamsx.objectstorage.internal.sink.OSObjectRegistryListener;
import com.ibm.streamsx.objectstorage.internal.sink.OSObjectSerializer;
import com.ibm.streamsx.objectstorage.internal.sink.RollingPolicyType;
import com.ibm.streamsx.objectstorage.internal.sink.StorageFormat;
import com.ibm.streamsx.objectstorage.internal.sink.TuplesPerObjectExpiry;

public class TestCache {

	// dispatcher concurrency
	private static final int CACHE_DISPATCHER_CONCURRENCY = 2;
	
	private static final long SIZE_OF_MAX_OBJECT_GRAPH = 1024 * 512;
	
	// event listener pool settings
	private static final String EVENT_LISTENER_THREADPOOL_NAME = "EventListenerThreadPool";
	private static final int EVENT_LISTENER_THREADPOOL_MIN_SIZE = 1;
	private static final int EVENT_LISTENER_THREADPOOL_MAX_SIZE = 1;
	
	// default thread pool settings
	private static final String DEFAULT_THREADPOOL_NAME = "OSRegistryDefaultThreadPool";
	private static final int DEFAULT_THREADPOOL_MIN_SIZE = 3;
	private static final int DEFAULT_THREADPOOL_MAX_SIZE = 10;
	private DescriptiveStatistics stats = new DescriptiveStatistics();
	
	private Cache<String, OSObject> fCache;
	private CacheEventListener<? super String, ? super OSObject> fOSObjectRegistryListenerMock = mock(OSObjectRegistryListener.class);
	
	
	/**
	 * Creates new OSObject. Each OSObject represents 
	 * entity (i.e. object) that about to be written to object storage.
	 */
	public static OSObject osObjectFactory(final String partitionPath,
         final String objectname, 
         final String fHeaderRow, 
         final int dataIndex, 
         final MetaType dataType,			                     
         final Tuple tuple) {
		
		RollingPolicyType rollingPolicyType = getRollingPolicyType(0, 0, 1000);
		
		OSObject res = new OSObject(
				objectname,
				fHeaderRow, 
				"UTF-8", 
				dataIndex,
				StorageFormat.raw.name());
		
		res.setPartitionPath(partitionPath != null ? partitionPath : "");
		res.setRollingPolicyType(rollingPolicyType.toString());
		
		return res;

	}
	
	private static RollingPolicyType getRollingPolicyType(Integer timePerObject, Integer dataBytesPerObject, Integer tuplesPerObject) {
		if (timePerObject > 0) return RollingPolicyType.TIME;
		if (dataBytesPerObject > 0) return RollingPolicyType.SIZE;
		if (tuplesPerObject > 0) return RollingPolicyType.TUPLES_NUM;
		
		return RollingPolicyType.UNDEFINED;
	}

	
	private static String bigString() {
		String res = "";
		for (int i = 0; i < 1000;i ++) {
			res += "X";
		}
	
		return res;
	}
	
	@Before
	public void init() {
		//initAutoManagedCache();
		initUserManagedCache();
	}
	
	
	public void initUserManagedCache() {
		
		
		CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
 			.newEventListenerConfiguration(fOSObjectRegistryListenerMock,EventType.CREATED , EventType.REMOVED, EventType.EVICTED, EventType.EXPIRED
 			 ) 
 			.ordered().asynchronous();
		
		TuplesPerObjectExpiry expiry = new TuplesPerObjectExpiry(1000);

		OSObjectCacheEventDispatcher<String, OSObject> eventDispatcher = new OSObjectCacheEventDispatcher<String, OSObject>(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5));
		
		UserManagedCacheBuilder<String, OSObject, UserManagedCache<String, OSObject>> umcb = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, OSObject.class)
				.withEventExecutors(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5))
				.withEventDispatcher(eventDispatcher)
				.withEventListeners(cacheEventListenerConfiguration)
				.withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
				.withDispatcherConcurrency(CACHE_DISPATCHER_CONCURRENCY)
				.withExpiry(expiry)									
				.withSizeOfMaxObjectGraph(SIZE_OF_MAX_OBJECT_GRAPH);
		
		fCache = umcb.build(true);
	}
	
	public void initAutoManagedCache() {
		CacheManager fCacheManager = null;		
		CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder().
				using(PooledExecutionServiceConfigurationBuilder.newPooledExecutionServiceConfigurationBuilder().
						defaultPool(DEFAULT_THREADPOOL_NAME, 
								    DEFAULT_THREADPOOL_MIN_SIZE, 
								    DEFAULT_THREADPOOL_MAX_SIZE).
						pool(EVENT_LISTENER_THREADPOOL_NAME, 
							 EVENT_LISTENER_THREADPOOL_MIN_SIZE, 
							 EVENT_LISTENER_THREADPOOL_MAX_SIZE).build()); 

		CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
 			.newEventListenerConfiguration(fOSObjectRegistryListenerMock,EventType.CREATED , EventType.REMOVED, EventType.EVICTED, EventType.EXPIRED
 			 ) 
 			.ordered().asynchronous();
		TuplesPerObjectExpiry expiry = new TuplesPerObjectExpiry(1000);
		
		CacheConfigurationBuilder<String, OSObject> cacheConfigBuilder = 
				CacheConfigurationBuilder.
					newCacheConfigurationBuilder(String.class, OSObject.class, ResourcePoolsBuilder.newResourcePoolsBuilder().
					heap(10, EntryUnit.ENTRIES)).
					withValueSerializer(OSObjectSerializer.class). // use custom serializer for disk			
					withDispatcherConcurrency(CACHE_DISPATCHER_CONCURRENCY).
					withEventListenersThreadPool(EVENT_LISTENER_THREADPOOL_NAME).							
					withExpiry(expiry).										
					withSizeOfMaxObjectGraph(SIZE_OF_MAX_OBJECT_GRAPH)
					.add(cacheEventListenerConfiguration);
	
		// bypass trying to load the Agent entirely 
		System.setProperty(AgentSizeOf.BYPASS_LOADING, "true");		
		
		String fCacheName = "OSObjectCache";
		
		fCacheManager = cacheManagerBuilder.
					withCache(fCacheName , cacheConfigBuilder).build(true);
		fCache = fCacheManager.getCache(fCacheName, String.class, OSObject.class);
	}
	

	
	
	/**
	 * The test uses mock listener that does nothing 
	 * to measure replace operation for growing object 
	 * in EHCache
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test	
	public void replaceTestGrowingObjectWithValidation() throws IOException, InterruptedException { 
		stats = new DescriptiveStatistics();
		
		int objsNum = 100;
		String currPath = null;
		for (int i = 0; i < objsNum; i++) {
			OSObject osObject = TestCache.osObjectFactory("Path" + i, "Obj" + i, null, 0, MetaType.BOOLEAN, null);
			currPath = "Path" + i;
			fCache.put(currPath, osObject);			
			int opCount = 1000;
			for (int j = 0; j < opCount; j++) {
					osObject.fTestDataBuffer.add(TestCache.bigString());					
					long start = System.nanoTime();
					fCache.replace(currPath, osObject);
					long operationTime = System.nanoTime() - start;
					stats.addValue(operationTime);
			}
		}
		
		System.out.println("replaceTestGrowingObject -> mean: " + stats.getMean() + ", std: " + stats.getStandardDeviation() + ", median: " + stats.getPercentile(90));
		assertTrue("Replace operation average performance  is '" + stats.getMean() + "' is slower than 5 microseconds", stats.getMean() < 5000);
	}
}
	