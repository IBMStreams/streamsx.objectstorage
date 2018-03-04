package com.ibm.streamsx.objectstorage.unitest.sink.standalone;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Random;
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
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.statistics.TierStatistics;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.impl.internal.statistics.DefaultStatisticsService;
import org.ehcache.sizeof.impl.AgentSizeOf;
import org.junit.Before;
import org.junit.Test;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.objectstorage.BaseObjectStorageSink;
import com.ibm.streamsx.objectstorage.internal.sink.OSObject;
import com.ibm.streamsx.objectstorage.internal.sink.OSObjectCacheEventDispatcher;
import com.ibm.streamsx.objectstorage.internal.sink.OSObjectSerializer;
import com.ibm.streamsx.objectstorage.internal.sink.StorageFormat;
import com.ibm.streamsx.objectstorage.internal.sink.TuplesPerObjectExpiry;

public class TestCachePerformance {

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
	
	private Cache<String, OSObject> fCache;
	//private CacheEventListener<? super String, ? super OSObject> fOSObjectRegistryListenerMock = mock(OSObjectRegistryListener.class);
	private CacheEventListener<? super String, ? super OSObject> fOSObjectRegistryListenerMock = new TestListener();
	private BaseObjectStorageSink fOSSinkOp =   mock(BaseObjectStorageSink.class);
	private StatisticsService fStatisticsService;

	
	
	@Before
	public void init() {
		//initAutoManagedCache();
//		initUserManagedCache();
		//osSinkOp = mock(BaseObjectStorageSink.class);
		//fOSObjectRegistryListenerMock = new OSObjectRegistryListener(null);
		initUserManagedCacheWithStats();
	}
	
	
	public void initUserManagedCache() {
		CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
 			.newEventListenerConfiguration(fOSObjectRegistryListenerMock,EventType.CREATED , EventType.REMOVED, EventType.EVICTED, EventType.EXPIRED
 			 ) 
 			.ordered().asynchronous();
		
		TuplesPerObjectExpiry expiry = new TuplesPerObjectExpiry(1000);

//		OSObjectCacheEventDispatcher<String, OSObject> eventDispatcher = 
//				new OSObjectCacheEventDispatcher<String, OSObject>(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5), fOSSinkOp);
		OSObjectCacheEventDispatcher<String, OSObject> eventDispatcher = 
		new OSObjectCacheEventDispatcher<String, OSObject>(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5));
		
		UserManagedCacheBuilder<String, OSObject, UserManagedCache<String, OSObject>> umcb = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, OSObject.class)
				.withEventExecutors(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5))
				.withEventDispatcher(eventDispatcher)
				.withEventListeners(cacheEventListenerConfiguration)
				.withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
				//.withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(400, MemoryUnit.MB))
				.withDispatcherConcurrency(CACHE_DISPATCHER_CONCURRENCY)
				.withExpiry(expiry)									
				.withSizeOfMaxObjectGraph(SIZE_OF_MAX_OBJECT_GRAPH);
		
		fCache = umcb.build(true);
	}

	public void initUserManagedCacheWithStats() {
		fStatisticsService = new DefaultStatisticsService();

		CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
	 			.newEventListenerConfiguration(fOSObjectRegistryListenerMock,EventType.CREATED , EventType.REMOVED, EventType.EVICTED, EventType.EXPIRED
	 			 ) 
	 			.ordered().asynchronous();
			
			TuplesPerObjectExpiry expiry = new TuplesPerObjectExpiry(1000);

			OSObjectCacheEventDispatcher<String, OSObject> eventDispatcher = 
			new OSObjectCacheEventDispatcher<String, OSObject>(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5));
			
			UserManagedCacheBuilder<String, OSObject, UserManagedCache<String, OSObject>> umcb = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, OSObject.class)
					.using(fStatisticsService)
					.withEventExecutors(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5))
					.withEventDispatcher(eventDispatcher)
					.withEventListeners(cacheEventListenerConfiguration)
					.withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
					//.withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(400, MemoryUnit.MB))
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
		DescriptiveStatistics stats = new DescriptiveStatistics();
		Random r = new Random();
		TierStatistics tierStatistics;
		
		int objsNum = 100;
		int recordsCountPerObject = 1000;
		String currPath = null;
		for (int i = 0; i < objsNum; i++) {
			OSObject osObject = Utils.osObjectFactory("Path" + i, "Obj" + i, null, 0, MetaType.BOOLEAN, null, StorageFormat.parquet);
			currPath = "Path" + i;
			fCache.put(currPath, osObject);						
			for (int j = 0; j < recordsCountPerObject; j++) {
					// the real tuple type is InputTuple.
					// As input tuple can't be instantiated using
				    // output tuple as a base type for buffer content.
					OutputTuple tupleMock = mock(OutputTuple.class);
					tupleMock.setString("id", Utils.genBuffer());
					tupleMock.setString("tz", Utils.genBuffer());
					tupleMock.setString("dateutc", Utils.getCurrentTimestamp());
					tupleMock.setString("time_stamp", Utils.getCurrentTimestamp());
					tupleMock.setDouble("longitude", Utils.genRandomLon(r));
					tupleMock.setDouble("latitude", Utils.genRandomLat(r));
					tupleMock.setDouble("temperature", Utils.genRandomTemperature(r));
					tupleMock.setDouble("baromin", Utils.genRandomBaromin(r));
					tupleMock.setDouble("humidity", Utils.genRandomHumidity(r));
					tupleMock.setDouble("rainin", Utils.genRandomRainin(r));
					osObject.getDataBuffer().add(tupleMock);
					long start = System.nanoTime();
					fCache.replace(currPath, osObject);
					long operationTime = System.nanoTime() - start;
					stats.addValue(operationTime);
					
					tierStatistics = fStatisticsService
						      .getCacheStatistics("OSObjectCache")
						      .getTierStatistics()
						      .get("Heap");
					System.out.println("Occupied: " + tierStatistics.getOccupiedByteSize());
					System.out.println("Allocated: " + tierStatistics.getAllocatedByteSize());
			}
		}
		
		System.out.println("replaceTestGrowingObject -> mean: " + stats.getMean() + " nanoseconds, std: " + stats.getStandardDeviation() + "nanoseconds, median: " + stats.getPercentile(90) + " nanoseconds");
		System.out.println("cache perforemance -> mean " + (stats.getMean() / (objsNum * recordsCountPerObject)) * 1000000000 + " replace operations per second, median: " +  (stats.getPercentile(90) / (objsNum * recordsCountPerObject)) * 1000000000 + " replace operations per sec");
		assertTrue("Replace operation average performance  is '" + stats.getMean() + "' is slower than 5 microseconds", stats.getMean() < 5000);
	}
}
	