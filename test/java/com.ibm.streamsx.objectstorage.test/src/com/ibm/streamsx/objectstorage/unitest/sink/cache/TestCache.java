package com.ibm.streamsx.objectstorage.unitest.sink.cache;

public class TestCache {

//OSRegistry allows to utilize 70% of operator's JVM heap
	private static final double OSREGISTRY_MEMORY_PORTION = 0.7;
	// dispatcher concurrency
	private static final int CACHE_DISPATCHER_CONCURRENCY = 2;
	
	private static final long SIZE_OF_MAX_OBJECT_GRAPH = 1024 * 512;
	
	// event listener pool settings
	private static final String EVENT_LISTENER_THREADPOOL_NAME = "EventListenerThreadPool";
	private static final int EVENT_LISTENER_THREADPOOL_MIN_SIZE = 1;
	private static final int EVENT_LISTENER_THREADPOOL_MAX_SIZE = 1;
	
	// disk tier settings
	private static final int DISK_HEAP_MAX_CACHE_SIZE_GB = 1;
	private static final String DISK_CACHE_DIR = "/tmp/objectStorageRegistryCache";
	private static final String DISK_STORE_THREADPOOL_NAME = "DiskStoreThreadPool";
	private static final int DISK_STORE_THREADPOOL_MIN_SIZE = 0;
	private static final int DISK_STORE_THREADPOOL_MAX_SIZE = 1;
	
	// default thread pool settings
	private static final String DEFAULT_THREADPOOL_NAME = "OSRegistryDefaultThreadPool";
	private static final int DEFAULT_THREADPOOL_MIN_SIZE = 3;
	private static final int DEFAULT_THREADPOOL_MAX_SIZE = 10;
	//private static final int DEFAULT_THREADPOOL_MIN_SIZE = 3;
	//rivate static final int DEFAULT_THREADPOOL_MAX_SIZE = 10;
	private DescriptiveStatistics stats = new DescriptiveStatistics();
	
 private final ExecutorService testExecutorService = new ThreadPoolExecutor(1,1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

	
 
 
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

	private Cache<String, OSObject> fCache;
	private Cache<String, String> fCacheStr;
	private HashMap<String, OSObject> fHashMap;
	private CacheEventListener<? super String, ? super OSObject> fOSObjectRegistryListener = new OSObjectRegistryListener(null);
	
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
 			.newEventListenerConfiguration(fOSObjectRegistryListener,EventType.CREATED , EventType.REMOVED, EventType.EVICTED, EventType.EXPIRED
 			 ) 
 			.ordered().asynchronous();
		
		TuplesPerObjectExpiry expiry = new TuplesPerObjectExpiry(1000);

		MyCacheEventDispatcherImpl2<String, OSObject> eventDispatcher = new MyCacheEventDispatcherImpl2<String, OSObject>(testExecutorService, testExecutorService);
		//MyCacheEventDispatcherImpl<String, OSObject> eventDispatcher = new MyCacheEventDispatcherImpl<String, OSObject>(testExecutorService, testExecutorService);
		//MyCacheEventDispatcherImpl<String, OSObject> eventDispatcher = new MyCacheEventDispatcherImpl<String, OSObject>(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5));
		//CacheEventDispatcherImpl<String, OSObject> eventDispatcher = new CacheEventDispatcherImpl<String, OSObject>(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5));
		
		UserManagedCacheBuilder<String, OSObject, UserManagedCache<String, OSObject>> umcb = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, OSObject.class)
//				.withEventExecutors(Executors.newSingleThreadExecutor(), Executors.newFixedThreadPool(5))
				.withEventExecutors(testExecutorService, testExecutorService)
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
 			.newEventListenerConfiguration(fOSObjectRegistryListener,EventType.CREATED , EventType.REMOVED, EventType.EVICTED, EventType.EXPIRED
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
	

	
	//@Test
	public void replaceTestGrowingObjectWithValidation() throws IOException, InterruptedException { 
		stats = new DescriptiveStatistics();
		
		int objsNum = 100;
		String currPath = null;
		for (int i = 0; i < objsNum; i++) {
			OSObject osObject = EHCacheTest.osObjectFactory("Path" + i, "Obj" + i, null, 0, MetaType.BOOLEAN, null);
			currPath = "Path" + i;
			fCache.put(currPath, osObject);			
			int opCount = 1000;
			for (int j = 0; j < opCount; j++) {
					osObject.fTestDataBuffer.add(EHCacheTest2.bigString());					
					long start = System.nanoTime();
					fCache.replace(currPath, osObject);
					long operationTime = System.nanoTime() - start;
					stats.addValue(operationTime);
			}
		}
		System.out.println("replaceTestGrowingObject -> mean: " + stats.getMean() + ", std: " + stats.getStandardDeviation() + ", median: " + stats.getPercentile(90));
	}
	
	@Test
	public void executorTest() throws IOException, InterruptedException {
		stats = new DescriptiveStatistics();

		int objsNum = 1000;
		for (int i = 0; i < objsNum; i++) {
			long start = System.nanoTime();
			testExecutorService.submit(new MyEventDispatchTask(null, null));
			long operationTime = System.nanoTime() - start;
			stats.addValue(operationTime);
		}		
		System.out.println("executorTest -> mean: " + stats.getMean() + ", std: " + stats.getStandardDeviation() + ", median: " + stats.getPercentile(90));
	}
	

	public static void main(String[] args) {
		EHCacheTest2 test = new EHCacheTest2();
		try {
			test.init();
			//test.createEntryWithListener();
			test.replaceTestGrowingObjectWithValidation();
			//test.executorTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
	