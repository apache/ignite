package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test to validate cache and partition events raised from entry processors executing against
 * partitions are are loading/unloading.
 * <p>
 * The test consists of two parts:
 * <p>
 * 1. The server side that maintains a map of partition id to the set of keys that belong to
 * that partition for all partitions (primary + backup) owned by the server. This map
 * is updated by a local listener registered for the following events:
 * <ul>
 *   <li>EVT_CACHE_OBJECT_PUT</li>
 *   <li>EVT_CACHE_OBJECT_REMOVED</li>
 *   <li>EVT_CACHE_REBALANCE_OBJECT_LOADED</li>
 *   <li>EVT_CACHE_REBALANCE_OBJECT_UNLOADED</li>
 *   <li>EVT_CACHE_REBALANCE_PART_LOADED</li>
 *   <li>EVT_CACHE_REBALANCE_PART_UNLOADED</li>
 *   <li>EVT_CACHE_REBALANCE_PART_DATA_LOST</li>
 * </ul>
 * 2. The client side that generates a random number of keys for each partition and populates
 * the cache. When the cache is loaded, each partition has at least one key assigned to it. The
 * client then issues an {@code invokeAll} on the cache with a key set consisting of one key
 * belonging to each partition.
 * <p>
 * The test makes the following assertions:
 * <ol>
 *     <li>EntryProcessors should execute against partitions that are owned/fully loaded.
 *     If a processor executes against a partition that is partially loaded, the message
 *     "Key validation requires a retry for partitions" is logged on the client, and
 *     "Retrying validation for primary partition N due to newly arrived partition..." and
 *     "Retrying validation for primary partition N due to forming partition..." is logged
 *     on the server side.</li>
 *     <li>Events for entries being added/removed and partitions being loaded/unloaded
 *     should always be delivered to the server nodes that own the partition. If this does
 *     not happen, the client will log "For primary partition N expected [...], but
 *     found [...]; missing local keys: []" and "Key validation failed for partitions: [...]".
 *     The server will log "Retrying validation for primary|backup partition N due to
 *     forming partition" and "For primary|backup partition N expected [...], but found [...];"
 *     </li>
 * </ol>
 */
public class GridCacheRebalancingOrderingTest
        extends GridCommonAbstractTest {

    /** {@link Random} for test key generation. */
    private final static Random RANDOM = new Random();

    /** Test cache name. */
    public static final String TEST_CACHE_NAME = "TestCache";

    /** Flag to configure transactional versus non-transactional cache. */
    public static final boolean TRANSACTIONAL = false;

    /**
     * Test constructor.
     *
     * @throws IgniteCheckedException
     */
    public GridCacheRebalancingOrderingTest()
            throws IgniteCheckedException {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName)
            throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        if (isFirstGrid(gridName)) {
            cfg.setClientMode(true);
            assert cfg.getDiscoverySpi() instanceof TcpDiscoverySpi;
            ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setForceServerMode(true);
        }
        else {
            cfg.setServiceConfiguration(getServiceConfiguration());
        }
        cfg.setCacheConfiguration(getCacheConfiguration());
        return cfg;
    }

    /**
     * @return service configuration for services used in test cluster
     * @see #getConfiguration()
     */
    protected ServiceConfiguration getServiceConfiguration() {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(PartitionObserver.class.getName());
        cfg.setService(new PartitionObserverService());
        cfg.setMaxPerNodeCount(1);
        cfg.setTotalCount(0); // 1 service per node

        return cfg;
    }

    /**
     * @return return cache configuration used by test
     * @see #getConfiguration()
     */
    protected CacheConfiguration<IntegerKey, Integer> getCacheConfiguration() {
        CacheConfiguration<IntegerKey, Integer> config = new CacheConfiguration<>();

        config.setAtomicityMode(TRANSACTIONAL ? CacheAtomicityMode.TRANSACTIONAL : CacheAtomicityMode.ATOMIC);
        config.setCacheMode(CacheMode.PARTITIONED);
        config.setName(TEST_CACHE_NAME);
        config.setAffinity(new RendezvousAffinityFunction(true /* machine-safe */, 271));
        config.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY);
        config.setBackups(1);
        config.setRebalanceMode(CacheRebalanceMode.SYNC);
        config.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return config;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 1000 * 60 * 5;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Convert the given key from binary form, if necessary.
     *
     * @param key the key to convert if necessary
     * @return the key
     */
    private static IntegerKey ensureKey(Object key) {
        Object converted = key instanceof BinaryObject ? ((BinaryObject) key).deserialize() : key;
        return converted instanceof IntegerKey ? (IntegerKey) converted : null;
    }

    /**
     * Determine which of the specified keys (if any) are missing locally from the given cache.
     *
     * @param cache the cache to check
     * @param expected the expected set of keys
     * @return the set of missing keys
     */
    private static Set<IntegerKey> getMissingKeys(IgniteCache<IntegerKey, Integer> cache, Set<IntegerKey> expected) {
        Set<IntegerKey> missing = new HashSet<>();
        for (IntegerKey key : expected) {
            if (cache.localPeek(key, CachePeekMode.ALL) == null) {
                missing.add(key);
            }
        }
        return missing;
    }

    /**
     * For an Ignite cache, generate a random {@link IntegerKey} per partition. The number
     * of partitions is determined by the cache's {@link Affinity}.
     *
     * @param ignite Ignite instance
     * @param cache  cache to generate keys for
     * @return map of partition number to randomly generated key
     */
    private Map<Integer, IntegerKey> generateKeysForPartitions(Ignite ignite, IgniteCache<IntegerKey, Integer> cache) {
        Affinity<IntegerKey> affinity = ignite.affinity(cache.getName());
        int parts = affinity.partitions();
        Map<Integer, IntegerKey> keyMap = new HashMap<>(parts);

        for (int i = 0; i < parts; i++) {
            boolean found = false;
            do {
                IntegerKey key = new IntegerKey(RANDOM.nextInt(10000));
                if (affinity.partition(key) == i) {
                    keyMap.put(i, key);
                    found = true;
                }
            } while (!found);
        }

        // sanity check
        if (keyMap.size() != affinity.partitions()) {
            throw new IllegalStateException("Inconsistent partition count");
        }
        for (int i = 0; i < parts; i++) {
            IntegerKey key = keyMap.get(i);
            if (affinity.partition(key) != i) {
                throw new IllegalStateException("Inconsistent partition");
            }
        }

        return keyMap;
    }

    /**
     * Start background thread that launches servers. This method will block
     * until at least one server is running.
     *
     * @return {@link ServerStarter} runnable that starts servers
     * @throws InterruptedException
     */
    private ServerStarter startServers() throws InterruptedException {
        ServerStarter serverStarter = new ServerStarter();
        Thread t = new Thread(serverStarter);
        t.setDaemon(true);
        t.setName("Server Starter");
        t.start();

        serverStarter.waitForServerStart();
        return serverStarter;
    }

    public void testEvents()
            throws Exception {
        Ignite ignite = startGrid(0);
        ServerStarter serverStarter = startServers();

        IgniteCache<IntegerKey, Integer> cache = ignite.cache(TEST_CACHE_NAME);

        // generate a key per partition
        Map<Integer, IntegerKey> keyMap = generateKeysForPartitions(ignite, cache);

        // populate a random number of keys per partition
        Map<Integer, Set<IntegerKey>> partMap = new HashMap<>(keyMap.size());
        for (Map.Entry<Integer, IntegerKey> entry : keyMap.entrySet()) {
            Integer part = entry.getKey();
            int affinity = entry.getValue().getKey();
            int count = RANDOM.nextInt(10) + 1;
            Set<IntegerKey> keys = new HashSet<>(count);
            for (int i = 0; i < count; i++) {
                IntegerKey key = new IntegerKey(RANDOM.nextInt(10000), affinity);
                keys.add(key);
                cache.put(key, RANDOM.nextInt());
            }
            partMap.put(part, keys);
        }

        // display the partition map
        X.println("Partition Map:");
        for (Map.Entry<Integer, Set<IntegerKey>> entry : partMap.entrySet()) {
            X.println(entry.getKey() + ": " + entry.getValue());
        }

        // validate keys across all partitions
        Affinity<IntegerKey> affinity = ignite.affinity(cache.getName());
        Map<IntegerKey, KeySetValidator> validatorMap = new HashMap<>(partMap.size());
        for (Map.Entry<Integer, Set<IntegerKey>> partEntry : partMap.entrySet()) {
            Integer part = partEntry.getKey();
            validatorMap.put(keyMap.get(part), new KeySetValidator(partEntry.getValue()));
        }
        int i = 0;
        while (!serverStarter.isDone()) {
            Map<IntegerKey, EntryProcessorResult<KeySetValidator.Result>> results = cache.invokeAll(validatorMap);

            Set<Integer> failures = new HashSet<>();
            Set<Integer> retries = new HashSet<>();
            for (Map.Entry<IntegerKey, EntryProcessorResult<KeySetValidator.Result>> result : results.entrySet()) {
                try {
                    if (result.getValue().get() == KeySetValidator.Result.RETRY) {
                        retries.add(affinity.partition(result.getKey()));
                    }
                }
                catch (Exception e) {
                    X.println("!!! " + e.getMessage());
                    e.printStackTrace();
                    failures.add(affinity.partition(result.getKey()));
                }
            }

            if (!failures.isEmpty()) {
                X.println("*** Key validation failed for partitions: " + failures);
                fail("https://issues.apache.org/jira/browse/IGNITE-3456");
            }
            else if (!retries.isEmpty()) {
                X.println("*** Key validation requires a retry for partitions: " + retries);
                retries.clear();
            }
            else {
                X.println("*** Key validation " + i + " was successful");
            }
            i++;
            Thread.sleep(500);
        }
    }

    /**
     * EntryProcessor that validates that the partition associated with the targeted key has a specified set of keys.
     */
    public static class KeySetValidator
            implements EntryProcessor<IntegerKey, Integer, KeySetValidator.Result> {
        private final Set<IntegerKey> keys;

        /**
         * Create a new KeySetValidator.
         *
         * @param keys the expected keys belonging to the partition that owns the targeted key
         */
        public KeySetValidator(Set<IntegerKey> keys) {
            if (keys == null) {
                throw new IllegalArgumentException();
            }
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Result process(MutableEntry<IntegerKey, Integer> entry, Object... objects)
                throws EntryProcessorException {
            try {
                Ignite ignite = entry.unwrap(Ignite.class);
                PartitionObserver observer = ignite.services().service(PartitionObserver.class.getName());
                assertNotNull(observer);

                IgniteCache<IntegerKey, Integer> cache = ignite.cache(TEST_CACHE_NAME);
                Affinity<IntegerKey> affinity = ignite.affinity(TEST_CACHE_NAME);
                Set<IntegerKey> expected = this.keys;
                Set<IntegerKey> missing = getMissingKeys(cache, expected);
                IntegerKey key = entry.getKey();
                int part = affinity.partition(key);
                String ownership = affinity.isPrimary(ignite.cluster().localNode(), key) ? "primary" : "backup";

                // wait for the local listener to sync past events
                if (!observer.getIgniteLocalSyncListener().isSynced()) {
                    X.println(">>> Retrying validation for " + ownership + " partition " + part
                            + " due to initial sync");
                    return Result.RETRY;
                }

                // determine if the partition is being loaded and wait for it to load completely
                if (observer.getLoadingMap().containsKey(part)) {
                    X.println(">>> Retrying validation for " + ownership + " partition " + part
                            + " due to forming partition; expected keys: " + expected + "; loaded keys: "
                            + observer.getLoadingMap().get(part) + "; missing local keys: " + missing);
                    return Result.RETRY;
                }
                if (!observer.getPartitionMap().containsKey(part)) {
                    X.println(">>> Retrying validation for " + ownership + " partition " + part
                            + " due to newly arrived partition; missing local keys: " + missing);
                    return Result.RETRY;
                }

                // validate the key count
                Set<IntegerKey> current = observer.ensureKeySet(part);
                if (current.equals(expected) && missing.isEmpty()) {
                    return Result.OK;
                }

                String message = String.format("For %s partition %s:\n\texpected  %s,\n\t" +
                        "but found %s;\n\tmissing local keys: %s",
                        ownership, part, new TreeSet<>(expected), new TreeSet<>(current), new TreeSet<>(missing));
                X.println(">>> " + message);
                throw new EntryProcessorException(message);
            }
            catch (NullPointerException e) {
                e.printStackTrace();
                throw e;
            }
        }

        enum Result {
            OK,
            RETRY
        }
    }

    /**
     * Integer value that can optionally be associated with another integer.
     */
    public static class IntegerKey implements Comparable<IntegerKey> {
        /**
         * The integer key value.
         */
        private final int value;

        /**
         * The optional associated integer.
         */
        @AffinityKeyMapped
        private final Integer affinity;

        /**
         * Create a new IntegerKey for the given integer value.
         *
         * @param value the integer key value
         */
        public IntegerKey(int value) {
            this.value = value;
            this.affinity = value;
        }

        /**
         * Create a new IntegerKey for the given integer value that is associated with the specified integer.
         *
         * @param value the integer key value
         * @param affinity the associated integer
         */
        public IntegerKey(int value, int affinity) {
            this.value = value;
            this.affinity = affinity;
        }

        /**
         * Return the integer key value.
         *
         * @return the integer key value
         */
        public int getKey() {
            return this.value;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return this.value;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o == null) {
                return false;
            }

            if (IntegerKey.class.equals(o.getClass())) {
                IntegerKey that = (IntegerKey) o;
                return this.value == that.value;
            }
            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            int value = this.value;
            Integer affinity = this.affinity;
            if (value == affinity) {
                return String.valueOf(value);
            }
            return value + ":" + affinity;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(final IntegerKey that) {
            int i = this.affinity.compareTo(that.affinity);
            if (i == 0) {
                i = Integer.compare(this.getKey(), that.getKey());
            }
            return i;
        }
    }

    /**
     * Local listener wrapper that brings a delegate listener up to date with the latest events.
     */
    public static class IgniteLocalSyncListener
            implements IgnitePredicate<Event> {
        private final IgnitePredicate<Event> delegate;
        private final int[] causes;

        private volatile boolean isSynced;
        private volatile long syncedId = Long.MIN_VALUE;

        public IgniteLocalSyncListener(IgnitePredicate<Event> delegate, int... causes) {
            this.delegate = delegate;
            this.causes = causes;
        }

        protected Ignite ignite() {
            return Ignition.localIgnite();
        }

        public void register() {
            ignite().events().localListen(this.delegate, this.causes);
            sync();
        }

        public void sync() {
            if (!this.isSynced) {
                synchronized (this) {
                    if (!this.isSynced) {
                        Collection<Event> events = ignite().events().localQuery(new IgnitePredicate<Event>() {
                                    @Override
                                    public boolean apply(final Event event) {
                                        return true;
                                    }
                                }, this.causes);

                        for (Event event : events) {
                            // Events returned from localQuery() are ordered by increasing local ID. Update the sync ID
                            // within a finally block to avoid applying duplicate events if the delegate listener throws an
                            // exception while processing the event.
                            try {
                                applyInternal(event);
                            }
                            finally {
                                this.syncedId = event.localOrder();
                            }
                        }
                        this.isSynced = true;
                        notifyAll();
                    }
                }
            }
        }

        public boolean isSynced() {
            return this.isSynced;
        }

        @Override public boolean apply(Event event) {
            sync();
            return applyInternal(event);
        }

        protected boolean applyInternal(Event event) {
            // avoid applying previously recorded events
            if (event.localOrder() > this.syncedId) {
                try {
                    return this.delegate.apply(event);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Service interface for server side partition observation.
     */
    interface PartitionObserver {

        /**
         * @return map of partitions to the keys belonging to that partition
         */
        ConcurrentMap<Integer, Set<IntegerKey>> getPartitionMap();

        /**
         * Map of partitions that are in the process of loading and the current keys that belong to that partition.
         * Currently it seems that an EntryProcessor is not guaranteed to have a "stable" view of a partition and
         * can see entries as they are being loaded into the partition, so we must batch these events up in the map
         * and update the {@link #getPartitionMap() partition map} atomically once the partition has been fully loaded.
         *
         * @return
         */
        ConcurrentMap<Integer, Set<IntegerKey>> getLoadingMap();

        /**
         * Ensure that the {@link #getPartitionMap() partition map} has a set of keys associated with the given
         * partition, creating one if it doesn't already exist.
         * @param part the partition
         * @return the set for the given partition
         */
        Set<IntegerKey> ensureKeySet(int part);

        /**
         * @return listener wrapper that brings a delegate listener up to date with the latest events
         */
        IgniteLocalSyncListener getIgniteLocalSyncListener();
    }

    public static class PartitionObserverService implements Service, PartitionObserver, Serializable {

        private final ConcurrentMap<Integer, Set<IntegerKey>> partitionMap = new ConcurrentHashMap<>();

        private final ConcurrentMap<Integer, Set<IntegerKey>> loadingMap = new ConcurrentHashMap<>();

        private final IgnitePredicate<Event> predicate = (IgnitePredicate<Event>) new IgnitePredicate<Event>() {
            @Override
            public boolean apply(Event event) {
                // handle:
                // EVT_CACHE_OBJECT_PUT
                // EVT_CACHE_REBALANCE_OBJECT_LOADED
                // EVT_CACHE_OBJECT_REMOVED
                // EVT_CACHE_REBALANCE_OBJECT_UNLOADED
                if (event instanceof CacheEvent) {
                    CacheEvent cacheEvent = (CacheEvent) event;
                    int part = cacheEvent.partition();

                    // only handle events for the test cache
                    if (TEST_CACHE_NAME.equals(cacheEvent.cacheName())) {
                        switch (event.type()) {
                            case EventType.EVT_CACHE_OBJECT_PUT: {
                                ensureKeySet(part).add(ensureKey(cacheEvent.key()));
                                break;
                            }

                            case EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED: {
                                // batch up objects that are being loaded
                                ensureKeySet(part, loadingMap).add(ensureKey(cacheEvent.key()));
                                break;
                            }

                            case EventType.EVT_CACHE_OBJECT_REMOVED:
                            case EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED: {
                                ensureKeySet(part).remove(ensureKey(cacheEvent.key()));
                                break;
                            }
                        }
                    }
                }

                // handle:
                // EVT_CACHE_REBALANCE_PART_LOADED
                // EVT_CACHE_REBALANCE_PART_UNLOADED
                // EVT_CACHE_REBALANCE_PART_DATA_LOST
                else if (event instanceof CacheRebalancingEvent) {
                    CacheRebalancingEvent rebalancingEvent = (CacheRebalancingEvent) event;
                    int part = rebalancingEvent.partition();

                    // only handle events for the test cache
                    if (TEST_CACHE_NAME.equals(rebalancingEvent.cacheName())) {
                        switch (event.type()) {
                            case EventType.EVT_CACHE_REBALANCE_PART_UNLOADED: {
                                Set<IntegerKey> keys = partitionMap.get(part);
                                if (keys != null && !keys.isEmpty()) {
                                    X.println("!!! Attempting to unload non-empty partition: " + part + "; keys=" + keys);
                                }
                                partitionMap.remove(part);
                                X.println("*** Unloaded partition: " + part);
                                break;
                            }

                            case EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST: {
                                partitionMap.remove(part);
                                X.println("*** Lost partition: " + part);
                                break;
                            }

                            case EventType.EVT_CACHE_REBALANCE_PART_LOADED: {
                                // atomically update the key count for the new partition
                                Set<IntegerKey> keys = loadingMap.get(part);
                                partitionMap.put(part, keys);
                                loadingMap.remove(part);
                                X.println("*** Loaded partition: " + part + "; keys=" + keys);
                                break;
                            }
                        }
                    }
                }

                return true;
            }
        };

        private final IgniteLocalSyncListener listener = new IgniteLocalSyncListener(predicate,
            EventType.EVT_CACHE_OBJECT_PUT, EventType.EVT_CACHE_OBJECT_REMOVED,
            EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED, EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED,
            EventType.EVT_CACHE_REBALANCE_PART_LOADED, EventType.EVT_CACHE_REBALANCE_PART_UNLOADED,
            EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        @Override
        public ConcurrentMap<Integer, Set<IntegerKey>> getPartitionMap() {
            return partitionMap;
        }

        @Override
        public ConcurrentMap<Integer, Set<IntegerKey>> getLoadingMap() {
            return loadingMap;
        }

        @Override
        public IgniteLocalSyncListener getIgniteLocalSyncListener() {
            return listener;
        }

        /**
         * Ensure that the static partition map has a set of keys associated with the given partition, creating one if it
         * doesn't already exist.
         *
         * @param part the partition
         * @return the set for the given partition
         */
        public Set<IntegerKey> ensureKeySet(final int part) {
            return ensureKeySet(part, partitionMap);
        }

        /**
         * Ensure that the given partition map has a set of keys associated with the given partition, creating one if it
         * doesn't already exist.
         *
         * @param part the partition
         * @param map the partition map
         *
         * @return the set for the given partition
         */
        public Set<IntegerKey> ensureKeySet(final int part, final ConcurrentMap<Integer, Set<IntegerKey>> map) {
            Set<IntegerKey> keys = map.get(part);
            if (keys == null) {
                map.putIfAbsent(part, new CopyOnWriteArraySet<IntegerKey>());
                keys = map.get(part);
            }
            return keys;
        }


        @Override
        public void cancel(final ServiceContext ctx) {
        }

        @Override
        public void init(final ServiceContext ctx) throws Exception {
            this.listener.register();
        }

        @Override
        public void execute(final ServiceContext ctx) throws Exception {
        }
    }

    /**
     * Runnable that starts {@value #SERVER_COUNT} servers. This runnable starts
     * servers every {@value #START_DELAY} milliseconds. The staggered start is intended
     * to allow partitions to move every time a new server is started.
     */
    public class ServerStarter
            implements Runnable {
        public static final int SERVER_COUNT = 10;

        public static final int START_DELAY = 2000;

        private volatile boolean done = false;

        private final CountDownLatch started = new CountDownLatch(1);

        @Override public void run() {
            try {
                for (int i = 1; i <= SERVER_COUNT; i++) {
                    startGrid(i);
                    Thread.sleep(START_DELAY);
                    awaitPartitionMapExchange();
                    this.started.countDown();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                X.println("Shutting down server starter thread");
            }
            finally {
                done = true;
            }
        }

        /**
         * Block the executing thread until at least one server has started.
         * @throws InterruptedException
         */
        public void waitForServerStart() throws InterruptedException {
            started.await(getTestTimeout(), TimeUnit.MILLISECONDS);
        }

        /** @return true if {@value #SERVER_COUNT} servers have started. */
        public boolean isDone() {
            return done;
        }
    }
}
