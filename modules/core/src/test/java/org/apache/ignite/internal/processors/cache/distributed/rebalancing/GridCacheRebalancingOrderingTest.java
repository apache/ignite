/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
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
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

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
public class GridCacheRebalancingOrderingTest extends GridCommonAbstractTest {
    /** {@link Random} for test key generation. */
    private static final Random RANDOM = new Random();

    /** Test cache name. */
    private static final String TEST_CACHE_NAME = "TestCache";

    /** Flag to configure transactional versus non-transactional cache. */
    public static final boolean TRANSACTIONAL = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (isFirstGrid(igniteInstanceName)) {
            assert cfg.getDiscoverySpi() instanceof TcpDiscoverySpi : cfg.getDiscoverySpi();

            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        }
        else
            cfg.setServiceConfiguration(getServiceConfiguration());

        cfg.setCacheConfiguration(getCacheConfiguration());

        return cfg;
    }

    /**
     * @return service configuration for services used in test cluster
     * @see #getConfiguration()
     */
    private ServiceConfiguration getServiceConfiguration() {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(PartitionObserver.class.getName());
        cfg.setService(new PartitionObserverService());
        cfg.setMaxPerNodeCount(1);
        cfg.setTotalCount(0); // 1 service per node.

        return cfg;
    }

    /**
     * @return Cache configuration used by test.
     * @see #getConfiguration().
     */
    protected CacheConfiguration<IntegerKey, Integer> getCacheConfiguration() {
        CacheConfiguration<IntegerKey, Integer> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setAtomicityMode(TRANSACTIONAL ? CacheAtomicityMode.TRANSACTIONAL : CacheAtomicityMode.ATOMIC);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName(TEST_CACHE_NAME);
        cfg.setAffinity(new RendezvousAffinityFunction(true /* machine-safe */, 271));
        cfg.setBackups(1);
        cfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 1000 * 60 * 5;
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
     * @param cache The cache to check.
     * @param exp The expected set of keys.
     * @return The set of missing keys.
     */
    private static Set<IntegerKey> getMissingKeys(IgniteCache<IntegerKey, Integer> cache, Set<IntegerKey> exp) {
        Set<IntegerKey> missing = new HashSet<>();

        for (IntegerKey key : exp) {
            if (cache.localPeek(key, CachePeekMode.ALL) == null)
                missing.add(key);
        }

        return missing;
    }

    /**
     * For an Ignite cache, generate a random {@link IntegerKey} per partition. The number
     * of partitions is determined by the cache's {@link Affinity}.
     *
     * @param ignite Ignite instance.
     * @param cache  Cache to generate keys for.
     * @return Map of partition number to randomly generated key.
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

        // Sanity check.
        if (keyMap.size() != affinity.partitions())
            throw new IllegalStateException("Inconsistent partition count");

        for (int i = 0; i < parts; i++) {
            IntegerKey key = keyMap.get(i);

            if (affinity.partition(key) != i)
                throw new IllegalStateException("Inconsistent partition");
        }

        return keyMap;
    }

    /**
     * Starts background thread that launches servers. This method will block
     * until at least one server is running.
     *
     * @return {@link ServerStarter} runnable that starts servers
     * @throws Exception If failed.
     */
    private ServerStarter startServers() throws Exception {
        ServerStarter srvStarter = new ServerStarter();

        Thread t = new Thread(srvStarter);
        t.setDaemon(true);
        t.setName("Server Starter");
        t.start();

        srvStarter.waitForServerStart();

        return srvStarter;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEvents() throws Exception {
        Ignite ignite = startClientGrid(0);

        ServerStarter srvStarter = startServers();

        IgniteCache<IntegerKey, Integer> cache = ignite.cache(TEST_CACHE_NAME);

        // Generate a key per partition.
        Map<Integer, IntegerKey> keyMap = generateKeysForPartitions(ignite, cache);

        // Populate a random number of keys per partition.
        Map<Integer, Set<IntegerKey>> partMap = new HashMap<>(keyMap.size());

        for (Map.Entry<Integer, IntegerKey> entry : keyMap.entrySet()) {
            Integer part = entry.getKey();
            int affinity = entry.getValue().getKey();
            int cnt = RANDOM.nextInt(10) + 1;

            Set<IntegerKey> keys = new HashSet<>(cnt);

            for (int i = 0; i < cnt; i++) {
                IntegerKey key = new IntegerKey(RANDOM.nextInt(10000), affinity);
                keys.add(key);
                cache.put(key, RANDOM.nextInt());
            }

            partMap.put(part, keys);
        }

        // Display the partition map.
        X.println("Partition Map:");

        for (Map.Entry<Integer, Set<IntegerKey>> entry : partMap.entrySet())
            X.println(entry.getKey() + ": " + entry.getValue());

        // Validate keys across all partitions.
        Affinity<IntegerKey> affinity = ignite.affinity(cache.getName());

        Map<IntegerKey, KeySetValidator> validatorMap = new HashMap<>(partMap.size());

        for (Map.Entry<Integer, Set<IntegerKey>> partEntry : partMap.entrySet()) {
            Integer part = partEntry.getKey();

            validatorMap.put(keyMap.get(part), new KeySetValidator(partEntry.getValue()));
        }

        int i = 0;

        while (!srvStarter.isDone()) {
            Map<IntegerKey, EntryProcessorResult<KeySetValidator.Result>> results = cache.invokeAll(validatorMap);

            Set<Integer> failures = new HashSet<>();
            Set<Integer> retries = new HashSet<>();

            for (Map.Entry<IntegerKey, EntryProcessorResult<KeySetValidator.Result>> result : results.entrySet()) {
                try {
                    if (result.getValue().get() == KeySetValidator.Result.RETRY)
                        retries.add(affinity.partition(result.getKey()));
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
            else
                X.println("*** Key validation was successful: " + i);

            i++;

            Thread.sleep(500);
        }
    }

    /**
     * EntryProcessor that validates that the partition associated with the targeted key has a specified set of keys.
     */
    public static class KeySetValidator implements EntryProcessor<IntegerKey, Integer, KeySetValidator.Result> {
        /** */
        private final Set<IntegerKey> keys;

        /**
         * Create a new KeySetValidator.
         *
         * @param keys the expected keys belonging to the partition that owns the targeted key
         */
        KeySetValidator(Set<IntegerKey> keys) {
            if (keys == null)
                throw new IllegalArgumentException();

            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Result process(MutableEntry<IntegerKey, Integer> entry, Object... objects) {
            try {
                Ignite ignite = entry.unwrap(Ignite.class);

                PartitionObserver observer = ignite.services().service(PartitionObserver.class.getName());

                assertNotNull(observer);

                IgniteCache<IntegerKey, Integer> cache = ignite.cache(TEST_CACHE_NAME);

                Affinity<IntegerKey> affinity = ignite.affinity(TEST_CACHE_NAME);

                Set<IntegerKey> exp = this.keys;

                Set<IntegerKey> missing = getMissingKeys(cache, exp);

                IntegerKey key = entry.getKey();

                int part = affinity.partition(key);

                String ownership = affinity.isPrimary(ignite.cluster().localNode(), key) ? "primary" : "backup";

                // Wait for the local listener to sync past events.
                if (!observer.getIgniteLocalSyncListener().isSynced()) {
                    ignite.log().info("Retrying validation for " + ownership + " partition " + part
                            + " due to initial sync");

                    return Result.RETRY;
                }

                // Determine if the partition is being loaded and wait for it to load completely.
                if (observer.getLoadingMap().containsKey(part)) {
                    ignite.log().info("Retrying validation due to forming partition [ownership=" + ownership +
                        ", partition=" + part +
                        ", expKeys=" + exp +
                        ", loadedKeys=" + observer.getLoadingMap().get(part) +
                        ", missingLocalKeys=" + missing + ']');

                    return Result.RETRY;
                }

                if (!observer.getPartitionMap().containsKey(part)) {
                    ignite.log().info("Retrying validation due to newly arrived partition [ownership=" + ownership +
                        ", partition=" + part +
                        ", missingLocalKeys=" + missing + ']');

                    return Result.RETRY;
                }

                // Validate the key count.
                Set<IntegerKey> curr = observer.ensureKeySet(part);

                if (curr.equals(exp) && missing.isEmpty())
                    return Result.OK;

                String msg = String.format("For %s partition %s:\n\texpected  %s,\n\t" +
                    "but found %s;\n\tmissing local keys: %s",
                    ownership, part, new TreeSet<>(exp), new TreeSet<>(curr), new TreeSet<>(missing));

                ignite.log().info(">>> " + msg);

                throw new EntryProcessorException(msg);
            }
            catch (NullPointerException e) {
                e.printStackTrace();

                throw e;
            }
        }

        /**
         *
         */
        enum Result {
            /** */
            OK,
            /** */
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
        private final int val;

        /**
         * The optional associated integer.
         */
        @AffinityKeyMapped
        private final Integer affinity;

        /**
         * Create a new IntegerKey for the given integer value.
         *
         * @param val the integer key value
         */
        IntegerKey(int val) {
            this.val = val;
            this.affinity = val;
        }

        /**
         * Create a new IntegerKey for the given integer value that is associated with the specified integer.
         *
         * @param val the integer key value
         * @param affinity the associated integer
         */
        IntegerKey(int val, int affinity) {
            this.val = val;
            this.affinity = affinity;
        }

        /**
         * Return the integer key value.
         *
         * @return the integer key value
         */
        public int getKey() {
            return this.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return this.val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (o == this)
                return true;
            if (o == null)
                return false;

            if (IntegerKey.class.equals(o.getClass())) {
                IntegerKey that = (IntegerKey) o;
                return this.val == that.val;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            int val = this.val;
            Integer affinity = this.affinity;

            if (val == affinity)
                return String.valueOf(val);

            return "IntKey [val=" + val + ", aff=" + affinity + ']';
        }

        /** {@inheritDoc} */
        @Override public int compareTo(final IntegerKey that) {
            int i = this.affinity.compareTo(that.affinity);

            if (i == 0)
                i = Integer.compare(this.getKey(), that.getKey());

            return i;
        }
    }

    /**
     * Local listener wrapper that brings a delegate listener up to date with the latest events.
     */
    private static class IgniteLocalSyncListener implements IgnitePredicate<Event> {
        /** */
        private final IgnitePredicate<Event> delegate;

        /** */
        private final int[] causes;

        /** */
        private volatile boolean isSynced;

        /** */
        private volatile long syncedId = Long.MIN_VALUE;

        /**
         * @param delegate Event listener.
         * @param causes Event types to listen.
         */
        IgniteLocalSyncListener(IgnitePredicate<Event> delegate, int... causes) {
            this.delegate = delegate;
            this.causes = causes;
        }

        /**
         * @return Local ignite.
         */
        protected Ignite ignite() {
            return Ignition.localIgnite();
        }

        /**
         *
         */
        public void register() {
            ignite().events().localListen(this.delegate, this.causes);

            sync();
        }

        /**
         *
         */
        public void sync() {
            if (!this.isSynced) {
                synchronized (this) {
                    if (!this.isSynced) {
                        Collection<Event> evts = ignite().events().localQuery(new IgnitePredicate<Event>() {
                                @Override public boolean apply(final Event evt) {
                                    return true;
                                }
                            },
                            this.causes);

                        for (Event event : evts) {
                            // Events returned from localQuery() are ordered by increasing local ID. Update the sync ID
                            // within a finally block to avoid applying duplicate events if the delegate listener
                            // throws an exception while processing the event.
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

        /**
         * @return Synced flag.
         */
        boolean isSynced() {
            return isSynced;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            sync();

            return applyInternal(evt);
        }

        /**
         * @param evt Event.
         * @return See {@link IgniteEvents#localListen}.
         */
        boolean applyInternal(Event evt) {
            // Avoid applying previously recorded events.
            if (evt.localOrder() > this.syncedId) {
                try {
                    return this.delegate.apply(evt);
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
         * @return Map of partitions that are in the process of loading and the current keys that belong to that partition.
         * Currently it seems that an EntryProcessor is not guaranteed to have a "stable" view of a partition and
         * can see entries as they are being loaded into the partition, so we must batch these events up in the map
         * and update the {@link #getPartitionMap() partition map} atomically once the partition has been fully loaded.
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

    /**
     *
     */
    private static class PartitionObserverService implements Service, PartitionObserver, Serializable {
        /** */
        private final ConcurrentMap<Integer, Set<IntegerKey>> partMap = new ConcurrentHashMap<>();

        /** */
        private final ConcurrentMap<Integer, Set<IntegerKey>> loadingMap = new ConcurrentHashMap<>();

        /** */
        private final IgnitePredicate<Event> pred = (IgnitePredicate<Event>) new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                // Handle:
                // EVT_CACHE_OBJECT_PUT
                // EVT_CACHE_REBALANCE_OBJECT_LOADED
                // EVT_CACHE_OBJECT_REMOVED
                // EVT_CACHE_REBALANCE_OBJECT_UNLOADED
                if (evt instanceof CacheEvent) {
                    CacheEvent cacheEvt = (CacheEvent) evt;
                    int part = cacheEvt.partition();

                    // Oonly handle events for the test cache.
                    if (TEST_CACHE_NAME.equals(cacheEvt.cacheName())) {
                        switch (evt.type()) {
                            case EventType.EVT_CACHE_OBJECT_PUT: {
                                ensureKeySet(part).add(ensureKey(cacheEvt.key()));
                                break;
                            }

                            case EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED: {
                                // Batch up objects that are being loaded.
                                ensureKeySet(part, loadingMap).add(ensureKey(cacheEvt.key()));
                                break;
                            }

                            case EventType.EVT_CACHE_OBJECT_REMOVED:
                            case EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED: {
                                ensureKeySet(part).remove(ensureKey(cacheEvt.key()));
                                break;
                            }
                        }
                    }
                }
                // Handle:
                // EVT_CACHE_REBALANCE_PART_LOADED
                // EVT_CACHE_REBALANCE_PART_UNLOADED
                // EVT_CACHE_REBALANCE_PART_DATA_LOST
                else if (evt instanceof CacheRebalancingEvent) {
                    CacheRebalancingEvent rebalancingEvt = (CacheRebalancingEvent) evt;

                    int part = rebalancingEvt.partition();

                    // Only handle events for the test cache.
                    if (TEST_CACHE_NAME.equals(rebalancingEvt.cacheName())) {
                        switch (evt.type()) {
                            case EventType.EVT_CACHE_REBALANCE_PART_UNLOADED: {
                                Set<IntegerKey> keys = partMap.get(part);

                                if (keys != null && !keys.isEmpty())
                                    X.println("!!! Attempting to unload non-empty partition: " + part + "; keys=" + keys);

                                partMap.remove(part);

                                X.println("*** Unloaded partition: " + part);

                                break;
                            }

                            case EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST: {
                                partMap.remove(part);

                                X.println("*** Lost partition: " + part);

                                break;
                            }

                            case EventType.EVT_CACHE_REBALANCE_PART_LOADED: {
                                // Atomically update the key count for the new partition.
                                Set<IntegerKey> keys = loadingMap.get(part);
                                partMap.put(part, keys);
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

        /** */
        private final IgniteLocalSyncListener lsnr = new IgniteLocalSyncListener(pred,
            EventType.EVT_CACHE_OBJECT_PUT,
            EventType.EVT_CACHE_OBJECT_REMOVED,
            EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED,
            EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED,
            EventType.EVT_CACHE_REBALANCE_PART_LOADED,
            EventType.EVT_CACHE_REBALANCE_PART_UNLOADED,
            EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        /** {@inheritDoc} */
        @Override public ConcurrentMap<Integer, Set<IntegerKey>> getPartitionMap() {
            return partMap;
        }

        /** {@inheritDoc} */
        @Override public ConcurrentMap<Integer, Set<IntegerKey>> getLoadingMap() {
            return loadingMap;
        }

        /** {@inheritDoc} */
        @Override public IgniteLocalSyncListener getIgniteLocalSyncListener() {
            return lsnr;
        }

        /**
         * Ensure that the static partition map has a set of keys associated with the given partition,
         * creating one if it doesn't already exist.
         *
         * @param part the partition
         * @return the set for the given partition
         */
        @Override public Set<IntegerKey> ensureKeySet(final int part) {
            return ensureKeySet(part, partMap);
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
        Set<IntegerKey> ensureKeySet(final int part, final ConcurrentMap<Integer, Set<IntegerKey>> map) {
            Set<IntegerKey> keys = map.get(part);

            if (keys == null) {
                map.putIfAbsent(part, new CopyOnWriteArraySet<IntegerKey>());

                keys = map.get(part);
            }

            return keys;
        }


        /** {@inheritDoc} */
        @Override public void cancel(final ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(final ServiceContext ctx) throws Exception {
            this.lsnr.register();
        }

        /** {@inheritDoc} */
        @Override public void execute(final ServiceContext ctx) throws Exception {
            // No-op.
        }
    }

    /**
     * Runnable that starts {@value #SERVER_COUNT} servers. This runnable starts
     * servers every {@value #START_DELAY} milliseconds. The staggered start is intended
     * to allow partitions to move every time a new server is started.
     */
    private class ServerStarter implements Runnable {
        /** */
        static final int SERVER_COUNT = 10;

        /** */
        static final int START_DELAY = 2000;

        /** */
        private volatile boolean done;

        /** */
        private final CountDownLatch started = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                for (int i = 1; i <= SERVER_COUNT; i++) {
                    startGrid(i);

                    Thread.sleep(START_DELAY);

                    awaitPartitionMapExchange();

                    started.countDown();
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
         * Blocks the executing thread until at least one server has started.
         *
         * @throws InterruptedException If interrupted.
         */
        void waitForServerStart() throws InterruptedException {
            started.await(getTestTimeout(), TimeUnit.MILLISECONDS);
        }

        /** @return true if {@value #SERVER_COUNT} servers have started. */
        public boolean isDone() {
            return done;
        }
    }
}
