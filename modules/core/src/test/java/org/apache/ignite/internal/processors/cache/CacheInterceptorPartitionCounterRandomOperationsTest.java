/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheInterceptorEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.jetbrains.annotations.NotNull;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheInterceptorPartitionCounterRandomOperationsTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    private static final int KEYS = 50;

    /** */
    private static final int VALS = 10;

    /** */
    public static final int ITERATION_CNT = 100;

    /** */
    private boolean client;

    /** */
    private static ConcurrentMap<UUID, BlockingQueue<Cache.Entry<TestKey, TestValue>>>
        afterPutEvts = new ConcurrentHashMap<>();

    /** */
    private static ConcurrentMap<UUID, BlockingQueue<Cache.Entry<TestKey, TestValue>>>
        afterRmvEvts = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES - 1);

        client = true;

        startGrid(NODES - 1);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        afterPutEvts.clear();
        afterRmvEvts.clear();

        for (int i = 0; i < NODES; i++) {
            afterRmvEvts.put(grid(i).cluster().localNode().id(),
                new BlockingArrayQueue<Cache.Entry<TestKey, TestValue>>());
            afterPutEvts.put(grid(i).cluster().localNode().id(),
                new BlockingArrayQueue<Cache.Entry<TestKey, TestValue>>());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            false);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            true);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            false);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicatedWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            true);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoBackups() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            false);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            false);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            true);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            false);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL,
            false);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicatedWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL,
            true);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackups() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            false);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackupsWithStore() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            true);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackupsExplicit() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            false);

        doTestPartitionCounterOperation(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    protected void doTestPartitionCounterOperation(CacheConfiguration<Object, Object> ccfg)
        throws Exception {
        ignite(0).createCache(ccfg);

        try {
            long seed = System.currentTimeMillis();

            Random rnd = new Random(seed);

            log.info("Random seed: " + seed);

            ConcurrentMap<Object, Object> expData = new ConcurrentHashMap<>();

            Map<Integer, Long> partCntr = new ConcurrentHashMap<>();

            for (int i = 0; i < ITERATION_CNT; i++) {
                if (i % 20 == 0)
                    log.info("Iteration: " + i);

                for (int idx = 0; idx < NODES; idx++)
                    randomUpdate(rnd, expData, partCntr, grid(idx).cache(ccfg.getName()));
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param rnd Random generator.
     * @param expData Expected cache data.
     * @param partCntr Partition counter.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void randomUpdate(
        Random rnd,
        ConcurrentMap<Object, Object> expData,
        Map<Integer, Long> partCntr,
        IgniteCache<Object, Object> cache)
        throws Exception {
        Object key = new TestKey(rnd.nextInt(KEYS));
        Object newVal = value(rnd);
        Object oldVal = expData.get(key);

        int op = rnd.nextInt(11);

        Ignite ignite = cache.unwrap(Ignite.class);

        Transaction tx = null;

        if (cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL && rnd.nextBoolean())
            tx = ignite.transactions().txStart(txRandomConcurrency(rnd), txRandomIsolation(rnd));

        try {
            //log.info("Random operation [key=" + key + ", op=" + op + ']');

            switch (op) {
                case 0: {
                    cache.put(key, newVal);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(cache, partCntr, affinity(cache), key, newVal, oldVal, false);

                    expData.put(key, newVal);

                    break;
                }

                case 1: {
                    cache.getAndPut(key, newVal);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(cache, partCntr, affinity(cache), key, newVal, oldVal, false);

                    expData.put(key, newVal);

                    break;
                }

                case 2: {
                    cache.remove(key);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(cache, partCntr, affinity(cache), key, null, oldVal, true);

                    expData.remove(key);

                    break;
                }

                case 3: {
                    cache.getAndRemove(key);

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(cache, partCntr, affinity(cache), key, null, oldVal, true);

                    expData.remove(key);

                    break;
                }

                case 4: {
                    cache.invoke(key, new EntrySetValueProcessor(newVal, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(cache, partCntr, affinity(cache), key, newVal, oldVal, false);

                    expData.put(key, newVal);

                    break;
                }

                case 5: {
                    cache.invoke(key, new EntrySetValueProcessor(null, rnd.nextBoolean()));

                    if (tx != null)
                        tx.commit();

                    updatePartitionCounter(cache, key, partCntr);

                    waitAndCheckEvent(cache, partCntr, affinity(cache), key, null, oldVal, true);

                    expData.remove(key);

                    break;
                }

                case 6: {
                    cache.putIfAbsent(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal == null) {
                        updatePartitionCounter(cache, key, partCntr);

                        waitAndCheckEvent(cache, partCntr, affinity(cache), key, newVal, null, false);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(getInterceptorQueues(cache, key, false));

                    break;
                }

                case 7: {
                    cache.getAndPutIfAbsent(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal == null) {
                        updatePartitionCounter(cache, key, partCntr);

                        waitAndCheckEvent(cache, partCntr, affinity(cache), key, newVal, null, false);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(getInterceptorQueues(cache, key, false));

                    break;
                }

                case 8: {
                    cache.replace(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal != null) {
                        updatePartitionCounter(cache, key, partCntr);

                        waitAndCheckEvent(cache, partCntr, affinity(cache), key, newVal, oldVal, false);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(getInterceptorQueues(cache, key, false));

                    break;
                }

                case 9: {
                    cache.getAndReplace(key, newVal);

                    if (tx != null)
                        tx.commit();

                    if (oldVal != null) {
                        updatePartitionCounter(cache, key, partCntr);

                        waitAndCheckEvent(cache, partCntr, affinity(cache), key, newVal, oldVal, false);

                        expData.put(key, newVal);
                    }
                    else
                        checkNoEvent(getInterceptorQueues(cache, key, false));

                    break;
                }

                case 10: {
                    if (oldVal != null) {
                        Object replaceVal = value(rnd);

                        boolean success = replaceVal.equals(oldVal);

                        if (success) {
                            cache.replace(key, replaceVal, newVal);

                            if (tx != null)
                                tx.commit();

                            updatePartitionCounter(cache, key, partCntr);

                            waitAndCheckEvent(cache, partCntr, affinity(cache), key, newVal, oldVal, false);

                            expData.put(key, newVal);
                        }
                        else {
                            cache.replace(key, replaceVal, newVal);

                            if (tx != null)
                                tx.commit();

                            checkNoEvent(getInterceptorQueues(cache, key, false));
                        }
                    }
                    else {
                        cache.replace(key, value(rnd), newVal);

                        if (tx != null)
                            tx.commit();

                        checkNoEvent(getInterceptorQueues(cache, key, false));
                    }

                    break;
                }

                default:
                    fail("Op:" + op);
            }
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param rmv Remove operation.
     * @return Queues.
     */
    @NotNull private List<BlockingQueue<Cache.Entry<TestKey, TestValue>>> getInterceptorQueues(
        IgniteCache<Object, Object> cache,
        Object key,
        boolean rmv
    ) {
        Collection<ClusterNode> nodes =
            cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ?
                affinity(cache).mapKeyToPrimaryAndBackups(key) :
                Collections.singletonList(affinity(cache).mapKeyToNode(key));

        assert !nodes.isEmpty();

        List<BlockingQueue<Cache.Entry<TestKey, TestValue>>> queues = new ArrayList<>();

        for (ClusterNode node : nodes)
            queues.add(rmv ? afterRmvEvts.get(node.id()) : afterPutEvts.get(node.id()));

        return queues;
    }

    /**
     * @param rnd {@link Random}.
     * @return {@link TransactionIsolation}.
     */
    private TransactionIsolation txRandomIsolation(Random rnd) {
        int val = rnd.nextInt(3);

        if (val == 0)
            return READ_COMMITTED;
        else if (val == 1)
            return REPEATABLE_READ;
        else
            return SERIALIZABLE;
    }

    /**
     * @param rnd {@link Random}.
     * @return {@link TransactionConcurrency}.
     */
    private TransactionConcurrency txRandomConcurrency(Random rnd) {
        return rnd.nextBoolean() ? TransactionConcurrency.OPTIMISTIC : TransactionConcurrency.PESSIMISTIC;
    }

    /**
     * @param cache Cache.
     * @param key Key
     * @param cntrs Partition counters.
     */
    private void updatePartitionCounter(IgniteCache<Object, Object> cache, Object key, Map<Integer, Long> cntrs) {
        Affinity<Object> aff = cache.unwrap(Ignite.class).affinity(cache.getName());

        int part = aff.partition(key);

        Long partCntr = cntrs.get(part);

        if (partCntr == null)
            partCntr = 0L;

        cntrs.put(part, ++partCntr);
    }

    /**
     * @param rnd Random generator.
     * @return Cache value.
     */
    private static Object value(Random rnd) {
        return new TestValue(rnd.nextInt(VALS));
    }

    /**
     * @param cache Ignite cache.
     * @param partCntrs Partition counters.
     * @param aff Affinity function.
     * @param key Key.
     * @param val Value.
     * @param oldVal Old value.
     * @param rmv Remove operation.
     * @throws Exception If failed.
     */
    private void waitAndCheckEvent(IgniteCache<Object, Object> cache,
        Map<Integer, Long> partCntrs,
        Affinity<Object> aff,
        Object key,
        Object val,
        Object oldVal,
        boolean rmv)
        throws Exception {
        Collection<BlockingQueue<Cache.Entry<TestKey, TestValue>>> entries = getInterceptorQueues(cache, key,
            rmv);

        assert !entries.isEmpty();

        if (val == null && oldVal == null) {
            checkNoEvent(entries);

            return;
        }

        for (BlockingQueue<Cache.Entry<TestKey, TestValue>> evtsQueue : entries) {
            Cache.Entry<TestKey, TestValue> entry = evtsQueue.poll(5, SECONDS);

            assertNotNull("Failed to wait for event [key=" + key + ", val=" + val + ", oldVal=" + oldVal + ']', entry);
            assertEquals(key, entry.getKey());
            assertEquals(rmv ? oldVal : val, entry.getValue());

            long cntr = partCntrs.get(aff.partition(key));
            CacheInterceptorEntry interceptorEntry = entry.unwrap(CacheInterceptorEntry.class);

            assertNotNull(cntr);
            assertNotNull(interceptorEntry);

            assertEquals(cntr, interceptorEntry.getPartitionUpdateCounter());

            assertNull(evtsQueue.peek());
        }
    }

    /**
     * @param evtsQueues Event queue.
     * @throws Exception If failed.
     */
    private void checkNoEvent(Collection<BlockingQueue<Cache.Entry<TestKey, TestValue>>> evtsQueues)
        throws Exception {
        for (BlockingQueue<Cache.Entry<TestKey, TestValue>> evtsQueue : evtsQueues) {
            Cache.Entry<TestKey, TestValue> evt = evtsQueue.poll(50, MILLISECONDS);

            assertTrue(evt == null || evt.getValue() == null);
        }
    }

    /**
     *
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param store If {@code true} configures dummy cache store.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode,
        boolean store) {
        CacheConfiguration<TestKey, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (store) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
        }

        ccfg.setInterceptor(new TestInterceptor());

        return (CacheConfiguration)ccfg;
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public CacheStore<Object, Object> create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    // No-op.
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    // No-op.
                }
            };
        }
    }

    /**
     *
     */
    public static class TestKey implements Serializable, Comparable {
        /** */
        private final Integer key;

        /**
         * @param key Key.
         */
        public TestKey(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey that = (TestKey)o;

            return key.equals(that.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestKey.class, this);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(Object o) {
            return key - ((TestKey)o).key;
        }
    }

    /**
     *
     */
    private static class TestInterceptor extends CacheInterceptorAdapter<TestKey, TestValue> {
        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> e) {
            e.getKey();
            e.getValue();

            UUID id = e.unwrap(Ignite.class).cluster().localNode().id();

            BlockingQueue<Cache.Entry<TestKey, TestValue>> ents = afterPutEvts.get(id);

            if (ents == null) {
                ents = new BlockingArrayQueue<>();

                BlockingQueue<Cache.Entry<TestKey, TestValue>> oldVal = afterPutEvts.putIfAbsent(id, ents);

                ents = oldVal == null ? ents : oldVal;
            }

            ents.add(e);
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<TestKey, TestValue> e) {
            e.getKey();
            e.getValue();

            UUID id = e.unwrap(Ignite.class).cluster().localNode().id();

            BlockingQueue<Cache.Entry<TestKey, TestValue>> ents = afterRmvEvts.get(id);

            if (ents == null) {
                ents = new BlockingArrayQueue<>();

                BlockingQueue<Cache.Entry<TestKey, TestValue>> oldVal = afterRmvEvts.putIfAbsent(id, ents);

                ents = oldVal == null ? ents : oldVal;
            }

            ents.add(e);
        }
    }

    /**
     *
     */
    public static class TestValue implements Serializable {
        /** */
        @GridToStringInclude
        protected final Integer val1;

        /** */
        @GridToStringInclude
        protected final String val2;

        /**
         * @param val Value.
         */
        public TestValue(Integer val) {
            this.val1 = val;
            this.val2 = String.valueOf(val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue that = (TestValue) o;

            return val1.equals(that.val1) && val2.equals(that.val2);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = val1.hashCode();

            res = 31 * res + val2.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    /**
     *
     */
    protected static class EntrySetValueProcessor implements EntryProcessor<Object, Object, Object> {
        /** */
        private Object val;

        /** */
        private boolean retOld;

        /**
         * @param val Value to set.
         * @param retOld Return old value flag.
         */
        public EntrySetValueProcessor(Object val, boolean retOld) {
            this.val = val;
            this.retOld = retOld;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> e, Object... args) {
            Object old = retOld ? e.getValue() : null;

            if (val != null)
                e.setValue(val);
            else
                e.remove();

            return old;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntrySetValueProcessor.class, this);
        }
    }
}
