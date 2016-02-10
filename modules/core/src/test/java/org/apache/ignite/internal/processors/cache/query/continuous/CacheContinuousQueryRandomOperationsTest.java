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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheContinuousQueryRandomOperationsTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    private static final int KEYS = 10;

    /** */
    private static final int VALS = 10;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

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
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapValues() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_VALUES,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTiered() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_TIERED,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNoBackups() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            ATOMIC,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapValues() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_VALUES,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapTiered() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_TIERED,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxNoBackups() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            0,
            TRANSACTIONAL,
            ONHEAP_TIERED,
            false);

        testContinuousQuery(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void testContinuousQuery(CacheConfiguration<Object, Object> ccfg) throws Exception {
        ignite(0).createCache(ccfg);

        try {
            IgniteCache<Object, Object> cache = ignite(NODES - 1).cache(ccfg.getName());

            long seed = System.currentTimeMillis();

            Random rnd = new Random(seed);

            log.info("Random seed: " + seed);

            ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

            final BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue =
                new ArrayBlockingQueue<>(10_000);

            qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
                @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                    for (CacheEntryEvent<?, ?> evt : evts) {
                        // System.out.println("Event: " + evt);

                        evtsQueue.add(evt);
                    }
                }
            });

            QueryCursor<?> cur = cache.query(qry);

            ConcurrentMap<Object, Object> expData = new ConcurrentHashMap<>();

            try {
                for (int i = 0; i < 1000; i++) {
                    if (i % 100 == 0)
                        log.info("Iteration: " + i);

                    randomUpdate(rnd, evtsQueue, expData, cache);
                }
            }
            finally {
                cur.close();
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param rnd Random generator.
     * @param evtsQueue Events queue.
     * @param expData Expected cache data.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void randomUpdate(
        Random rnd,
        BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue,
        ConcurrentMap<Object, Object> expData,
        IgniteCache<Object, Object> cache)
        throws Exception {
        Object key = new QueryTestKey(rnd.nextInt(KEYS));
        Object newVal = value(rnd);
        Object oldVal = expData.get(key);

        int op = rnd.nextInt(11);

        // log.info("Random operation [key=" + key + ", op=" + op + ']');

        switch (op) {
            case 0: {
                cache.put(key, newVal);

                waitEvent(evtsQueue, key, newVal, oldVal);

                expData.put(key, newVal);

                break;
            }

            case 1: {
                cache.getAndPut(key, newVal);

                waitEvent(evtsQueue, key, newVal, oldVal);

                expData.put(key, newVal);

                break;
            }

            case 2: {
                cache.remove(key);

                waitEvent(evtsQueue, key, null, oldVal);

                expData.remove(key);

                break;
            }

            case 3: {
                cache.getAndRemove(key);

                waitEvent(evtsQueue, key, null, oldVal);

                expData.remove(key);

                break;
            }

            case 4: {
                cache.invoke(key, new EntrySetValueProcessor(newVal, rnd.nextBoolean()));

                waitEvent(evtsQueue, key, newVal, oldVal);

                expData.put(key, newVal);

                break;
            }

            case 5: {
                cache.invoke(key, new EntrySetValueProcessor(null, rnd.nextBoolean()));

                waitEvent(evtsQueue, key, null, oldVal);

                expData.remove(key);

                break;
            }

            case 6: {
                cache.putIfAbsent(key, newVal);

                if (oldVal == null) {
                    waitEvent(evtsQueue, key, newVal, null);

                    expData.put(key, newVal);
                }
                else
                    checkNoEvent(evtsQueue);

                break;
            }

            case 7: {
                cache.getAndPutIfAbsent(key, newVal);

                if (oldVal == null) {
                    waitEvent(evtsQueue, key, newVal, null);

                    expData.put(key, newVal);
                }
                else
                    checkNoEvent(evtsQueue);

                break;
            }

            case 8: {
                cache.replace(key, newVal);

                if (oldVal != null) {
                    waitEvent(evtsQueue, key, newVal, oldVal);

                    expData.put(key, newVal);
                }
                else
                    checkNoEvent(evtsQueue);

                break;
            }

            case 9: {
                cache.getAndReplace(key, newVal);

                if (oldVal != null) {
                    waitEvent(evtsQueue, key, newVal, oldVal);

                    expData.put(key, newVal);
                }
                else
                    checkNoEvent(evtsQueue);

                break;
            }

            case 10: {
                if (oldVal != null) {
                    Object replaceVal = value(rnd);

                    boolean success = replaceVal.equals(oldVal);

                    if (success) {
                        cache.replace(key, replaceVal, newVal);

                        waitEvent(evtsQueue, key, newVal, oldVal);

                        expData.put(key, newVal);
                    }
                    else {
                        cache.replace(key, replaceVal, newVal);

                        checkNoEvent(evtsQueue);
                    }
                }
                else {
                    cache.replace(key, value(rnd), newVal);

                    checkNoEvent(evtsQueue);
                }

                break;
            }

            default:
                fail();
        }
    }

    /**
     * @param rnd Random generator.
     * @return Cache value.
     */
    private static Object value(Random rnd) {
        return new QueryTestValue(rnd.nextInt(VALS));
    }

    /**
     * @param evtsQueue Event queue.
     * @param key Key.
     * @param val Value.
     * @param oldVal Old value.
     * @throws Exception If failed.
     */
    private void waitEvent(BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue,
        Object key, Object val, Object oldVal) throws Exception {
        if (val == null && oldVal == null) {
            checkNoEvent(evtsQueue);

            return;
        }

        CacheEntryEvent<?, ?> evt = evtsQueue.poll(5, SECONDS);

        assertNotNull("Failed to wait for event [key=" + key +
            ", val=" + val +
            ", oldVal=" + oldVal + ']', evt);
        assertEquals(key, evt.getKey());
        assertEquals(val, evt.getValue());
        assertEquals(oldVal, evt.getOldValue());
    }

    /**
     * @param evtsQueue Event queue.
     * @throws Exception If failed.
     */
    private void checkNoEvent(BlockingQueue<CacheEntryEvent<?, ?>> evtsQueue) throws Exception {
        CacheEntryEvent<?, ?> evt = evtsQueue.poll(50, MILLISECONDS);

        assertNull(evt);
    }

    /**
     *
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param memoryMode Cache memory mode.
     * @param store If {@code true} configures dummy cache store.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode,
        boolean store) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        if (store) {
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
        }

        return ccfg;
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
    static class QueryTestKey implements Serializable {
        /** */
        private final Integer key;

        /**
         * @param key Key.
         */
        public QueryTestKey(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestKey that = (QueryTestKey)o;

            return key.equals(that.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestKey.class, this);
        }
    }

    /**
     *
     */
    static class QueryTestValue implements Serializable {
        /** */
        private final Integer val1;

        /** */
        private final String val2;

        /**
         * @param val Value.
         */
        public QueryTestValue(Integer val) {
            this.val1 = val;
            this.val2 = String.valueOf(val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestValue that = (QueryTestValue) o;

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
            return S.toString(QueryTestValue.class, this);
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
