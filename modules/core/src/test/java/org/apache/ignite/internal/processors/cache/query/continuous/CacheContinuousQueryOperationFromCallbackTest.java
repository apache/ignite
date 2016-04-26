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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 *
 */
public class CacheContinuousQueryOperationFromCallbackTest extends GridCommonAbstractTest {
    /** */
    public static final int KEYS = 10;

    /** */
    public static final int KEYS_FROM_CALLBACK = 20;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    public static final int ITERATION_CNT = 20;

    /** */
    public static final int SYSTEM_POOL_SIZE = 10;

    /** */
    private boolean client;

    /** */
    private static AtomicInteger filterCbCntr = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setSystemThreadPoolSize(SYSTEM_POOL_SIZE);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        MemoryEventStorageSpi storeSpi = new MemoryEventStorageSpi();
        storeSpi.setExpireCount(100);

        cfg.setEventStorageSpi(storeSpi);

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        filterCbCntr.set(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicTwoBackups() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 2, ATOMIC, FULL_SYNC);

        doTest(ccfg, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxTwoBackupsFilter() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, FULL_SYNC);

        doTest(ccfg, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxTwoBackupsFilterPrimary() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, PRIMARY_SYNC);

        doTest(ccfg, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicatedFilter() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED, 0, TRANSACTIONAL, FULL_SYNC);

        doTest(ccfg, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxTwoBackup() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 2, TRANSACTIONAL, FULL_SYNC);

        doTest(ccfg, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED, 2, TRANSACTIONAL, FULL_SYNC);

        doTest(ccfg, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicatedPrimary() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED, 2, TRANSACTIONAL, PRIMARY_SYNC);

        doTest(ccfg, true);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    protected void doTest(final CacheConfiguration ccfg, boolean fromLsnr) throws Exception {
        ignite(0).createCache(ccfg);

        List<QueryCursor<?>> qries = new ArrayList<>();

        assertEquals(0, filterCbCntr.get());

        try {
            List<Set<T2<QueryTestKey, QueryTestValue>>> rcvdEvts = new ArrayList<>(NODES);
            List<Set<T2<QueryTestKey, QueryTestValue>>> evtsFromCallbacks = new ArrayList<>(NODES);

            final AtomicInteger qryCntr = new AtomicInteger(0);

            final AtomicInteger cbCntr = new AtomicInteger(0);

            final int threadCnt = SYSTEM_POOL_SIZE * 2;

            for (int idx = 0; idx < NODES; idx++) {
                Set<T2<QueryTestKey, QueryTestValue>> evts = Collections.
                    newSetFromMap(new ConcurrentHashMap<T2<QueryTestKey, QueryTestValue>, Boolean>());
                Set<T2<QueryTestKey, QueryTestValue>> evtsFromCb = Collections.
                    newSetFromMap(new ConcurrentHashMap<T2<QueryTestKey, QueryTestValue>, Boolean>());

                IgniteCache<Object, Object> cache = grid(idx).getOrCreateCache(ccfg.getName());

                ContinuousQuery qry = new ContinuousQuery();

                qry.setLocalListener(new TestCacheAsyncEventListener(evts, evtsFromCb,
                    fromLsnr ? cache : null, qryCntr, cbCntr));

                if (!fromLsnr)
                    qry.setRemoteFilterFactory(
                        FactoryBuilder.factoryOf(new CacheTestRemoteFilterAsync(ccfg.getName())));

                rcvdEvts.add(evts);
                evtsFromCallbacks.add(evtsFromCb);

                QueryCursor qryCursor = cache.query(qry);

                qries.add(qryCursor);
            }

            IgniteInternalFuture<Long> f = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < ITERATION_CNT; i++) {
                        IgniteCache<QueryTestKey, QueryTestValue> cache =
                            grid(rnd.nextInt(NODES)).cache(ccfg.getName());

                        QueryTestKey key = new QueryTestKey(rnd.nextInt(KEYS));

                        boolean startTx = cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() ==
                            TRANSACTIONAL && rnd.nextBoolean();

                        Transaction tx = null;

                        if (startTx)
                            tx = cache.unwrap(Ignite.class).transactions().txStart();

                        try {
                            if ((cache.get(key) == null) || rnd.nextBoolean())
                                cache.invoke(key, new IncrementTestEntryProcessor());
                            else {
                                QueryTestValue val;
                                QueryTestValue newVal;

                                do {
                                    val = cache.get(key);

                                    newVal = val == null ?
                                        new QueryTestValue(0) : new QueryTestValue(val.val1 + 1);
                                }
                                while (!cache.replace(key, val, newVal));
                            }
                        }
                        finally {
                            if (tx != null)
                                tx.commit();
                        }
                    }
                }
            }, threadCnt, "put-thread");

            f.get(30, TimeUnit.SECONDS);

            assert GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return qryCntr.get() >= ITERATION_CNT * threadCnt * NODES;
                }
            }, TimeUnit.MINUTES.toMillis(2));

            for (Set<T2<QueryTestKey, QueryTestValue>> set : rcvdEvts)
                checkEvents(set, ITERATION_CNT * threadCnt, grid(0).cache(ccfg.getName()), false);

            if (fromLsnr) {
                final int expCnt = qryCntr.get() * NODES * KEYS_FROM_CALLBACK;

                boolean res = GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        return cbCntr.get() >= expCnt;
                    }
                }, TimeUnit.SECONDS.toMillis(60));

                assertTrue("Failed to wait events [exp=" + expCnt + ", act=" + cbCntr.get() + "]", res);

                assertEquals(expCnt, cbCntr.get());

                for (Set<T2<QueryTestKey, QueryTestValue>> set : evtsFromCallbacks)
                    checkEvents(set, qryCntr.get() * KEYS_FROM_CALLBACK, grid(0).cache(ccfg.getName()), true);
            }
            else {
                final int expInvkCnt = ITERATION_CNT * threadCnt *
                    (ccfg.getCacheMode() != REPLICATED ? (ccfg.getBackups() + 1) : NODES - 1) * NODES;

                GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        return filterCbCntr.get() >= expInvkCnt;
                    }
                }, TimeUnit.SECONDS.toMillis(60));

                assertEquals(expInvkCnt, filterCbCntr.get());

                for (Set<T2<QueryTestKey, QueryTestValue>> set : evtsFromCallbacks)
                    checkEvents(set, expInvkCnt * KEYS_FROM_CALLBACK, grid(0).cache(ccfg.getName()), true);
            }
        }
        finally {
            for (QueryCursor<?> qry : qries)
                qry.close();

            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param expCnt Expected count.
     * @param cache Cache.
     * @param set Received events.
     * @throws Exception If failed.
     */
    private void checkEvents(final Set<T2<QueryTestKey, QueryTestValue>> set, final int expCnt, IgniteCache cache,
        boolean cb) throws Exception {
        assertTrue("Expected size: " + expCnt + ", actual: " + set.size(), GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return set.size() >= expCnt;
            }
        }, 10000L));

        int startKey = cb ? KEYS : 0;
        int endKey = cb ? KEYS + KEYS_FROM_CALLBACK : KEYS;

        for (int i = startKey; i < endKey; i++) {
            QueryTestKey key = new QueryTestKey(i);

            QueryTestValue maxVal = (QueryTestValue)cache.get(key);

            for (int val = 0; val <= maxVal.val1; val++)
                assertTrue(set.remove(new T2<>(key, new QueryTestValue(val))));
        }

        assertTrue(set.isEmpty());
    }

    /**
     *
     */
    private static class IncrementTestEntryProcessor implements
        CacheEntryProcessor<QueryTestKey, QueryTestValue, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<QueryTestKey, QueryTestValue> entry, Object... arguments)
            throws EntryProcessorException {
            if (entry.exists())
                entry.setValue(new QueryTestValue(entry.getValue().val1 + 1));
            else
                entry.setValue(new QueryTestValue(0));

            return null;
        }
    }

    /**
     *
     */
    @IgniteAsyncCallback
    private static class CacheTestRemoteFilterAsync implements
        CacheEntryEventSerializableFilter<QueryTestKey, QueryTestValue> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private String cacheName;

        /**
         * @param cacheName Cache name.
         */
        public CacheTestRemoteFilterAsync(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> e)
            throws CacheEntryListenerException {
            if (e.getKey().compareTo(new QueryTestKey(KEYS)) < 0) {
                IgniteCache<QueryTestKey, QueryTestValue> cache = ignite.cache(cacheName);

                if (ThreadLocalRandom.current().nextBoolean()) {
                    Set<QueryTestKey> keys = new LinkedHashSet<>();

                    for (int key = KEYS; key < KEYS + KEYS_FROM_CALLBACK; key++)
                        keys.add(new QueryTestKey(key));

                    cache.invokeAll(keys, new IncrementTestEntryProcessor());
                }
                else {
                    for (int key = KEYS; key < KEYS + KEYS_FROM_CALLBACK; key++)
                        cache.invoke(new QueryTestKey(key), new IncrementTestEntryProcessor());
                }

                filterCbCntr.incrementAndGet();
            }

            return true;
        }
    }

    /**
     *
     */
    @IgniteAsyncCallback
    private static class TestCacheAsyncEventListener
        implements CacheEntryUpdatedListener<QueryTestKey, QueryTestValue> {
        /** */
        private final Set<T2<QueryTestKey, QueryTestValue>> rcvsEvts;

        /** */
        private final AtomicInteger cntr;

        /** */
        private final AtomicInteger cbCntr;

        /** */
        private final Set<T2<QueryTestKey, QueryTestValue>> evtsFromCb;

        /** */
        private IgniteCache<QueryTestKey, QueryTestValue> cache;

        /**
         * @param rcvsEvts Set for received events.
         * @param evtsFromCb Set for received events.
         * @param cache Ignite cache.
         * @param cntr Received events counter.
         * @param cbCntr Received events counter from callbacks.
         */
        public TestCacheAsyncEventListener(Set<T2<QueryTestKey, QueryTestValue>> rcvsEvts,
            Set<T2<QueryTestKey, QueryTestValue>> evtsFromCb,
            @Nullable IgniteCache cache,
            AtomicInteger cntr,
            AtomicInteger cbCntr) {
            this.rcvsEvts = rcvsEvts;
            this.evtsFromCb = evtsFromCb;
            this.cache = cache;
            this.cntr = cntr;
            this.cbCntr = cbCntr;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue>> evts)
            throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends QueryTestKey, ? extends QueryTestValue> e : evts) {
                if (e.getKey().compareTo(new QueryTestKey(KEYS)) < 0) {
                    rcvsEvts.add(new T2<>(e.getKey(), e.getValue()));

                    cntr.incrementAndGet();

                    if (cache != null) {
                        if (ThreadLocalRandom.current().nextBoolean()) {
                            Set<QueryTestKey> keys = new LinkedHashSet<>();

                            for (int key = KEYS; key < KEYS + KEYS_FROM_CALLBACK; key++)
                                keys.add(new QueryTestKey(key));

                            cache.invokeAll(keys, new IncrementTestEntryProcessor());
                        }
                        else {
                            for (int key = KEYS; key < KEYS + KEYS_FROM_CALLBACK; key++)
                                cache.invoke(new QueryTestKey(key), new IncrementTestEntryProcessor());
                        }
                    }
                }
                else {
                    evtsFromCb.add(new T2<>(e.getKey(), e.getValue()));

                    cbCntr.incrementAndGet();
                }
            }
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param writeMode Write sync mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode,
        CacheWriteSynchronizationMode writeMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setName("test-cache-" + atomicityMode + "-" + cacheMode + "-" + writeMode + "-" + backups);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(writeMode);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    public static class QueryTestKey implements Serializable, Comparable {
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

        /** {@inheritDoc} */
        @Override public int compareTo(Object o) {
            return key - ((QueryTestKey)o).key;
        }
    }

    /**
     *
     */
    public static class QueryTestValue implements Serializable {
        /** */
        @GridToStringInclude
        protected final Integer val1;

        /** */
        @GridToStringInclude
        protected final String val2;

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
}
