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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheExpireAndUpdateConsistencyTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 5;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(4);

        Ignite client = startClientGrid(4);

        assertTrue(client.configuration().isClientMode());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomic1() throws Exception {
        updateAndEventConsistencyTest(cacheConfiguration(ATOMIC, 0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomic2() throws Exception {
        updateAndEventConsistencyTest(cacheConfiguration(ATOMIC, 1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomic3() throws Exception {
        updateAndEventConsistencyTest(cacheConfiguration(ATOMIC, 2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTx1() throws Exception {
        updateAndEventConsistencyTest(cacheConfiguration(TRANSACTIONAL, 0));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTx2() throws Exception {
        updateAndEventConsistencyTest(cacheConfiguration(TRANSACTIONAL, 1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTx3() throws Exception {
        updateAndEventConsistencyTest(cacheConfiguration(TRANSACTIONAL, 2));
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void updateAndEventConsistencyTest(CacheConfiguration<TestKey, TestValue> ccfg) throws Exception {
        ignite(0).createCache(ccfg);

        try {
            List<ConcurrentMap<TestKey, List<T2<TestValue, TestValue>>>> nodesEvts = new ArrayList<>();

            for (int i = 0; i < NODES; i++) {
                Ignite ignite = ignite(i);

                IgniteCache<TestKey, TestValue> cache = ignite.cache(ccfg.getName());

                ContinuousQuery<TestKey, TestValue> qry = new ContinuousQuery<>();

                final ConcurrentMap<TestKey, List<T2<TestValue, TestValue>>> allEvts = new ConcurrentHashMap<>();

                qry.setLocalListener(new CacheEntryUpdatedListener<TestKey, TestValue>() {
                    @Override public void onUpdated(Iterable<CacheEntryEvent<? extends TestKey, ? extends TestValue>> evts) {
                        for (CacheEntryEvent<? extends TestKey, ? extends TestValue> e : evts) {
                            List<T2<TestValue, TestValue>> keyEvts = allEvts.get(e.getKey());

                            if (keyEvts == null) {
                                List<T2<TestValue, TestValue>> old =
                                    allEvts.putIfAbsent(e.getKey(), keyEvts = new ArrayList<>());

                                assertNull(old);
                            }

                            synchronized (keyEvts) {
                                keyEvts.add(new T2<TestValue, TestValue>(e.getValue(), e.getOldValue()));
                            }
                        }
                    }
                });

                cache.query(qry);

                nodesEvts.add(allEvts);
            }

            final AtomicInteger keyVal = new AtomicInteger();

            for (int i = 0; i < NODES; i++) {
                Ignite ignite = ignite(i);

                log.info("Test with node: " + ignite.name());

                updateAndEventConsistencyTest(ignite, ccfg.getName(), keyVal, nodesEvts, false);

                if (ccfg.getAtomicityMode() == TRANSACTIONAL)
                    updateAndEventConsistencyTest(ignite, ccfg.getName(), keyVal, nodesEvts, true);
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param node Node.
     * @param cacheName Cache name.
     * @param keyVal Key counter.
     * @param nodesEvts Events map.
     * @param useTx If {@code true} executes update with explicit transaction.
     * @throws Exception If failed.
     */
    private void updateAndEventConsistencyTest(final Ignite node,
        String cacheName,
        final AtomicInteger keyVal,
        List<ConcurrentMap<TestKey, List<T2<TestValue, TestValue>>>> nodesEvts,
        final boolean useTx) throws Exception {
        final ConcurrentMap<TestKey, List<T2<TestValue, TestValue>>> updates = new ConcurrentHashMap<>();

        final int THREADS = 5;
        final int KEYS_PER_THREAD = 100;

        final IgniteCache<TestKey, TestValue> cache = node.cache(cacheName);

        final IgniteCache<TestKey, TestValue> expPlcCache =
            cache.withExpiryPolicy(new CreatedExpiryPolicy(new Duration(SECONDS, 2)));

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                List<TestKey> keys = new ArrayList<>();

                for (int i = 0; i < KEYS_PER_THREAD; i++)
                    keys.add(new TestKey(keyVal.incrementAndGet()));

                for (TestKey key : keys) {
                    expPlcCache.put(key, new TestValue(0));

                    List<T2<TestValue, TestValue>> keyUpdates = new ArrayList<>();

                    keyUpdates.add(new T2<>(new TestValue(0), (TestValue)null));

                    updates.put(key, keyUpdates);
                }

                long stopTime = U.currentTimeMillis() + 10_000;

                int val = 0;

                Set<TestKey> expired = new HashSet<>();

                IgniteTransactions txs = node.transactions();

                while (U.currentTimeMillis() < stopTime) {
                    val++;

                    TestValue newVal = new TestValue(val);

                    for (TestKey key : keys) {
                        Transaction tx = useTx ? txs.txStart(PESSIMISTIC, REPEATABLE_READ) : null;

                        TestValue oldVal = cache.getAndPut(key, newVal);

                        if (tx != null)
                            tx.commit();

                        List<T2<TestValue, TestValue>> keyUpdates = updates.get(key);

                        keyUpdates.add(new T2<>(newVal, oldVal));

                        if (oldVal == null)
                            expired.add(key);
                    }

                    if (expired.size() == keys.size())
                        break;
                }

                assertEquals(keys.size(), expired.size());
            }
        }, THREADS, "update-thread");

        for (ConcurrentMap<TestKey, List<T2<TestValue, TestValue>>> evts : nodesEvts)
            checkEvents(updates, evts);

        nodesEvts.clear();
    }

    /**
     * @param updates Cache update.
     * @param evts Received events.
     * @throws Exception If failed.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void checkEvents(ConcurrentMap<TestKey, List<T2<TestValue, TestValue>>> updates,
        final ConcurrentMap<TestKey, List<T2<TestValue, TestValue>>> evts) throws Exception {
        for (final TestKey key : updates.keySet()) {
            final List<T2<TestValue, TestValue>> keyUpdates = updates.get(key);

            assert (!F.isEmpty(keyUpdates));

            GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    List<T2<TestValue, TestValue>> keyEvts = evts.get(key);

                    if (keyEvts == null)
                        return false;

                    synchronized (keyEvts) {
                        return keyEvts.size() == keyUpdates.size();
                    }
                }
            }, 5000);

            List<T2<TestValue, TestValue>> keyEvts = evts.get(key);

            assertNotNull(keyEvts);

            for (int i = 0; i < keyUpdates.size(); i++) {
                T2<TestValue, TestValue> update = keyUpdates.get(i);
                T2<TestValue, TestValue> evt = keyEvts.get(i);

                assertEquals(update.get1(), evt.get1());
                assertEquals(update.get2(), evt.get2());
            }
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<TestKey, TestValue> cacheConfiguration(CacheAtomicityMode atomicityMode, int backups) {
        CacheConfiguration<TestKey, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    static class TestKey implements Serializable {
        /** */
        private int key;

        /**
         * @param key Key.
         */
        public TestKey(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey testKey = (TestKey)o;

            return key == testKey.key;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestKey.class, this);
        }
    }

    /**
     *
     */
    static class TestValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public TestValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue testVal = (TestValue)o;

            return val == testVal.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }
}
