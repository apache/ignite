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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Test getEntry and getEntries methods.
 */
public abstract class CacheGetEntryAbstractTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String UPDATED_ENTRY_ERR = "Impossible to get version for entry updated in transaction";

    /** */
    private static final String ENTRY_AFTER_GET_ERR = "Impossible to get entry version after get()";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @return Transaction concurrency.
     */
    abstract protected TransactionConcurrency concurrency();

    /**
     *
     * @return Transaction isolation.
     */
    abstract protected TransactionIsolation isolation();

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(null);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNear() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(ATOMIC);
        cfg.setName("near");
        cfg.setNearConfiguration(new NearCacheConfiguration());

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearTransactional() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("nearT");
        cfg.setNearConfiguration(new NearCacheConfiguration());

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitioned() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(ATOMIC);
        cfg.setName("partitioned");

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedTransactional() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("partitionedT");

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocal() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(LOCAL);
        cfg.setAtomicityMode(ATOMIC);
        cfg.setName("local");

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalTransactional() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(LOCAL);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("localT");

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicated() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(REPLICATED);
        cfg.setAtomicityMode(ATOMIC);
        cfg.setName("replicated");

        test(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTransactional() throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(REPLICATED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setName("replicatedT");

        test(cfg);
    }

    /**
     * @param cfg Cache configuration.
     * @throws Exception If failed.
     */
    private void test(CacheConfiguration cfg) throws Exception {
        test(cfg, true);

        test(cfg, false);
    }

    /**
     * @param cfg Cache configuration.
     * @param oneEntry If {@code true} then single entry is tested.
     * @throws Exception If failed.
     */
    private void test(CacheConfiguration cfg, final boolean oneEntry) throws Exception {
        final IgniteCache<Integer, TestValue> cache = grid(0).createCache(cfg);

        try {
            init(cache);

            test(cache, null, null, null, oneEntry);

            if (cfg.getAtomicityMode() == TRANSACTIONAL) {
                TransactionConcurrency txConcurrency = concurrency();
                TransactionIsolation txIsolation = isolation();

                try (Transaction tx = grid(0).transactions().txStart(txConcurrency, txIsolation)) {
                    initTx(cache);

                    test(cache, txConcurrency, txIsolation, tx, oneEntry);

                    tx.commit();
                }

                testConcurrentTx(cache, OPTIMISTIC, REPEATABLE_READ, oneEntry);
                testConcurrentTx(cache, OPTIMISTIC, READ_COMMITTED, oneEntry);

                testConcurrentTx(cache, PESSIMISTIC, REPEATABLE_READ, oneEntry);
                testConcurrentTx(cache, PESSIMISTIC, READ_COMMITTED, oneEntry);

                testConcurrentOptimisticTxGet(cache, REPEATABLE_READ);
                testConcurrentOptimisticTxGet(cache, READ_COMMITTED);
                testConcurrentOptimisticTxGet(cache, SERIALIZABLE);
            }
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @param cache Cache.
     * @param txIsolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void testConcurrentOptimisticTxGet(final IgniteCache<Integer, TestValue> cache,
        final TransactionIsolation txIsolation) throws Exception {
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                final int key = 42;

                IgniteTransactions txs = grid(0).transactions();

                cache.put(key, new TestValue(key));

                long stopTime = System.currentTimeMillis() + 3000;

                while (System.currentTimeMillis() < stopTime) {
                    try (Transaction tx = txs.txStart(OPTIMISTIC, txIsolation)) {
                        cache.get(key);

                        tx.commit();
                    }
                    catch (TransactionOptimisticException ignored) {
                        assertTrue("Should not throw optimistic exception in only read TX. Tx isolation: "
                            + txIsolation, false);
                    }
                }
            }
        }, 10, "tx-thread");
    }

    /**
     * @param cache Cache.
     * @param txConcurrency Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @param oneEntry If {@code true} then single entry is tested.
     * @throws Exception If failed.
     */
    private void testConcurrentTx(final IgniteCache<Integer, TestValue> cache,
        final TransactionConcurrency txConcurrency,
        final TransactionIsolation txIsolation,
        final boolean oneEntry) throws Exception {
        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteTransactions txs = grid(0).transactions();

                long stopTime = System.currentTimeMillis() + 3000;

                while (System.currentTimeMillis() < stopTime) {
                    Set<Integer> keys = new LinkedHashSet<>();

                    for (int i = 0; i < 100; i++)
                        keys.add(i);

                    try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
                        if (oneEntry) {
                            for (int i = 0; i < 100; i++)
                                cache.getEntry(i);
                        }
                        else
                            cache.getEntries(keys);

                        for (int i = 0; i < 100; i++)
                            cache.put(i, new TestValue(i));

                        tx.commit();
                    }
                }

                return null;
            }
        }, 10, "tx-thread");
    }

    /**
     * @param base Start value.
     * @return Keys.
     */
    private Set<Integer> getKeys(int base) {
        int start = 0;
        int finish = 100;

        Set<Integer> keys = new HashSet<>(finish - start);

        for (int i = base + start; i < base + finish; ++i)
            keys.add(i);

        return keys;
    }

    /**
     * @return Keys.
     */
    private Set<Integer> createdBeforeTxKeys() {
        return getKeys(0);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> createdBeforeTxWithBinaryKeys() {
        return getKeys(1_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> createdBeforeTxKeys2() {
        return getKeys(2_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> createdBeforeTxWithBinaryKeys2() {
        return getKeys(3_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> createdBeforeTxKeys3() {
        return getKeys(4_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> createdBeforeTxWithBinaryKeys3() {
        return getKeys(5_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> removedBeforeTxKeys() {
        return getKeys(6_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> removedBeforeTxWithBinaryKeys() {
        return getKeys(7_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> createdAtTxKeys() {
        return getKeys(8_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> createdAtTxWithBinaryKeys() {
        return getKeys(9_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> removedAtTxKeys() {
        return getKeys(10_000);
    }

    /**
     * @return Keys.
     */
    private Set<Integer> removedAtTxWithBinaryKeys() {
        return getKeys(11_000);
    }

    /**
     * @param cache Cacge.
     */
    private void init(IgniteCache<Integer, TestValue> cache) {
        Set<Integer> keys = new HashSet<>();

        keys.addAll(createdBeforeTxKeys());
        keys.addAll(createdBeforeTxWithBinaryKeys());
        keys.addAll(createdBeforeTxKeys2());
        keys.addAll(createdBeforeTxWithBinaryKeys2());
        keys.addAll(createdBeforeTxKeys3());
        keys.addAll(createdBeforeTxWithBinaryKeys3());
        keys.addAll(removedBeforeTxKeys());
        keys.addAll(removedBeforeTxWithBinaryKeys());
        keys.addAll(removedAtTxKeys());
        keys.addAll(removedAtTxWithBinaryKeys());

        for (int i : keys)
            cache.put(i, new TestValue(i));

        for (int i : removedBeforeTxKeys())
            cache.remove(i);

        for (int i : removedBeforeTxWithBinaryKeys())
            cache.remove(i);
    }

    /**
     * @param cache Cache.
     */
    private void initTx(IgniteCache<Integer, TestValue> cache) {
        for (int i : createdAtTxKeys())
            cache.put(i, new TestValue(i));

        for (int i : createdAtTxWithBinaryKeys())
            cache.put(i, new TestValue(i));

        for (int i : removedAtTxKeys())
            cache.remove(i);

        for (int i : removedAtTxWithBinaryKeys())
            cache.remove(i);
    }

    /**
     * @param e Entry.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void compareVersionWithPrimaryNode(CacheEntry<Integer, ?> e, IgniteCache<Integer, TestValue> cache)
        throws Exception {
        CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

        if (cfg.getCacheMode() != LOCAL) {
            Ignite prim = primaryNode(e.getKey(), cache.getName());

            GridCacheAdapter<Object, Object> cacheAdapter = ((IgniteKernal)prim).internalCache(cache.getName());

            if (cfg.getNearConfiguration() != null)
                cacheAdapter = ((GridNearCacheAdapter)cacheAdapter).dht();

            IgniteCacheObjectProcessor cacheObjects = cacheAdapter.context().cacheObjects();

            CacheObjectContext cacheObjCtx = cacheAdapter.context().cacheObjectContext();

            GridCacheMapEntry mapEntry = cacheAdapter.map().getEntry(cacheObjects.toCacheKeyObject(
                cacheObjCtx, null, e.getKey(), true));

            assertNotNull("No entry for key: " + e.getKey(), mapEntry);
            assertEquals(mapEntry.version(), e.version());
        }
    }

    /**
     * @param cache Cache.
     * @param i Key.
     * @param oneEntry If {@code true} then single entry is tested.
     * @param getVerErr Not null error if entry version access should fail.
     * @param expKeys Expected keys with values.
     * @throws Exception If failed.
     */
    private void checkData(IgniteCache<Integer, TestValue> cache,
        int i,
        boolean oneEntry,
        @Nullable String getVerErr,
        Set<Integer> expKeys) throws Exception {
        if (oneEntry) {
            final CacheEntry<Integer, TestValue> e = cache.getEntry(i);

            if (getVerErr == null)
                compareVersionWithPrimaryNode(e, cache);
            else {
                Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        e.version();

                        return null;
                    }
                }, IgniteException.class, null);

                assertTrue("Unexpected error message: " + err.getMessage(), err.getMessage().startsWith(getVerErr));
            }

            assertEquals(e.getValue().val, i);
        }
        else {
            Set<Integer> set = new HashSet<>();

            int expCnt = 0;

            for (int j = 0; j < 10; j++) {
                Integer key = i + j;

                set.add(key);

                if (expKeys.contains(key))
                    expCnt++;
            }

            Collection<CacheEntry<Integer, TestValue>> entries = cache.getEntries(set);

            assertEquals(expCnt, entries.size());

            for (final CacheEntry<Integer, TestValue> e : entries) {
                if (getVerErr == null)
                    compareVersionWithPrimaryNode(e, cache);
                else {
                    Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            e.version();

                            return null;
                        }
                    }, IgniteException.class, null);

                    assertTrue("Unexpected error message: " + err.getMessage(), err.getMessage().startsWith(getVerErr));
                }

                assertEquals((Integer)e.getValue().val, e.getKey());

                assertTrue(set.contains(e.getValue().val));
            }
        }
    }

    /**
     * @param cache Cache.
     * @param i Key.
     * @param oneEntry If {@code true} then single entry is tested.
     * @param getVerErr Not null error if entry version access should fail.
     * @param expKeys Expected keys with values.
     * @throws Exception If failed.
     */
    private void checkBinaryData(IgniteCache<Integer, TestValue> cache,
        int i,
        boolean oneEntry,
        @Nullable String getVerErr,
        Set<Integer> expKeys) throws Exception {
        IgniteCache<Integer, BinaryObject> cacheB = cache.withKeepBinary();

        if (oneEntry) {
            final CacheEntry<Integer, BinaryObject> e = cacheB.getEntry(i);

            if (getVerErr == null)
                compareVersionWithPrimaryNode(e, cache);
            else {
                Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        e.version();

                        return null;
                    }
                }, IgniteException.class, null);

                assertTrue("Unexpected error message: " + err.getMessage(), err.getMessage().startsWith(getVerErr));
            }

            assertEquals(((TestValue)e.getValue().deserialize()).val, i);
        }
        else {
            Set<Integer> set = new HashSet<>();

            int expCnt = 0;

            for (int j = 0; j < 10; j++) {
                Integer key = i + j;

                set.add(key);

                if (expKeys.contains(key))
                    expCnt++;
            }

            Collection<CacheEntry<Integer, BinaryObject>> entries = cacheB.getEntries(set);

            assertEquals(expCnt, entries.size());

            for (final CacheEntry<Integer, BinaryObject> e : entries) {
                if (getVerErr == null)
                    compareVersionWithPrimaryNode(e, cache);
                else {
                    Throwable err = GridTestUtils.assertThrows(log, new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            e.version();

                            return null;
                        }
                    }, IgniteException.class, null);

                    assertTrue("Unexpected error message: " + err.getMessage(), err.getMessage().startsWith(getVerErr));
                }

                TestValue tv = e.getValue().deserialize();

                assertEquals((Integer)tv.val, e.getKey());

                assertTrue(set.contains((tv).val));
            }
        }
    }

    /**
     * @param cache Cache.
     * @param i Key.
     * @param oneEntry If {@code true} then single entry is tested.
     */
    private void checkRemoved(IgniteCache<Integer, TestValue> cache, int i, boolean oneEntry) {
        if (oneEntry) {
            CacheEntry<Integer, TestValue> e = cache.getEntry(i);

            assertNull(e);
        }
        else {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(i + j);

            Collection<CacheEntry<Integer, TestValue>> es = cache.getEntries(set);

            assertTrue(es.isEmpty());
        }
    }

    /**
     * @param cache Cache.
     * @param i Key.
     * @param oneEntry If {@code true} then single entry is tested.
     */
    private void checkBinaryRemoved(IgniteCache<Integer, TestValue> cache, int i, boolean oneEntry) {
        IgniteCache<Integer, BinaryObject> cacheB = cache.withKeepBinary();

        if (oneEntry) {
            CacheEntry<Integer, BinaryObject> e = cacheB.getEntry(i);

            assertNull(e);
        }
        else {
            Set<Integer> set = new HashSet<>();

            for (int j = 0; j < 10; j++)
                set.add(i + j);

            Collection<CacheEntry<Integer, BinaryObject>> es = cacheB.getEntries(set);

            assertTrue(es.isEmpty());
        }
    }

    /**
     * @param cache Cache.
     * @param txConcurrency Transaction concurrency.
     * @param txIsolation Transaction isolation.
     * @param tx Transaction.
     * @param oneEntry If {@code true} then single entry is tested.
     * @throws Exception If failed.
     */
    private void test(IgniteCache<Integer, TestValue> cache,
        TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation,
        Transaction tx,
        boolean oneEntry) throws Exception {
        if (tx == null) {
            Set<Integer> keys = createdBeforeTxKeys();

            for (int i : keys)
                checkData(cache, i, oneEntry, null, keys);

            keys = createdBeforeTxWithBinaryKeys();

            for (int i : keys)
                checkBinaryData(cache, i, oneEntry, null, keys);

            for (int i : removedBeforeTxKeys())
                checkRemoved(cache, i, oneEntry);

            for (int i : removedBeforeTxWithBinaryKeys())
                checkBinaryRemoved(cache, i, oneEntry);
        }
        else {
            Set<Integer> keys = createdBeforeTxKeys2();

            for (int i : keys) {
                checkData(cache, i, oneEntry, null, keys);
                checkData(cache, i, oneEntry, null, keys);
            }

            keys = createdBeforeTxWithBinaryKeys2();

            for (int i : keys) {
                checkBinaryData(cache, i, oneEntry, null, keys);
                checkBinaryData(cache, i, oneEntry, null, keys);
            }

            String verGetErr = null;

            if (txConcurrency == OPTIMISTIC && txIsolation == REPEATABLE_READ)
                verGetErr = ENTRY_AFTER_GET_ERR;

            keys = createdBeforeTxKeys3();

            for (int i : keys) {
                if (oneEntry)
                    cache.get(i);
                else {
                    Set<Integer> set = new HashSet<>();

                    for (int j = 0; j < 10; j++)
                        set.add(i + j);

                    cache.getAll(set);
                }

                checkData(cache, i, oneEntry, verGetErr, keys);
            }

            keys = createdBeforeTxWithBinaryKeys3();

            for (int i : keys) {
                if (oneEntry)
                    cache.get(i);
                else {
                    Set<Integer> set = new HashSet<>();

                    for (int j = 0; j < 10; j++)
                        set.add(i + j);

                    cache.getAll(set);
                }

                checkBinaryData(cache, i, oneEntry, verGetErr, keys);
            }

            keys = createdAtTxKeys();

            for (int i : keys)
                checkData(cache, i, oneEntry, UPDATED_ENTRY_ERR, keys);

            keys = createdAtTxWithBinaryKeys();

            for (int i : keys)
                checkBinaryData(cache, i, oneEntry, UPDATED_ENTRY_ERR, keys);

            for (int i : removedBeforeTxKeys())
                checkRemoved(cache, i, oneEntry);

            for (int i : removedBeforeTxWithBinaryKeys())
                checkBinaryRemoved(cache, i, oneEntry);

            for (int i : removedAtTxKeys())
                checkRemoved(cache, i, oneEntry);

            for (int i : removedAtTxWithBinaryKeys())
                checkBinaryRemoved(cache, i, oneEntry);
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public TestValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}
