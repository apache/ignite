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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests that transaction is invalidated in case of {@link IgniteTxHeuristicCheckedException}.
 */
public abstract class IgniteTxExceptionAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int PRIMARY = 0;

    /** */
    private static final int BACKUP = 1;

    /** */
    private static final int NOT_PRIMARY_AND_BACKUP = 2;

    /** */
    private static volatile Integer lastKey;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setIndexingSpi(new TestIndexingSpi());

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setCacheStoreFactory(null);
        ccfg.setReadThrough(false);
        ccfg.setWriteThrough(false);
        ccfg.setLoadPreviousValue(true);

        ccfg.setIndexedTypes(Integer.class, Integer.class);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        lastKey = 0;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        TestIndexingSpi.forceFail(false);

        Transaction tx = jcache().unwrap(Ignite.class).transactions().tx();

        if (tx != null) {
            tx.close();

            fail("Cache transaction remained after test completion: " + tx);
        }

        for (int key = 0; key <= lastKey; key++)
            grid(0).cache(null).remove(key);

        assertEquals(0, jcache(0).size(CachePeekMode.ALL));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        lastKey = 0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutNear() throws Exception {
        checkPut(true, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));

        checkPut(false, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPrimary() throws Exception {
        checkPut(true, keyForNode(grid(0).localNode(), PRIMARY));

        checkPut(false, keyForNode(grid(0).localNode(), PRIMARY));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutBackup() throws Exception {
        checkPut(true, keyForNode(grid(0).localNode(), BACKUP));

        checkPut(false, keyForNode(grid(0).localNode(), BACKUP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        checkPutAll(true, keyForNode(grid(0).localNode(), PRIMARY),
            keyForNode(grid(0).localNode(), PRIMARY),
            keyForNode(grid(0).localNode(), PRIMARY));

        checkPutAll(false, keyForNode(grid(0).localNode(), PRIMARY),
            keyForNode(grid(0).localNode(), PRIMARY),
            keyForNode(grid(0).localNode(), PRIMARY));

        if (gridCount() > 1) {
            checkPutAll(true, keyForNode(grid(1).localNode(), PRIMARY),
                keyForNode(grid(1).localNode(), PRIMARY),
                keyForNode(grid(1).localNode(), PRIMARY));

            checkPutAll(false, keyForNode(grid(1).localNode(), PRIMARY),
                keyForNode(grid(1).localNode(), PRIMARY),
                keyForNode(grid(1).localNode(), PRIMARY));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveNear() throws Exception {
        checkRemove(false, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));

        checkRemove(true, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemovePrimary() throws Exception {
        checkRemove(false, keyForNode(grid(0).localNode(), PRIMARY));

        checkRemove(true, keyForNode(grid(0).localNode(), PRIMARY));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveBackup() throws Exception {
        checkRemove(false, keyForNode(grid(0).localNode(), BACKUP));

        checkRemove(true, keyForNode(grid(0).localNode(), BACKUP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformNear() throws Exception {
        checkTransform(false, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));

        checkTransform(true, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPrimary() throws Exception {
        checkTransform(false, keyForNode(grid(0).localNode(), PRIMARY));

        checkTransform(true, keyForNode(grid(0).localNode(), PRIMARY));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformBackup() throws Exception {
        checkTransform(false, keyForNode(grid(0).localNode(), BACKUP));

        checkTransform(true, keyForNode(grid(0).localNode(), BACKUP));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutNearTx() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                checkPutTx(true, concurrency, isolation, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));

                checkPutTx(false, concurrency, isolation, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPrimaryTx() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                checkPutTx(true, concurrency, isolation, keyForNode(grid(0).localNode(), PRIMARY));

                checkPutTx(false, concurrency, isolation, keyForNode(grid(0).localNode(), PRIMARY));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutBackupTx() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                checkPutTx(true, concurrency, isolation, keyForNode(grid(0).localNode(), BACKUP));

                checkPutTx(false, concurrency, isolation, keyForNode(grid(0).localNode(), BACKUP));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutMultipleKeysTx() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                checkPutTx(true, concurrency, isolation,
                    keyForNode(grid(0).localNode(), PRIMARY),
                    keyForNode(grid(0).localNode(), PRIMARY),
                    keyForNode(grid(0).localNode(), PRIMARY));

                checkPutTx(false, concurrency, isolation,
                    keyForNode(grid(0).localNode(), PRIMARY),
                    keyForNode(grid(0).localNode(), PRIMARY),
                    keyForNode(grid(0).localNode(), PRIMARY));

                if (gridCount() > 1) {
                    checkPutTx(true, concurrency, isolation,
                        keyForNode(grid(1).localNode(), PRIMARY),
                        keyForNode(grid(1).localNode(), PRIMARY),
                        keyForNode(grid(1).localNode(), PRIMARY));

                    checkPutTx(false, concurrency, isolation,
                        keyForNode(grid(1).localNode(), PRIMARY),
                        keyForNode(grid(1).localNode(), PRIMARY),
                        keyForNode(grid(1).localNode(), PRIMARY));
                }
            }
        }
    }

    /**
     * @param putBefore If {@code true} then puts some value before executing failing operation.
     * @param keys Keys.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void checkPutTx(boolean putBefore, TransactionConcurrency concurrency,
        TransactionIsolation isolation, final Integer... keys) throws Exception {
        assertTrue(keys.length > 0);

        info("Test transaction [concurrency=" + concurrency + ", isolation=" + isolation + ']');

        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        if (putBefore) {
            TestIndexingSpi.forceFail(false);

            info("Start transaction.");

            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                for (Integer key : keys) {
                    info("Put " + key);

                    cache.put(key, 1);
                }

                info("Commit.");

                tx.commit();
            }
        }

        // Execute get from all nodes to create readers for near cache.
        for (int i = 0; i < gridCount(); i++) {
            for (Integer key : keys)
                grid(i).cache(null).get(key);
        }

        TestIndexingSpi.forceFail(true);

        try {
            info("Start transaction.");

            try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                for (Integer key : keys) {
                    info("Put " + key);

                    cache.put(key, 2);
                }

                info("Commit.");

                tx.commit();
            }

            fail("Transaction should fail.");
        }
        catch (TransactionHeuristicException e) {
            log.info("Expected exception: " + e);
        }

        for (Integer key : keys)
            checkUnlocked(key);

        for (int i = 0; i < gridCount(); i++)
            assertEquals(0, ((IgniteKernal)ignite(0)).internalCache(null).context().tm().idMapSize());
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkUnlocked(final Integer key) throws Exception {
        TestIndexingSpi.forceFail(false);

        awaitPartitionMapExchange();

        info("Check key: " + key);

        for (int i = 0; i < gridCount(); i++) {
            final int idx = i;

            GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    IgniteKernal grid = (IgniteKernal)grid(idx);

                    GridCacheAdapter cache = grid.internalCache(null);

                    GridCacheEntryEx entry = cache.peekEx(key);

                    log.info("Entry: " + entry);

                    if (entry != null) {
                        try {
                            boolean locked = entry.lockedByAny();

                            if (locked) {
                                info("Unexpected entry for grid [i=" + idx + ", entry=" + entry + ']');

                                return false;
                            }
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            // Obsolete entry cannot be locked.
                        }
                    }

                    if (cache.isNear()) {
                        entry = ((GridNearCacheAdapter)cache).dht().peekEx(key);

                        log.info("Dht entry: " + entry);

                        if (entry != null) {
                            try {
                                boolean locked = entry.lockedByAny();

                                if (locked) {
                                    info("Unexpected entry for grid [i=" + idx + ", entry=" + entry + ']');

                                    return false;
                                }
                            }
                            catch (GridCacheEntryRemovedException ignore) {
                                // Obsolete entry cannot be locked.
                            }
                        }
                    }

                    return true;
                }
            }, getTestTimeout());
        }
    }

    /**
     * @param putBefore If {@code true} then puts some value before executing failing operation.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkPut(boolean putBefore, final Integer key) throws Exception {
        if (putBefore) {
            TestIndexingSpi.forceFail(false);

            info("Put key: " + key);

            grid(0).cache(null).put(key, 1);
        }

        // Execute get from all nodes to create readers for near cache.
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(null).get(key);

        TestIndexingSpi.forceFail(true);

        info("Going to put: " + key);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).cache(null).put(key, 2);

                return null;
            }
        }, TransactionHeuristicException.class, null);

        checkUnlocked(key);
    }

    /**
     * @param putBefore If {@code true} then puts some value before executing failing operation.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkTransform(boolean putBefore, final Integer key) throws Exception {
        if (putBefore) {
            TestIndexingSpi.forceFail(false);

            info("Put key: " + key);

            grid(0).cache(null).put(key, 1);
        }

        // Execute get from all nodes to create readers for near cache.
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(null).get(key);

        TestIndexingSpi.forceFail(true);

        info("Going to transform: " + key);

        Throwable e = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).<Integer, Integer>cache(null).invoke(key, new EntryProcessor<Integer, Integer, Void>() {
                    @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
                        e.setValue(2);

                        return null;
                    }
                });

                return null;
            }
        }, CacheException.class, null);

        assertTrue("Unexpected cause: " + e, e.getCause() instanceof TransactionHeuristicException);

        checkUnlocked(key);
    }

    /**
     * @param putBefore If {@code true} then puts some value before executing failing operation.
     * @param keys Keys.
     * @throws Exception If failed.
     */
    private void checkPutAll(boolean putBefore, Integer... keys) throws Exception {
        assert keys.length > 1;

        if (putBefore) {
            TestIndexingSpi.forceFail(false);

            Map<Integer, Integer> m = new HashMap<>();

            for (Integer key : keys)
                m.put(key, 1);

            info("Put data: " + m);

            grid(0).cache(null).putAll(m);
        }

        // Execute get from all nodes to create readers for near cache.
        for (int i = 0; i < gridCount(); i++) {
            for (Integer key : keys)
                grid(i).cache(null).get(key);
        }

        TestIndexingSpi.forceFail(true);

        final Map<Integer, Integer> m = new HashMap<>();

        for (Integer key : keys)
            m.put(key, 2);

        info("Going to putAll: " + m);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).cache(null).putAll(m);

                return null;
            }
        }, TransactionHeuristicException.class, null);

        for (Integer key : m.keySet())
            checkUnlocked(key);
    }

    /**
     * @param putBefore If {@code true} then puts some value before executing failing operation.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkRemove(boolean putBefore, final Integer key) throws Exception {
        if (putBefore) {
            TestIndexingSpi.forceFail(false);

            info("Put key: " + key);

            grid(0).cache(null).put(key, 1);
        }

        // Execute get from all nodes to create readers for near cache.
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(null).get(key);

        TestIndexingSpi.forceFail(true);

        info("Going to remove: " + key);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).cache(null).remove(key);

                return null;
            }
        }, TransactionHeuristicException.class, null);

        checkUnlocked(key);
    }

    /**
     * Generates key of a given type for given node.
     *
     * @param node Node.
     * @param type Key type.
     * @return Key.
     */
    private Integer keyForNode(ClusterNode node, int type) {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() == LOCAL)
            return ++lastKey;

        if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() == REPLICATED && type == NOT_PRIMARY_AND_BACKUP)
            return ++lastKey;

        for (int key = lastKey + 1; key < (lastKey + 10_000); key++) {
            switch (type) {
                case NOT_PRIMARY_AND_BACKUP: {
                    if (!affinity(cache).isPrimaryOrBackup(node, key)) {
                        lastKey = key;

                        return key;
                    }

                    break;
                }

                case PRIMARY: {
                    if (affinity(cache).isPrimary(node, key)) {
                        lastKey = key;

                        return key;
                    }

                    break;
                }

                case BACKUP: {
                    if (affinity(cache).isBackup(node, key)) {
                        lastKey = key;

                        return key;
                    }

                    break;
                }

                default:
                    fail();
            }
        }

        throw new IllegalStateException("Failed to find key.");
    }

    /**
     * Indexing SPI that can fail on demand.
     */
    private static class TestIndexingSpi extends IgniteSpiAdapter implements IndexingSpi {
        /** Fail flag. */
        private static volatile boolean fail;

        /**
         * @param failFlag Fail flag.
         */
        public static void forceFail(boolean failFlag) {
            fail = failFlag;
        }

        /** {@inheritDoc} */
        @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String spaceName, Collection<Object> params,
            @Nullable IndexingQueryFilter filters) throws IgniteSpiException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void store(@Nullable String spaceName, Object key, Object val, long expirationTime)
            throws IgniteSpiException {
            if (fail) {
                fail = false;

                throw new IgniteSpiException("Test exception.");
            }
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String spaceName, Object k)
            throws IgniteSpiException {
            if (fail) {
                fail = false;

                throw new IgniteSpiException("Test exception.");
            }
        }

        /** {@inheritDoc} */
        @Override public void onSwap(@Nullable String spaceName, Object key) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onUnswap(@Nullable String spaceName, Object key, Object val) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }
    }
}
