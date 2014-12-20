/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.indexing.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests that transaction is invalidated in case of {@link GridCacheTxHeuristicException}.
 */
public abstract class GridCacheTxExceptionAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** Index SPI throwing exception. */
    private static TestIndexingSpi idxSpi = new TestIndexingSpi();

    /** */
    private static final int PRIMARY = 0;

    /** */
    private static final int BACKUP = 1;

    /** */
    private static final int NOT_PRIMARY_AND_BACKUP = 2;

    /** */
    private static Integer lastKey;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setIndexingSpi(idxSpi);

        cfg.getTransactionsConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setQueryIndexEnabled(true);
        ccfg.setStore(null);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        lastKey = 0;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        idxSpi.forceFail(false);
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
        for (GridCacheTxConcurrency concurrency : GridCacheTxConcurrency.values()) {
            for (GridCacheTxIsolation isolation : GridCacheTxIsolation.values()) {
                checkPutTx(true, concurrency, isolation, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));

                checkPutTx(false, concurrency, isolation, keyForNode(grid(0).localNode(), NOT_PRIMARY_AND_BACKUP));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPrimaryTx() throws Exception {
        for (GridCacheTxConcurrency concurrency : GridCacheTxConcurrency.values()) {
            for (GridCacheTxIsolation isolation : GridCacheTxIsolation.values()) {
                checkPutTx(true, concurrency, isolation, keyForNode(grid(0).localNode(), PRIMARY));

                checkPutTx(false, concurrency, isolation, keyForNode(grid(0).localNode(), PRIMARY));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutBackupTx() throws Exception {
        for (GridCacheTxConcurrency concurrency : GridCacheTxConcurrency.values()) {
            for (GridCacheTxIsolation isolation : GridCacheTxIsolation.values()) {
                checkPutTx(true, concurrency, isolation, keyForNode(grid(0).localNode(), BACKUP));

                checkPutTx(false, concurrency, isolation, keyForNode(grid(0).localNode(), BACKUP));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutMultipleKeysTx() throws Exception {
        for (GridCacheTxConcurrency concurrency : GridCacheTxConcurrency.values()) {
            for (GridCacheTxIsolation isolation : GridCacheTxIsolation.values()) {
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
    private void checkPutTx(boolean putBefore, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, final Integer... keys) throws Exception {
        assertTrue(keys.length > 0);

        info("Test transaction [concurrency=" + concurrency + ", isolation=" + isolation + ']');

        GridCache<Integer, Integer> cache = grid(0).cache(null);

        if (putBefore) {
            idxSpi.forceFail(false);

            info("Start transaction.");

            try (GridCacheTx tx = cache.txStart(concurrency, isolation)) {
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

        idxSpi.forceFail(true);

        try {
            info("Start transaction.");

            try (GridCacheTx tx = cache.txStart(concurrency, isolation)) {
                for (Integer key : keys) {
                    info("Put " + key);

                    cache.put(key, 2);
                }

                info("Commit.");

                tx.commit();
            }

            fail("Transaction should fail.");
        }
        catch (GridCacheTxHeuristicException e) {
            log.info("Expected exception: " + e);
        }

        for (Integer key : keys)
            checkEmpty(key);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkEmpty(final Integer key) throws Exception {
        idxSpi.forceFail(false);

        info("Check key: " + key);

        for (int i = 0; i < gridCount(); i++) {
            GridKernal grid = (GridKernal) grid(i);

            GridCacheAdapter cache = grid.internalCache(null);

            GridCacheMapEntry entry = cache.map().getEntry(key);

            log.info("Entry: " + entry);

            if (entry != null) {
                assertFalse("Unexpected entry for grid [i=" + i + ", entry=" + entry + ']', entry.lockedByAny());
                assertFalse("Unexpected entry for grid [i=" + i + ", entry=" + entry + ']', entry.hasValue());
            }

            if (cache.isNear()) {
                entry = ((GridNearCacheAdapter)cache).dht().map().getEntry(key);

                log.info("Dht entry: " + entry);

                if (entry != null) {
                    assertFalse("Unexpected entry for grid [i=" + i + ", entry=" + entry + ']', entry.lockedByAny());
                    assertFalse("Unexpected entry for grid [i=" + i + ", entry=" + entry + ']', entry.hasValue());
                }
            }
        }

        for (int i = 0; i < gridCount(); i++)
            assertEquals("Unexpected value for grid " + i, null, grid(i).cache(null).get(key));
    }

    /**
     * @param putBefore If {@code true} then puts some value before executing failing operation.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkPut(boolean putBefore, final Integer key) throws Exception {
        if (putBefore) {
            idxSpi.forceFail(false);

            info("Put key: " + key);

            grid(0).cache(null).put(key, 1);
        }

        // Execute get from all nodes to create readers for near cache.
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(null).get(key);

        idxSpi.forceFail(true);

        info("Going to put: " + key);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).cache(null).put(key, 2);

                return null;
            }
        }, GridCacheTxHeuristicException.class, null);

        checkEmpty(key);
    }

    /**
     * @param putBefore If {@code true} then puts some value before executing failing operation.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkTransform(boolean putBefore, final Integer key) throws Exception {
        if (putBefore) {
            idxSpi.forceFail(false);

            info("Put key: " + key);

            grid(0).cache(null).put(key, 1);
        }

        // Execute get from all nodes to create readers for near cache.
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(null).get(key);

        idxSpi.forceFail(true);

        info("Going to transform: " + key);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).cache(null).transform(key, new IgniteClosure<Object, Object>() {
                    @Override public Object apply(Object o) {
                        return 2;
                    }
                });

                return null;
            }
        }, GridCacheTxHeuristicException.class, null);

        checkEmpty(key);
    }

    /**
     * @param putBefore If {@code true} then puts some value before executing failing operation.
     * @param keys Keys.
     * @throws Exception If failed.
     */
    private void checkPutAll(boolean putBefore, Integer ... keys) throws Exception {
        assert keys.length > 1;

        if (putBefore) {
            idxSpi.forceFail(false);

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

        idxSpi.forceFail(true);

        final Map<Integer, Integer> m = new HashMap<>();

        for (Integer key : keys)
            m.put(key, 2);

        info("Going to putAll: " + m);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).cache(null).putAll(m);

                return null;
            }
        }, GridCacheTxHeuristicException.class, null);

        for (Integer key : m.keySet())
            checkEmpty(key);
    }

    /**
     * @param putBefore If {@code true} then puts some value before executing failing operation.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkRemove(boolean putBefore, final Integer key) throws Exception {
        if (putBefore) {
            idxSpi.forceFail(false);

            info("Put key: " + key);

            grid(0).cache(null).put(key, 1);
        }

        // Execute get from all nodes to create readers for near cache.
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(null).get(key);

        idxSpi.forceFail(true);

        info("Going to remove: " + key);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                grid(0).cache(null).remove(key);

                return null;
            }
        }, GridCacheTxHeuristicException.class, null);

        checkEmpty(key);
    }

    /**
     * Generates key of a given type for given node.
     *
     * @param node Node.
     * @param type Key type.
     * @return Key.
     */
    private Integer keyForNode(ClusterNode node, int type) {
        GridCache<Integer, Integer> cache = grid(0).cache(null);

        if (cache.configuration().getCacheMode() == LOCAL)
            return ++lastKey;

        if (cache.configuration().getCacheMode() == REPLICATED && type == NOT_PRIMARY_AND_BACKUP)
            return ++lastKey;

        for (int key = lastKey + 1; key < (lastKey + 10_000); key++) {
            switch (type) {
                case NOT_PRIMARY_AND_BACKUP: {
                    if (!cache.affinity().isPrimaryOrBackup(node, key)) {
                        lastKey = key;

                        return key;
                    }

                    break;
                }

                case PRIMARY: {
                    if (cache.affinity().isPrimary(node, key)) {
                        lastKey = key;

                        return key;
                    }

                    break;
                }

                case BACKUP: {
                    if (cache.affinity().isBackup(node, key)) {
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
    private static class TestIndexingSpi extends IgniteSpiAdapter implements GridIndexingSpi {
        /** Fail flag. */
        private volatile boolean fail;

        /**
         * @param fail Fail flag.
         */
        public void forceFail(boolean fail) {
            this.fail = fail;
        }

        /** {@inheritDoc} */
        @Override public Iterator<?> query(@Nullable String spaceName, Collection<Object> params, @Nullable GridIndexingQueryFilter filters) throws IgniteSpiException {
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
