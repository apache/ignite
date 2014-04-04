/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.spi.indexing.h2.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests that exception from innerSet is propagated to user.
 */
public abstract class GridCacheTxExceptionAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** Test index SPI. */
    private static TestIndexingSpi idxSpi = new TestIndexingSpi();

    /** Key count. */
    public static final int KEY_CNT = 5;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        idxSpi.setDefaultIndexPrimitiveKey(true);

        cfg.setIndexingSpi(idxSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cacheCfg = super.cacheConfiguration(gridName);

        cacheCfg.setQueryIndexEnabled(true);

        return cacheCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutNear() throws Exception {
        checkPut(keyForNode(cache(0), grid(0).localNode(), 0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPrimary() throws Exception {
        checkPut(keyForNode(cache(0), grid(0).localNode(), 1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutBackup() throws Exception {
        checkPut(keyForNode(cache(0), grid(0).localNode(), 2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxNear() throws Exception {
        checkPutx(keyForNode(cache(0), grid(0).localNode(), 0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxPrimary() throws Exception {
        checkPutx(keyForNode(cache(0), grid(0).localNode(), 1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxBackup() throws Exception {
        checkPutx(keyForNode(cache(0), grid(0).localNode(), 2));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkPut(final String key) throws Exception {
        idxSpi.forceFail(true);

        info("Going to put key: " + key);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return cache(0).put(key, 1);
            }
        }, GridCacheTxHeuristicException.class, null);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkPutx(final String key) throws Exception {
        idxSpi.forceFail(true);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return cache(0).putx(key, 1);
            }
        }, GridCacheTxHeuristicException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testRemove() throws Exception {
        idxSpi.forceFail(true);

        for (int i = 0; i < KEY_CNT; i++) {
            final int key = i;

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return cache(0).remove(String.valueOf(key));
                }
            }, GridCacheTxHeuristicException.class, null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testRemovex() throws Exception {
        idxSpi.forceFail(true);

        for (int i = 0; i < KEY_CNT; i++) {
            final int key = i;

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return cache(0).removex(String.valueOf(key));
                }
            }, GridCacheTxHeuristicException.class, null);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        idxSpi.forceFail(false);

        super.afterTest();
    }

    /**
     * Generates key of a given type for given node.
     *
     * @param cache Cache.
     * @param node Node.
     * @param type Key type.
     * @return Key.
     */
    private String keyForNode(GridCache<String,Integer> cache, GridNode node, int type) {
        for (int i = 0; i < 1000; i++) {
            String key = String.valueOf(i);

            switch (type) {
                // Near.
                case 0: {
                    if (!cache.affinity().isPrimaryOrBackup(node, key))
                        return key;

                    break;
                }

                // Primary.
                case 1: {
                    if (cache.affinity().isPrimary(node, key))
                        return key;

                    break;
                }

                // Backup.
                case 2: {
                    if (cache.affinity().isBackup(node, key))
                        return key;

                    break;
                }
            }
        }

        throw new IllegalStateException("Failed to find key.");
    }

    /**
     * Indexing SPI that can fail on demand.
     */
    private static class TestIndexingSpi extends GridH2IndexingSpi {
        /** Fail flag. */
        private volatile boolean fail;

        /**
         * @param fail Fail flag.
         */
        public void forceFail(boolean fail) {
            this.fail = fail;
        }

        /** {@inheritDoc} */
        @Override public <K, V> void store(@Nullable String spaceName, GridIndexingTypeDescriptor type,
            GridIndexingEntity<K> key, GridIndexingEntity<V> val, byte[] ver, long expirationTime)
            throws GridSpiException {
            if (fail)
                throw new GridSpiException("Test exception.");
        }

        /** {@inheritDoc} */
        @Override public <K> boolean remove(@Nullable String spaceName, GridIndexingEntity<K> k)
            throws GridSpiException {
            if (fail)
                throw new GridSpiException("Test exception.");

            return true;
        }

        /** {@inheritDoc} */
        @Override public void registerSpace(String spaceName) throws GridSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws GridSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws GridSpiException {
            // No-op.
        }
    }
}
