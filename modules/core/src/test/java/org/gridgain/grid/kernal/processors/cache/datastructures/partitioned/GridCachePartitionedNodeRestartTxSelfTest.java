/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures.partitioned;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test with variable number of nodes.
 */
public class GridCachePartitionedNodeRestartTxSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int INIT_GRID_NUM = 3;

    /** */
    private static final int MAX_GRID_NUM = 20;

    /**
     * Constructs a test.
     */
    public GridCachePartitionedNodeRestartTxSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);
        cacheCfg.setPreloadMode(SYNC);
        cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testSimple() throws Exception {
        String key = UUID.randomUUID().toString();

        try {
            // Prepare nodes and cache data.
            prepareSimple(key);

            // Test simple key/value.
            checkSimple(key);
        }
        finally {
            for (int i = 0; i < MAX_GRID_NUM; i++)
                stopGrid(i);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCustom() throws Exception {
        String key = UUID.randomUUID().toString();

        try {
            // Prepare nodes and cache data.
            prepareCustom(key);

            // Test {@link GridCacheInternalKey}/{@link GridCacheAtomicLongValue}.
            checkCustom(key);
        }
        finally {
            for (int i = 0; i < MAX_GRID_NUM; i++)
                stopGrid(i);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        String key = UUID.randomUUID().toString();

        try {
            // Prepare nodes and cache data.
            prepareAtomic(key);

            // Test AtomicLong
            checkAtomic(key);
        }
        finally {
            for (int i = 0; i < MAX_GRID_NUM; i++)
                stopGrid(i);
        }
    }

    /**
     *  Test simple key/value.
     * @param key Simple key.
     * @throws Exception If failed.
     */
    private void checkSimple(String key) throws Exception {
        for (int i = INIT_GRID_NUM; i < MAX_GRID_NUM; i++) {
            startGrid(i);

            assert PARTITIONED == grid(i).cache(null).configuration().getCacheMode();

            try (GridCacheTx tx = grid(i).cache(null).txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Integer val = (Integer) grid(i).cache(null).get(key);

                assertEquals("Simple check failed for node: " + i, (Integer) i, val);

                grid(i).cache(null).put(key, i + 1);

                tx.commit();
            }

            stopGrid(i);
        }
    }

    /**
     * Test {@link GridCacheInternalKey}/{@link GridCacheAtomicLongValue}.
     * @param name Name.
     * @throws Exception If failed.
     */
    private void checkCustom(String name) throws Exception {
        for (int i = INIT_GRID_NUM; i < 20; i++) {
            startGrid(i);

            assert PARTITIONED == grid(i).cache(null).configuration().getCacheMode();

            try (GridCacheTx tx = grid(i).cache(null).txStart(PESSIMISTIC, REPEATABLE_READ)) {
                GridCacheInternalKey key = new GridCacheInternalKeyImpl(name);

                GridCacheAtomicLongValue atomicVal = ((GridCacheAtomicLongValue) grid(i).cache(null).get(key));

                assertNotNull(atomicVal);

                assertEquals("Custom check failed for node: " + i, (long) i, atomicVal.get());

                atomicVal.set(i + 1);

                grid(i).cache(null).put(key, atomicVal);

                tx.commit();
            }

            stopGrid(i);
        }
    }

    /**
     * Test AtomicLong.
     * @param name Name of atomic.
     * @throws Exception If failed.
     */
    private void checkAtomic(String name) throws Exception {
        for (int i = INIT_GRID_NUM; i < 20; i++) {
            startGrid(i);

            assert PARTITIONED == grid(i).cache(null).configuration().getCacheMode();

            GridCacheAtomicLong atomic = grid(i).cache(null).dataStructures().atomicLong(name, 0, true);

            long val = atomic.get();

            assertEquals("Atomic check failed for node: " + i, (long)i, val);

            atomic.incrementAndGet();

            stopGrid(i);
        }
    }

    /**
     * Prepare test environment.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void prepareSimple(String key) throws Exception {
        // Start nodes.
        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert startGrid(i) != null;

        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert PARTITIONED == grid(i).cache(null).configuration().getCacheMode();

        // Init cache data.

        try (GridCacheTx tx = grid(0).cache(null).txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // Put simple value.
            grid(0).cache(null).put(key, INIT_GRID_NUM);

            tx.commit();
        }
    }

    /**
     * Prepare test environment.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void prepareCustom(String key) throws Exception {
        // Start nodes.
        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert startGrid(i) != null;

        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert PARTITIONED == grid(i).cache(null).configuration().getCacheMode();

        // Init cache data.

        try (GridCacheTx tx = grid(0).cache(null).txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // Put custom data
            grid(0).cache(null).put(new GridCacheInternalKeyImpl(key), new GridCacheAtomicLongValue(INIT_GRID_NUM));

            tx.commit();
        }

        stopGrid(0);
    }

    /**
     * Prepare test environment.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void prepareAtomic(String key) throws Exception {
        // Start nodes.
        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert startGrid(i) != null;

        for (int i = 0; i < INIT_GRID_NUM; i++)
            assert PARTITIONED == grid(i).cache(null).configuration().getCacheMode();

        // Init cache data.
        grid(0).cache(null).dataStructures().atomicLong(key, 0, true).getAndSet(INIT_GRID_NUM);

        assert INIT_GRID_NUM == grid(0).cache(null).dataStructures().atomicLong(key, 0, true).get();

        stopGrid(0);
    }
}
