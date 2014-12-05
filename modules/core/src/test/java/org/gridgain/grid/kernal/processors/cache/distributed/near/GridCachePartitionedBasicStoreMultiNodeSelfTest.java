/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCachePartitionedBasicStoreMultiNodeSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of grids to start. */
    private static final int GRID_CNT = 3;

    /** Cache store. */
    private static final GridCacheTestStore store = new GridCacheTestStore();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            cache(i).removeAll();

        store.reset();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setBackups(1);

        cc.setStore(store);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Distribution mode.
     */
    protected GridCacheDistributionMode mode() {
        return NEAR_PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutFromPrimary() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (cache.affinity().isPrimary(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.put(key, "val"));

        assertEquals(1, store.getLoadCount());
        assertEquals(1, store.getPutCount());

        Collection<GridCacheTx> txs = store.transactions();

        assertEquals(1, txs.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutFromBackup() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (cache.affinity().isBackup(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.put(key, "val"));

        assertEquals(1, store.getLoadCount());
        assertEquals(1, store.getPutCount());

        Collection<GridCacheTx> txs = store.transactions();

        assertEquals(1, txs.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutFromNear() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (!cache.affinity().isPrimaryOrBackup(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.put(key, "val"));

        assertEquals(1, store.getLoadCount());
        assertEquals(1, store.getPutCount());

        Collection<GridCacheTx> txs = store.transactions();

        assertEquals(1, txs.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentFromPrimary() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (cache.affinity().isPrimary(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.putIfAbsent(key, "val"));

        assertEquals(1, store.getLoadCount());
        assertEquals(1, store.getPutCount());

        Collection<GridCacheTx> txs = store.transactions();

        assertEquals(1, txs.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentFromBackup() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (cache.affinity().isBackup(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.putIfAbsent(key, "val"));

        assertEquals(1, store.getLoadCount());
        assertEquals(1, store.getPutCount());

        Collection<GridCacheTx> txs = store.transactions();

        assertEquals(1, txs.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentFromNear() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        int key = 0;

        while (true) {
            boolean found = false;

            for (ClusterNode n : grid(0).nodes()) {
                if (!cache.affinity().isPrimaryOrBackup(n, key)) {
                    found = true;

                    break;
                }
            }

            if (found)
                break;
        }

        assertNull(cache.putIfAbsent(key, "val"));

        assertEquals(1, store.getLoadCount());
        assertEquals(1, store.getPutCount());

        Collection<GridCacheTx> txs = store.transactions();

        assertEquals(1, txs.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        Map<Integer, String> map = new HashMap<>(10);

        for (int i = 0; i < 10; i++)
            map.put(i, "val");

        cache.putAll(map);

        assertEquals(1, store.getPutAllCount());

        Collection<GridCacheTx> txs = store.transactions();

        assertEquals(1, txs.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleOperations() throws Exception {
        GridCache<Integer, String> cache = cache(0);

        try (GridCacheTx tx = cache.txStart(OPTIMISTIC, REPEATABLE_READ)) {
            cache.put(1, "val");
            cache.put(2, "val");
            cache.put(3, "val");

            cache.get(4);

            cache.putAll(F.asMap(5, "val", 6, "val"));

            tx.commit();
        }

        assertEquals(4, store.getLoadCount());
        assertEquals(0, store.getPutCount());
        assertEquals(1, store.getPutAllCount());

        Collection<GridCacheTx> txs = store.transactions();

        assertEquals(1, txs.size());
    }
}
