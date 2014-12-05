/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.PESSIMISTIC;
import static org.gridgain.grid.cache.GridCacheTxIsolation.REPEATABLE_READ;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public abstract class GridCacheTxConsistencyRestartAbstractSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_CNT = 4;

    /** Key range. */
    private static final int RANGE = 100_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    public GridCacheConfiguration cacheConfiguration(String gridName) {
        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setCacheMode(cacheMode());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setDistributionMode(partitionDistributionMode());
        ccfg.setPreloadMode(SYNC);

        if (cacheMode() == GridCacheMode.PARTITIONED)
            ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @return Cache mode.
     */
    protected abstract GridCacheMode cacheMode();

    /**
     * @return Partition distribution mode for PARTITIONED cache.
     */
    protected abstract GridCacheDistributionMode partitionDistributionMode();

    /**
     * @throws Exception If failed.
     */
    public void testTxConsistency() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        IgniteDataLoader<Object, Object> ldr = grid(0).dataLoader(null);

        for (int i = 0; i < RANGE; i++) {
            ldr.addData(i, 0);

            if (i > 0 && i % 1000 == 0)
                info("Put keys: " + i);
        }

        ldr.close();

        final AtomicBoolean done = new AtomicBoolean(false);

        Thread restartThread = new Thread() {
            @Override public void run() {
                Random rnd = new Random();

                while (!done.get()) {
                    try {
                        int idx = rnd.nextInt(GRID_CNT);

                        stopGrid(idx);

                        startGrid(idx);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        restartThread.start();

        Random rnd = new Random();

        // Make some iterations with 1-3 keys transactions.
        for (int i = 0; i < 50_000; i++) {
            int idx = i % GRID_CNT;

            if (i > 0 && i % 1000 == 0)
                info("Running iteration: " + i);

            try {
                GridKernal grid = (GridKernal)grid(idx);

                GridCache<Integer, Integer> cache = grid.cache(null);

                List<Integer> keys = new ArrayList<>();

                int keyCnt = rnd.nextInt(3);

                for (int k = 0; k < keyCnt; k++)
                    keys.add(rnd.nextInt(RANGE));

                Collections.sort(keys);

                try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    Map<Integer, Integer> map = cache.getAll(keys);

                    for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
                        assertNotNull("Null value received from cache [key=" + entry.getKey() + "]", entry.getValue());

                        cache.put(entry.getKey(), entry.getValue() + 1);
                    }

                    tx.commit();
                }
            }
            catch (Exception e) {
                info("Failed to update keys: " + e.getMessage());
            }
        }

        done.set(true);

        restartThread.join();

        for (int k = 0; k < RANGE; k++) {
            Integer val = null;

            for (int i = 0; i < GRID_CNT; i++) {
                GridEx grid = grid(i);

                GridCache<Integer, Integer> cache = grid.cache(null);

                if (cache.affinity().isPrimaryOrBackup(grid.localNode(), k)) {
                    if (val == null) {
                        val = cache.peek(k);

                        assertNotNull("Failed to peek value for key: " + k, val);
                    }
                    else
                        assertEquals("Failed to find value in cache [primary=" +
                            cache.affinity().isPrimary(grid.localNode(), k) + ']',
                            val, cache.peek(k));
                }
            }
        }
    }
}
