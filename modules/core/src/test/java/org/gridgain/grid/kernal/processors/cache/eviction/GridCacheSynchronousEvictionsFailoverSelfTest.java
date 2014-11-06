/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 *
 */
public class GridCacheSynchronousEvictionsFailoverSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setSwapEnabled(false);
        ccfg.setEvictSynchronized(true);
        ccfg.setEvictSynchronizedKeyBufferSize(10);

        ccfg.setBackups(2);

        ccfg.setAffinity(new GridCacheConsistentHashAffinityFunction(false, 500));

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousEvictions() throws Exception {
        GridCache<String, Integer> cache = cache(0);

        final AtomicBoolean stop = new AtomicBoolean();

        GridFuture<?> fut = null;

        try {
            Map<String, Integer> data = new HashMap<>();

            addKeysForNode(cache.affinity(), grid(0).localNode(), data);
            addKeysForNode(cache.affinity(), grid(1).localNode(), data);
            addKeysForNode(cache.affinity(), grid(2).localNode(), data);

            fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Random rnd = new Random();

                    while (!stop.get()) {
                        int idx = rnd.nextBoolean() ? 1 : 2;

                        log.info("Stopping grid: " + idx);

                        stopGrid(idx);

                        U.sleep(100);

                        log.info("Starting grid: " + idx);

                        startGrid(idx);
                    }

                    return null;
                }
            });

            for (int i = 0 ; i < 100; i++) {
                log.info("Iteration: " + i);

                try {
                    cache.putAll(data);
                }
                catch (GridException ignore) {
                    continue;
                }

                for (String key : data.keySet())
                    cache.evict(key);
            }
        }
        finally {
            stop.set(true);

            if (fut != null)
                fut.get();
        }
    }

    /**
     * @param aff Cache affinity.
     * @param node Primary node for keys.
     * @param data Map where keys/values should be put to.
     */
    private void addKeysForNode(GridCacheAffinity<String> aff, GridNode node, Map<String, Integer> data) {
        int cntr = 0;

        for (int i = 0; i < 100_000; i++) {
            String key = String.valueOf(i);

            if (aff.isPrimary(node, key)) {
                data.put(key, i);

                cntr++;

                if (cntr == 500)
                    break;
            }
        }

        assertEquals(500, cntr);
    }
}
