/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

import java.util.*;

/**
 * Tests entry wrappers after preloading happened.
 */
public class GridCacheEntrySetIterationPreloadingSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return GridCacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return GridCacheDistributionMode.PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheAtomicityMode atomicityMode() {
        return GridCacheAtomicityMode.ATOMIC;
    }

    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setPreloadMode(GridCachePreloadMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteration()  throws Exception {
        try {
            final GridCache<String, Integer> cache = cache();

            final int entryCnt = 1000;

            for (int i = 0; i < entryCnt; i++)
                cache.put(String.valueOf(i), i);

            Collection<GridCacheEntry<String, Integer>> entries = new ArrayList<>(10_000);

            for (int i = 0; i < 10_000; i++)
                entries.add(cache.randomEntry());

            startGrid(1);
            startGrid(2);
            startGrid(3);

            for (GridCacheEntry<String, Integer> entry : entries)
                entry.partition();

            for (int i = 0; i < entryCnt; i++)
                cache.remove(String.valueOf(i));
        }
        finally {
            stopGrid(3);
            stopGrid(2);
            stopGrid(1);
        }
    }
}
