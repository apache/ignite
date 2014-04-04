/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.spi.swapspace.file.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Tests for byte array values in PARTITIONED-ONLY caches.
 */
public abstract class GridCacheAbstractPartitionedOnlyByteArrayValuesSelfTest extends
    GridCacheAbstractPartitionedByteArrayValuesSelfTest {
    /** Offheap cache name. */
    protected static final String CACHE_ATOMIC = "cache_atomic";

    /** Offheap cache name. */
    protected static final String CACHE_ATOMIC_OFFHEAP = "cache_atomic_offheap";

    /** Atomic caches. */
    private static GridCache<Integer, Object>[] cachesAtomic;

    /** Atomic offheap caches. */
    private static GridCache<Integer, Object>[] cachesAtomicOffheap;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration atomicCacheCfg = cacheConfiguration0();

        atomicCacheCfg.setName(CACHE_ATOMIC);
        atomicCacheCfg.setAtomicityMode(ATOMIC);

        GridCacheConfiguration atomicOffheapCacheCfg = offheapCacheConfiguration0();

        atomicOffheapCacheCfg.setName(CACHE_ATOMIC_OFFHEAP);
        atomicOffheapCacheCfg.setAtomicityMode(ATOMIC);

        c.setCacheConfiguration(cacheConfiguration(), offheapCacheConfiguration(), atomicCacheCfg,
            atomicOffheapCacheCfg);
        c.setSwapSpaceSpi(new GridFileSwapSpaceSpi());
        c.setPeerClassLoadingEnabled(peerClassLoading());

        return c;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        int gridCnt = gridCount();

        cachesAtomic = new GridCache[gridCnt];
        cachesAtomicOffheap = new GridCache[gridCnt];

        for (int i = 0; i < gridCount(); i++) {
            cachesAtomic[i] = grids[i].cache(CACHE_ATOMIC);
            cachesAtomicOffheap[i] = grids[i].cache(CACHE_ATOMIC_OFFHEAP);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cachesAtomic = null;
        cachesAtomicOffheap = null;

        super.afterTestsStopped();
    }

    /**
     * Test atomic cache.
     *
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        testAtomic0(cachesAtomic);
    }

    /**
     * Test atomic offheap cache.
     *
     * @throws Exception If failed.
     */
    public void testAtomicOffheap() throws Exception {
        testAtomic0(cachesAtomicOffheap);
    }

    /**
     * INternal routine for ATOMIC cache testing.
     *
     * @param caches Caches.
     * @throws Exception If failed.
     */
    private void testAtomic0(GridCache<Integer, Object>[] caches) throws Exception {
        byte[] val = wrap(1);

        for (GridCache<Integer, Object> cache : caches) {
            cache.put(KEY_1, val);

            for (GridCache<Integer, Object> cacheInner : caches)
                Arrays.equals(val, (byte[]) cacheInner.get(KEY_1));

            cache.remove(KEY_1);
        }
    }
}
