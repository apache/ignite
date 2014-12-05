/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.spi.swapspace.file.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.junit.Assert.*;

/**
 * Tests for byte array values in PARTITIONED-ONLY caches.
 */
public abstract class GridCacheAbstractPartitionedOnlyByteArrayValuesSelfTest extends
    GridCacheAbstractPartitionedByteArrayValuesSelfTest {
    /** Offheap cache name. */
    protected static final String CACHE_ATOMIC = "cache_atomic";

    /** Offheap cache name. */
    protected static final String CACHE_ATOMIC_OFFHEAP = "cache_atomic_offheap";

    /** Offheap tiered cache name. */
    protected static final String CACHE_ATOMIC_OFFHEAP_TIERED = "cache_atomic_offheap_tiered";

    /** Atomic caches. */
    private static GridCache<Integer, Object>[] cachesAtomic;

    /** Atomic offheap caches. */
    private static GridCache<Integer, Object>[] cachesAtomicOffheap;

    /** Atomic offheap caches. */
    private static GridCache<Integer, Object>[] cachesAtomicOffheapTiered;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration atomicCacheCfg = cacheConfiguration0();

        atomicCacheCfg.setName(CACHE_ATOMIC);
        atomicCacheCfg.setAtomicityMode(ATOMIC);
        atomicCacheCfg.setAtomicWriteOrderMode(PRIMARY);

        GridCacheConfiguration atomicOffheapCacheCfg = offheapCacheConfiguration0();

        atomicOffheapCacheCfg.setName(CACHE_ATOMIC_OFFHEAP);
        atomicOffheapCacheCfg.setAtomicityMode(ATOMIC);
        atomicOffheapCacheCfg.setAtomicWriteOrderMode(PRIMARY);

        GridCacheConfiguration atomicOffheapTieredCacheCfg = offheapTieredCacheConfiguration();

        atomicOffheapTieredCacheCfg.setName(CACHE_ATOMIC_OFFHEAP_TIERED);
        atomicOffheapTieredCacheCfg.setAtomicityMode(ATOMIC);
        atomicOffheapTieredCacheCfg.setAtomicWriteOrderMode(PRIMARY);

        c.setCacheConfiguration(cacheConfiguration(),
            offheapCacheConfiguration(),
            offheapTieredCacheConfiguration(),
            atomicCacheCfg,
            atomicOffheapCacheCfg,
            atomicOffheapTieredCacheCfg);

        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        c.setPeerClassLoadingEnabled(peerClassLoading());

        return c;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        int gridCnt = gridCount();

        cachesAtomic = new GridCache[gridCnt];
        cachesAtomicOffheap = new GridCache[gridCnt];
        cachesAtomicOffheapTiered = new GridCache[gridCnt];

        for (int i = 0; i < gridCount(); i++) {
            cachesAtomic[i] = ignites[i].cache(CACHE_ATOMIC);
            cachesAtomicOffheap[i] = ignites[i].cache(CACHE_ATOMIC_OFFHEAP);
            cachesAtomicOffheapTiered[i] = ignites[i].cache(CACHE_ATOMIC_OFFHEAP_TIERED);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cachesAtomic = null;
        cachesAtomicOffheap = null;
        cachesAtomicOffheapTiered = null;

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
     * Test atomic offheap cache.
     *
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTiered() throws Exception {
        testAtomic0(cachesAtomicOffheapTiered);
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
                assertArrayEquals(val, (byte[])cacheInner.get(KEY_1));

            cache.remove(KEY_1);

            assertNull(cache.get(KEY_1));
        }
    }
}
