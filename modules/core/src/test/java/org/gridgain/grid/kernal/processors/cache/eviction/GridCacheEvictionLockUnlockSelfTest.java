/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction;

import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 *
 */
public class GridCacheEvictionLockUnlockSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Evict latch. */
    private static CountDownLatch evictLatch;

    /** Evict counter. */
    private static final AtomicInteger evictCnt = new AtomicInteger();

    /** Touch counter. */
    private static final AtomicInteger touchCnt = new AtomicInteger();

    /** Cache mode. */
    private GridCacheMode mode;

    /** Number of grids to start. */
    private int gridCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);
        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setEvictionPolicy(new EvictionPolicy());
        cc.setNearEvictionPolicy(new EvictionPolicy());
        cc.setEvictNearSynchronized(false);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        if (mode == PARTITIONED)
            cc.setBackups(1);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        c.setDiscoverySpi(discoSpi);

        return c;
    }

    /** @throws Exception If failed. */
    public void testLocal() throws Exception {
        mode = LOCAL;
        gridCnt = 1;

        doTest();
    }

    /** @throws Exception If failed. */
    public void testReplicated() throws Exception {
        mode = REPLICATED;
        gridCnt = 3;

        doTest();
    }

    /** @throws Exception If failed. */
    public void testPartitioned() throws Exception {
        mode = PARTITIONED;
        gridCnt = 3;

        doTest();
    }

    /** @throws Exception If failed. */
    private void doTest() throws Exception {
        try {
            startGridsMultiThreaded(gridCnt);

            for (int i = 0; i < gridCnt; i++)
                grid(i).events().localListen(new EvictListener(), EVT_CACHE_ENTRY_EVICTED);

            for (int i = 0; i < gridCnt; i++) {
                reset();

                GridCache<Object, Object> cache = cache(i);

                cache.lock("key", 0L);
                cache.unlock("key");

                assertTrue(evictLatch.await(3, SECONDS));

                assertEquals(gridCnt, evictCnt.get());
                assertEquals(gridCnt, touchCnt.get());

                for (int j = 0; j < gridCnt; j++)
                    assertFalse(cache(j).containsKey("key"));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    private void reset() throws Exception {
        evictLatch = new CountDownLatch(gridCnt);

        evictCnt.set(0);
        touchCnt.set(0);
    }

    /** Eviction event listener. */
    private static class EvictListener implements IgnitePredicate<IgniteEvent> {
        /** {@inheritDoc} */
        @Override public boolean apply(IgniteEvent evt) {
            assert evt.type() == EVT_CACHE_ENTRY_EVICTED;

            evictCnt.incrementAndGet();

            evictLatch.countDown();

            return true;
        }
    }

    /** Eviction policy. */
    private static class EvictionPolicy implements GridCacheEvictionPolicy<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onEntryAccessed(boolean rmv, GridCacheEntry<Object, Object> entry) {
            touchCnt.incrementAndGet();

            entry.evict();
        }
    }
}
