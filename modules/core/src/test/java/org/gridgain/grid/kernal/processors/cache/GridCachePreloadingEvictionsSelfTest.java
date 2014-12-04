/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.apache.ignite.events.GridEventType.*;

/**
 *
 */
public class GridCachePreloadingEvictionsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String VALUE = createValue();

    /** */
    private final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private final AtomicInteger idxGen = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        GridCacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setAffinity(new GridCacheModuloAffinityFunction(1, 1));
        partCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        partCacheCfg.setDistributionMode(PARTITIONED_ONLY);
        partCacheCfg.setEvictSynchronized(true);
        partCacheCfg.setSwapEnabled(false);
        partCacheCfg.setEvictionPolicy(null);
        partCacheCfg.setEvictSynchronizedKeyBufferSize(25);
        partCacheCfg.setEvictMaxOverflowRatio(0.99f);
        partCacheCfg.setPreloadMode(ASYNC);
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);

        // This test requires artificial slowing down of the preloading.
        partCacheCfg.setPreloadThrottle(2000);

        cfg.setCacheConfiguration(partCacheCfg);

        cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, idxGen.getAndIncrement()));

        cfg.setNetworkTimeout(60000);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public void testEvictions() throws Exception {
        try {
            final Ignite ignite1 = startGrid(1);

            GridCache<Integer, Object> cache1 = ignite1.cache(null);

            for (int i = 0; i < 5000; i++)
                cache1.put(i, VALUE + i);

            info("Finished data population.");

            final AtomicBoolean done = new AtomicBoolean();

            final CountDownLatch startLatch = new CountDownLatch(1);

            int oldSize = cache1.size();

            IgniteFuture fut = multithreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        startLatch.await();

                        info("Started evicting...");

                        for (int i = 0; i < 3000 && !done.get(); i++) {
                            GridCacheEntry<Integer, Object> entry = randomEntry(ignite1);

                            if (entry != null)
                                entry.evict();
                            else
                                info("Entry is null.");
                        }

                        info("Finished evicting.");

                        return null;
                    }
                },
                1);

            ignite1.events().localListen(
                new IgnitePredicate<IgniteEvent>() {
                    @Override public boolean apply(IgniteEvent evt) {
                        startLatch.countDown();

                        return true;
                    }
                },
                EVT_NODE_JOINED);

            final Ignite ignite2 = startGrid(2);

            done.set(true);

            fut.get();

            sleepUntilCashesEqualize(ignite1, ignite2, oldSize);

            checkCachesConsistency(ignite1, ignite2);

            oldSize = cache1.size();

            info("Evicting on constant topology.");

            for (int i = 0; i < 1000; i++) {
                GridCacheEntry<Integer, Object> entry = randomEntry(ignite1);

                if (entry != null)
                    entry.evict();
                else
                    info("Entry is null.");
            }

            sleepUntilCashesEqualize(ignite1, ignite2, oldSize);

            checkCachesConsistency(ignite1, ignite2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Waits until cache stabilizes on new value.
     *
     * @param ignite1 Grid 1.
     * @param ignite2 Grid 2.
     * @param oldSize Old size, stable size should be .
     * @throws GridInterruptedException If interrupted.
     */
    private void sleepUntilCashesEqualize(final Ignite ignite1, final Ignite ignite2, final int oldSize)
        throws GridInterruptedException {
        info("Sleeping...");

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                int size1 = ignite1.cache(null).size();
                return size1 != oldSize && size1 == ignite2.cache(null).size();
            }
        }, getTestTimeout()));

        info("Sleep finished.");
    }

    /**
     * @param g Grid.
     * @return Random entry from cache.
     */
    @Nullable private GridCacheEntry<Integer, Object> randomEntry(Ignite g) {
        GridKernal g1 = (GridKernal)g;

        return g1.<Integer, Object>internalCache().randomEntry();
    }

    /**
     * @param ignite1 Grid 1.
     * @param ignite2 Grid 2.
     * @throws Exception If failed.
     */
    private void checkCachesConsistency(Ignite ignite1, Ignite ignite2) throws Exception {
        GridKernal g1 = (GridKernal) ignite1;
        GridKernal g2 = (GridKernal) ignite2;

        GridCacheAdapter<Integer, Object> cache1 = g1.internalCache();
        GridCacheAdapter<Integer, Object> cache2 = g2.internalCache();

        for (int i = 0; i < 3; i++) {
            if (cache1.size() != cache2.size()) {
                U.warn(log, "Sizes do not match (will retry in 1000 ms) [s1=" + cache1.size() +
                    ", s2=" + cache2.size() + ']');

                U.sleep(1000);
            }
            else
                break;
        }

        info("Cache1 size: " + cache1.size());
        info("Cache2 size: " + cache2.size());

        assert cache1.size() == cache2.size() : "Sizes do not match [s1=" + cache1.size() +
            ", s2=" + cache2.size() + ']';

        for (Integer key : cache1.keySet()) {
            Object e = cache1.peek(key);

            if (e != null)
                assert cache2.containsKey(key, null) : "Cache2 does not contain key: " + key;
        }
    }

    /**
     * @return Large value for test.
     */
    private static String createValue() {
        SB sb = new SB(1024);

        for (int i = 0; i < 64; i++)
            sb.a("val1");

        return sb.toString();
    }
}
