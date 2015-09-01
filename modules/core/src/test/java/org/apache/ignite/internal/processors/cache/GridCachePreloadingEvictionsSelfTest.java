/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 *
 */
public class GridCachePreloadingEvictionsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String VALUE = createValue();
    public static final CachePeekMode[] ALL_PEEK_MODES = new CachePeekMode[]{CachePeekMode.ALL};

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private final AtomicInteger idxGen = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        CacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setAffinity(new GridCacheModuloAffinityFunction(1, 1));
        partCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        partCacheCfg.setNearConfiguration(null);
        partCacheCfg.setEvictSynchronized(true);
        partCacheCfg.setSwapEnabled(false);
        partCacheCfg.setEvictionPolicy(null);
        partCacheCfg.setEvictSynchronizedKeyBufferSize(25);
        partCacheCfg.setEvictMaxOverflowRatio(0.99f);
        partCacheCfg.setRebalanceMode(ASYNC);
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);

        // This test requires artificial slowing down of the preloading.
        partCacheCfg.setRebalanceThrottle(2000);

        cfg.setCacheConfiguration(partCacheCfg);

        cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, idxGen.getAndIncrement()));

        cfg.setNetworkTimeout(60000);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictions() throws Exception {
        try {
            final Ignite ignite1 = startGrid(1);

            IgniteCache<Integer, Object> cache1 = ignite1.cache(null);

            for (int i = 0; i < 5000; i++)
                cache1.put(i, VALUE + i);

            info("Finished data population.");

            final AtomicBoolean done = new AtomicBoolean();

            final CountDownLatch startLatch = new CountDownLatch(1);

            int oldSize = cache1.localSize(CachePeekMode.ALL);

            IgniteInternalFuture fut = multithreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        startLatch.await();

                        info("Started evicting...");

                        for (int i = 0; i < 3000 && !done.get(); i++) {
                            Cache.Entry<Integer, Object> entry = randomEntry(ignite1);

                            if (entry != null)
                                ignite1.cache(null).localEvict(Collections.<Object>singleton(entry.getKey()));
                            else
                                info("Entry is null.");
                        }

                        info("Finished evicting.");

                        return null;
                    }
                },
                1);

            ignite1.events().localListen(
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
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

            oldSize = cache1.size(CachePeekMode.ALL);

            info("Evicting on constant topology.");

            for (int i = 0; i < 1000; i++) {
                Cache.Entry<Integer, Object> entry = randomEntry(ignite1);

                if (entry != null)
                    cache1.localEvict(Collections.singleton(entry.getKey()));
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
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void sleepUntilCashesEqualize(final Ignite ignite1, final Ignite ignite2, final int oldSize)
        throws IgniteInterruptedCheckedException {
        info("Sleeping...");

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                int size1 = ignite1.cache(null).localSize(CachePeekMode.ALL);
                return size1 != oldSize && size1 == ignite2.cache(null).localSize(CachePeekMode.ALL);
            }
        }, getTestTimeout()));

        info("Sleep finished.");
    }

    /**
     * @param g Grid.
     * @return Random entry from cache.
     */
    @Nullable private Cache.Entry<Integer, Object> randomEntry(Ignite g) {
        return g.<Integer, Object>cache(null).randomEntry();
    }

    /**
     * @param ignite1 Grid 1.
     * @param ignite2 Grid 2.
     * @throws Exception If failed.
     */
    private void checkCachesConsistency(Ignite ignite1, Ignite ignite2) throws Exception {
        IgniteKernal g1 = (IgniteKernal) ignite1;
        IgniteKernal g2 = (IgniteKernal) ignite2;

        GridCacheAdapter<Integer, Object> cache1 = g1.internalCache();
        GridCacheAdapter<Integer, Object> cache2 = g2.internalCache();

        for (int i = 0; i < 3; i++) {
            if (cache1.size(ALL_PEEK_MODES) != cache2.size(ALL_PEEK_MODES)) {
                U.warn(log, "Sizes do not match (will retry in 1000 ms) [s1=" + cache1.size(ALL_PEEK_MODES) +
                    ", s2=" + cache2.size(ALL_PEEK_MODES) + ']');

                U.sleep(1000);
            }
            else
                break;
        }

        info("Cache1 size: " + cache1.size(ALL_PEEK_MODES));
        info("Cache2 size: " + cache2.size(ALL_PEEK_MODES));

        assert cache1.size(ALL_PEEK_MODES) == cache2.size(ALL_PEEK_MODES) :
            "Sizes do not match [s1=" + cache1.size(ALL_PEEK_MODES) + ", s2=" + cache2.size(ALL_PEEK_MODES) + ']';

        for (Integer key : cache1.keySet()) {
            Object e = cache1.localPeek(key, new CachePeekMode[] {CachePeekMode.ONHEAP}, null);

            if (e != null)
                assert cache2.containsKey(key) : "Cache2 does not contain key: " + key;
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