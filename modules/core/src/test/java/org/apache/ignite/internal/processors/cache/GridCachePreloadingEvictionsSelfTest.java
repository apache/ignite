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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.events.EventType.*;

/**
 *
 */
public class GridCachePreloadingEvictionsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String VALUE = createValue();

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
        partCacheCfg.setDistributionMode(PARTITIONED_ONLY);
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
    @SuppressWarnings("BusyWait")
    public void testEvictions() throws Exception {
        try {
            final Ignite ignite1 = startGrid(1);

            IgniteCache<Integer, Object> cache1 = ignite1.jcache(null);

            for (int i = 0; i < 5000; i++)
                cache1.put(i, VALUE + i);

            info("Finished data population.");

            final AtomicBoolean done = new AtomicBoolean();

            final CountDownLatch startLatch = new CountDownLatch(1);

            int oldSize = cache1.localSize();

            IgniteInternalFuture fut = multithreadedAsync(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        startLatch.await();

                        info("Started evicting...");

                        for (int i = 0; i < 3000 && !done.get(); i++) {
                            Cache.Entry<Integer, Object> entry = randomEntry(ignite1);

                            if (entry != null)
                                ignite1.jcache(null).localEvict(Collections.<Object>singleton(entry.getKey()));
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

            oldSize = cache1.size();

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
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
     */
    private void sleepUntilCashesEqualize(final Ignite ignite1, final Ignite ignite2, final int oldSize)
        throws IgniteInterruptedCheckedException {
        info("Sleeping...");

        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                int size1 = ignite1.jcache(null).localSize();
                return size1 != oldSize && size1 == ignite2.jcache(null).localSize();
            }
        }, getTestTimeout()));

        info("Sleep finished.");
    }

    /**
     * @param g Grid.
     * @return Random entry from cache.
     */
    @Nullable private Cache.Entry<Integer, Object> randomEntry(Ignite g) {
        IgniteKernal g1 = (IgniteKernal)g;

        return g1.<Integer, Object>internalCache().randomEntry();
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
