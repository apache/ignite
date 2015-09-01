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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static java.lang.String.valueOf;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Memory model self test.
 */
@SuppressWarnings("deprecation")
public class GridCacheMemoryModeSelfTest extends GridCommonAbstractTest {
    /** */
    private TcpDiscoveryIpFinder ipFinder;

    /** */
    private boolean swapEnabled;

    /** */
    private CacheMode mode;

    /** */
    private CacheMemoryMode memoryMode;

    /** */
    private int maxOnheapSize;

    /** */
    private long offheapSize;

    /** */
    private CacheAtomicityMode atomicity;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * Returns cache configuration.
     *
     * @return cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        cacheCfg.setSwapEnabled(swapEnabled);
        cacheCfg.setCacheMode(mode);
        cacheCfg.setMemoryMode(memoryMode);

        LruEvictionPolicy plc = null;

        if (maxOnheapSize != Integer.MAX_VALUE) {
            plc = new LruEvictionPolicy();
            plc.setMaxSize(maxOnheapSize);
        }

        cacheCfg.setEvictionPolicy(plc);

        cacheCfg.setAtomicityMode(atomicity);
        cacheCfg.setOffHeapMaxMemory(offheapSize);

        return cacheCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnheap() throws Exception {
        mode = CacheMode.LOCAL;
        memoryMode = CacheMemoryMode.ONHEAP_TIERED;
        maxOnheapSize = Integer.MAX_VALUE;
        swapEnabled = false;
        atomicity = CacheAtomicityMode.ATOMIC;
        offheapSize = -1;

        doTestPutAndPutAll(1000, 0, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnheapSwap() throws Exception {
        mode = CacheMode.LOCAL;
        memoryMode = CacheMemoryMode.ONHEAP_TIERED;
        maxOnheapSize = 330;
        swapEnabled = true;
        atomicity = CacheAtomicityMode.ATOMIC;
        offheapSize = -1;

        doTestPutAndPutAll(330, 670, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffheap() throws Exception {
        mode = CacheMode.LOCAL;
        memoryMode = CacheMemoryMode.OFFHEAP_TIERED;
        maxOnheapSize = Integer.MAX_VALUE;
        swapEnabled = false;
        atomicity = CacheAtomicityMode.ATOMIC;
        offheapSize = -1; // Must be fixed in config validation.

        doTestPutAndPutAll(0, 1000, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffheapSwap() throws Exception {
        mode = CacheMode.LOCAL;
        memoryMode = CacheMemoryMode.OFFHEAP_TIERED;
        maxOnheapSize = Integer.MAX_VALUE;
        swapEnabled = true;
        atomicity = CacheAtomicityMode.ATOMIC;
        offheapSize = 1000; // Small for evictions from offheap to swap.

        doTestPutAndPutAll(0, 1000, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTiered() throws Exception {
        mode = CacheMode.LOCAL;
        memoryMode = CacheMemoryMode.ONHEAP_TIERED;
        maxOnheapSize = 24;
        swapEnabled = true;
        atomicity = CacheAtomicityMode.ATOMIC;
        offheapSize = 1000; // Small for evictions from offheap to swap.

        doTestPutAndPutAll(24, 976, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffheapValuesConfigFixBackward() throws Exception {
        mode = CacheMode.LOCAL;
        memoryMode = CacheMemoryMode.OFFHEAP_VALUES;
        maxOnheapSize = 24;
        swapEnabled = true;
        atomicity = CacheAtomicityMode.ATOMIC;
        offheapSize = -1;

        Ignite g = startGrid();

        CacheConfiguration cfg = g.cache(null).getConfiguration(CacheConfiguration.class);

        assertEquals(memoryMode, cfg.getMemoryMode());
        assertEquals(0, cfg.getOffHeapMaxMemory());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @param cache In cache.
     * @param offheapSwap In swap and offheap.
     * @param offheapEmpty Offheap is empty.
     * @param swapEmpty Swap is empty.
     * @throws Exception If failed.
     */
    private void doTestPutAndPutAll(int cache, int offheapSwap, boolean offheapEmpty, boolean swapEmpty)
        throws Exception {
        final int all = cache + offheapSwap;

        // put
        doTest(cache, offheapSwap, offheapEmpty, swapEmpty, new CIX1<IgniteCache<String, Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> c) throws IgniteCheckedException {
                for (int i = 0; i < all; i++)
                    c.put(valueOf(i), i);
            }
        });

        //putAll
        doTest(cache, offheapSwap, offheapEmpty, swapEmpty, new CIX1<IgniteCache<String, Integer>>() {
            @Override public void applyx(IgniteCache<String, Integer> c) throws IgniteCheckedException {
                Map<String, Integer> m = new HashMap<>();

                for (int i = 0; i < all; i++)
                    m.put(valueOf(i), i);

                c.putAll(m);
            }
        });
    }

    /**
     * @param cache Cache size.
     * @param offheapSwap Offheap + swap size.
     * @param offheapEmpty Offheap is empty.
     * @param swapEmpty Swap is empty.
     * @param x Cache modifier.
     * @throws IgniteCheckedException If failed.
     */
    void doTest(int cache, int offheapSwap, boolean offheapEmpty, boolean swapEmpty,
        CIX1<IgniteCache<String, Integer>> x) throws Exception {
        ipFinder = new TcpDiscoveryVmIpFinder(true);

        startGrid();

        final IgniteCache<String, Integer> c = jcache();

        x.applyx(c);

        assertEquals(cache, c.size(CachePeekMode.ONHEAP));
        assertEquals(offheapSwap, c.localSize(CachePeekMode.OFFHEAP) + c.localSize(CachePeekMode.SWAP));

        info("size: " + c.size());
        info("offheap: " + c.localSize(CachePeekMode.OFFHEAP));
        info("swap: " + c.localSize(CachePeekMode.SWAP));

        if (offheapEmpty)
            Assert.assertEquals(0, c.localSize(CachePeekMode.OFFHEAP));
        else
            Assert.assertNotEquals(0, c.localSize(CachePeekMode.OFFHEAP));

        if (swapEmpty)
            Assert.assertEquals(0, c.localSize(CachePeekMode.SWAP));
        else
            Assert.assertNotEquals(0, c.localSize(CachePeekMode.SWAP));

        stopAllGrids();
    }
}