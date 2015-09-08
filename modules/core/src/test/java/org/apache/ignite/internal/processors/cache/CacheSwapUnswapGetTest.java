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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheSwapUnswapGetTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final long DURATION = 30_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return cfg;
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param memMode Cache memory mode.
     * @param swap {@code True} if swap enabled.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, String> cacheConfiguration(CacheAtomicityMode atomicityMode,
        CacheMemoryMode memMode,
        boolean swap) {
        CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setMemoryMode(memMode);

        if (memMode == CacheMemoryMode.ONHEAP_TIERED) {
            LruEvictionPolicy plc = new LruEvictionPolicy();
            plc.setMaxSize(100);

            ccfg.setEvictionPolicy(plc);
        }

        if (swap) {
            ccfg.setSwapEnabled(true);

            ccfg.setOffHeapMaxMemory(1000);
        }
        else
            ccfg.setOffHeapMaxMemory(0);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return DURATION + 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCacheOffheapEvict() throws Exception {
        swapUnswap(TRANSACTIONAL, CacheMemoryMode.ONHEAP_TIERED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCacheOffheapTiered() throws Exception {
        swapUnswap(TRANSACTIONAL, CacheMemoryMode.OFFHEAP_TIERED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicCacheOffheapEvict() throws Exception {
        swapUnswap(ATOMIC, CacheMemoryMode.ONHEAP_TIERED, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicCacheOffheapTiered() throws Exception {
        swapUnswap(ATOMIC, CacheMemoryMode.OFFHEAP_TIERED, false);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param memMode Cache memory mode.
     * @param swap {@code True} if swap enabled.
     * @throws Exception If failed.
     */
    private void swapUnswap(CacheAtomicityMode atomicityMode, CacheMemoryMode memMode, boolean swap) throws Exception {
        log.info("Start test [mode=" + atomicityMode + ", swap=" + swap + ']');

        int threadCnt = 20;
        final int keyCnt = 1000;
        final int valCnt = 10000;

        final Ignite g = grid(0);

        final ConcurrentHashSet<Integer> keys = new ConcurrentHashSet<>();

        final AtomicBoolean done = new AtomicBoolean();

        g.destroyCache(null);

        final IgniteCache<Integer, String> cache = g.createCache(cacheConfiguration(atomicityMode, memMode, swap));

        try {
            IgniteInternalFuture<?> fut = multithreadedAsync(new CAX() {
                @Override public void applyx() throws IgniteCheckedException {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!done.get()) {
                        Integer key = rnd.nextInt(keyCnt);

                        switch (rnd.nextInt(3)) {
                            case 0:
                                cache.put(key, String.valueOf(rnd.nextInt(valCnt)));

                                keys.add(key);

                                break;

                            case 1:
                                cache.localEvict(Collections.singletonList(key));

                                break;

                            case 2:
                                if (keys.contains(key)) {
                                    String val = cache.get(key);

                                    assertNotNull(val);
                                }

                                break;

                            default:
                                assert false;
                        }
                    }
                }
            }, threadCnt, "update-thread");

            IgniteInternalFuture<?> getFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Thread.currentThread().setName("get-thread");

                    while (!done.get()) {
                        for (Integer key : keys) {
                            String val = cache.get(key);

                            assertNotNull(val);
                        }
                    }

                    return null;
                }
            });

            Thread.sleep(DURATION);

            done.set(true);

            fut.get();
            getFut.get();
        }
        finally {
            done.set(true);
        }
    }
}
