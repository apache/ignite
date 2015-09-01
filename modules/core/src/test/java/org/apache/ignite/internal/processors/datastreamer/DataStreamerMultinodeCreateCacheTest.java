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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class DataStreamerMultinodeCreateCacheTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setSocketTimeout(50);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setAckTimeout(50);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateCacheAndStream() throws Exception {
        final int THREADS = 5;

        startGrids(THREADS);

        final AtomicInteger idx = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int threadIdx = idx.getAndIncrement();

                long stopTime = System.currentTimeMillis() + 60_000;

                Ignite ignite = grid(threadIdx);

                int iter = 0;

                while (System.currentTimeMillis() < stopTime) {
                    String cacheName = "cache-" + threadIdx + "-" + (iter % 10);

                    IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheName);

                    try (IgniteDataStreamer<Object, Object> stmr = ignite.dataStreamer(cacheName)) {
                        ((DataStreamerImpl<Object, Object>)stmr).maxRemapCount(0);

                        for (int i = 0; i < 1000; i++)
                            stmr.addData(i, i);
                    }

                    cache.destroy();

                    iter++;
                }

                return null;
            }
        }, THREADS, "create-cache");

        fut.get(2 * 60_000);
    }
}