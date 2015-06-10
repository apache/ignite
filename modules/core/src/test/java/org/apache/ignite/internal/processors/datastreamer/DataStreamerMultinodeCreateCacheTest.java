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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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

                    try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheName)) {
                        try (IgniteDataStreamer<Object, Object> stmr = ignite.dataStreamer(cacheName)) {
                            ((DataStreamerImpl<Object, Object>)stmr).maxRemapCount(0);

                            for (int i = 0; i < 1000; i++)
                                stmr.addData(i, i);
                        }
                    }

                    iter++;
                }

                return null;
            }
        }, THREADS, "create-cache");

        fut.get(2 * 60_000);
    }
}
