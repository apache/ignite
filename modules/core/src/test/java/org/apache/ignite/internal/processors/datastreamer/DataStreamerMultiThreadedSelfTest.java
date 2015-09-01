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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for DataStreamer.
 */
public class DataStreamerMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean dynamicCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        if (!dynamicCache)
            cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopIgnites() throws Exception {
        startStopIgnites();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopIgnitesDynamicCache() throws Exception {
        dynamicCache = true;

        startStopIgnites();
    }

    /**
     * @throws Exception If failed.
     */
    private void startStopIgnites() throws Exception {
        for (int attempt = 0; attempt < 3; ++attempt) {
            log.info("Iteration: " + attempt);
            
            final Ignite ignite = startGrid(0);

            Set<IgniteFuture> futs = new HashSet<>();

            final AtomicInteger igniteId = new AtomicInteger(1);

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 1; i < 5; ++i)
                        startGrid(igniteId.incrementAndGet());

                    return true;
                }
            }, 2, "start-node-thread");

            if (dynamicCache)
                ignite.getOrCreateCache(cacheConfiguration());

            try (final DataStreamerImpl dataLdr = (DataStreamerImpl)ignite.dataStreamer(null)) {
                dataLdr.maxRemapCount(0);

                Random rnd = new Random();

                long endTime = U.currentTimeMillis() + 15_000;

                while (!fut.isDone() && U.currentTimeMillis() < endTime)
                    futs.add(dataLdr.addData(rnd.nextInt(100_000), String.valueOf(rnd.nextInt(100_000))));
            }

            for (IgniteFuture f : futs)
                f.get();

            fut.get();

            stopAllGrids();
        }
    }
}