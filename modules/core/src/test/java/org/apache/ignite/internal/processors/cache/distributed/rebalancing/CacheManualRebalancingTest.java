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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public class CacheManualRebalancingTest extends GridCommonAbstractTest {
    /** */
    private static final String MYCACHE = "mycache";

    /** */
    public static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(cacheConfiguration(), new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration(MYCACHE)
            .setAtomicityMode(ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setRebalanceMode(ASYNC)
            .setRebalanceDelay(-1)
            .setBackups(1)
            .setCopyOnRead(true)
            .setReadFromBackup(true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 400_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalance() throws Exception {
        // Fill cache with large dataset to make rebalancing slow.
        try (IgniteDataStreamer<Object, Object> streamer = grid(0).dataStreamer(MYCACHE)) {
            for (int i = 0; i < 100_000; i++)
                streamer.addData(i, i);
        }

        // Start new node.
        final IgniteEx newNode = startGrid(NODES_CNT);

        int newNodeCacheSize;

        // Start manual rebalancing.
        IgniteCompute compute = newNode.compute().withAsync();

        compute.broadcast(new MyCallable());

        final ComputeTaskFuture<Object> rebalanceTaskFuture = compute.future();

        boolean rebalanceFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return rebalanceTaskFuture.isDone();
            }
        }, 10_000);

        assertTrue(rebalanceFinished);

        assertTrue(newNode.context().cache().cache(MYCACHE).context().preloader().rebalanceFuture().isDone());

        newNodeCacheSize = newNode.cache(MYCACHE).localSize(CachePeekMode.ALL);

        System.out.println("New node cache local size: " + newNodeCacheSize);

        assertTrue(newNodeCacheSize > 0);

    }

    /** */
    public static class MyCallable implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        Ignite localNode;

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteLogger log = localNode.log();

            log.info("Start local rebalancing caches");

            for (String cacheName : localNode.cacheNames()) {
                IgniteCache<?, ?> cache = localNode.cache(cacheName);

                assertNotNull(cache);

                boolean finished;
                
                log.info("Start rebalancing cache: " + cacheName + ", size: " + cache.localSize());

                do {
                    IgniteFuture<?> rebalance = cache.rebalance();

                    log.info("Wait rebalancing cache: " + cacheName + " - " + rebalance);

                    finished = (Boolean)rebalance.get();

                    log.info("Rebalancing cache: " + cacheName + " - " + rebalance);

                    if (finished) {
                        log.info("Finished rebalancing cache: " + cacheName + ", size: " +
                            cache.localSize(CachePeekMode.PRIMARY) + cache.localSize(CachePeekMode.BACKUP));
                    } else
                        log.info("Rescheduled rebalancing cache: " + cacheName + ", size: " + cache.localSize());
                }
                while (!finished);
            }

            log.info("Finished local rebalancing caches");
        }
    }
}
