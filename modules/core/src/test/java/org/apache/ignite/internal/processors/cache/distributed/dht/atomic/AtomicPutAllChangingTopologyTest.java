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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CachePeekMode.BACKUP;
import static org.apache.ignite.cache.CachePeekMode.PRIMARY;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public class AtomicPutAllChangingTopologyTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES_CNT = 3;

    /** */
    public static final String CACHE_NAME = "test-cache";

    /** */
    private static final int CACHE_SIZE = 20_000;

    /** */
    private static volatile CountDownLatch FILLED_LATCH;

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfig() {
        return new CacheConfiguration<Integer, Integer>()
            .setAtomicityMode(ATOMIC)
            .setCacheMode(REPLICATED)
            .setAffinity(new FairAffinityFunction(false, 1))
            .setWriteSynchronizationMode(FULL_SYNC)
            .setRebalanceMode(SYNC)
            .setName(CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllOnChangingTopology() throws Exception {
        List<IgniteInternalFuture> futs = new LinkedList<>();

        for (int i = 1; i < NODES_CNT; i++)
            futs.add(startNodeAsync(i));

        futs.add(startSeedNodeAsync());

        boolean failed = false;

        for (IgniteInternalFuture fut : futs) {
            try {
                fut.get();
            }
            catch (Throwable th) {
                log.error("Check failed.", th);

                failed = true;
            }
        }

        if (failed)
            throw new RuntimeException("Test Failed.");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        FILLED_LATCH = new CountDownLatch(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @return Future.
     * @throws IgniteCheckedException If failed.
     */
    private IgniteInternalFuture startSeedNodeAsync() throws IgniteCheckedException {
        return GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Boolean call() throws Exception {
                Ignite node = startGrid(0);

                log.info("Creating cache.");

                IgniteCache<Integer, Integer> cache = node.getOrCreateCache(cacheConfig());

                log.info("Created cache.");

                Map<Integer, Integer> data = new HashMap<>(CACHE_SIZE);

                for (int i = 0; i < CACHE_SIZE; i++)
                    data.put(i, i);

                log.info("Filling.");

                cache.putAll(data);

                log.info("Filled.");

                FILLED_LATCH.countDown();

                checkCacheState(node, cache);

                return true;
            }
        });
    }

    /**
     * @param nodeId Node index.
     * @return Future.
     * @throws IgniteCheckedException If failed.
     */
    private IgniteInternalFuture startNodeAsync(final int nodeId) throws IgniteCheckedException {
        return GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Boolean call() throws Exception {
                Ignite node = startGrid(nodeId);

                log.info("Getting cache.");

                IgniteCache<Integer, Integer> cache = node.getOrCreateCache(cacheConfig());

                log.info("Got cache.");

                FILLED_LATCH.await();

                log.info("Got Filled.");

                cache.put(1, nodeId);

                checkCacheState(node, cache);

                return true;
            }
        });
    }

    /**
     * @param node Node.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkCacheState(Ignite node, IgniteCache<Integer, Integer> cache) throws Exception {
        int locSize = cache.localSize(PRIMARY, BACKUP);
        int locSize2 = -1;

        if (locSize != CACHE_SIZE) {
            U.sleep(5000);

            // Rechecking.
            locSize2 = cache.localSize(PRIMARY, BACKUP);
        }

        assertEquals("Wrong cache size on node [node=" + node.configuration().getGridName() +
            ", expected= " + CACHE_SIZE +
            ", actual=" + locSize +
            ", actual2=" + locSize2 + "]",
            locSize, CACHE_SIZE);
    }
}