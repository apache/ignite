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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests {@link TcpDiscoverySpi} in client mode with multiple client nodes that interact with a cache concurrently.
 */
public class GridCacheTcpClientDiscoveryMultiThreadedTest extends GridCacheAbstractSelfTest {
    /** Server nodes count. */
    private static int srvNodesCnt;

    /** Client nodes count. */
    private static int clientNodesCnt;

    /** Client node or not. */
    private static boolean client;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return srvNodesCnt + clientNodesCnt;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Filling configuration for client nodes
        if (client) {
            TcpDiscoveryVmIpFinder clientFinder = new TcpDiscoveryVmIpFinder();
            Collection<String> addrs = new ArrayList<>(ipFinder.getRegisteredAddresses().size());

            for (InetSocketAddress sockAddr : ipFinder.getRegisteredAddresses())
                addrs.add(sockAddr.getHostString() + ":" + sockAddr.getPort());

            clientFinder.setAddresses(addrs);

            cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(clientFinder));

            cfg.setClientMode(true);
        }

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheConcurrentlyWithMultipleClientNodes() throws Exception {
        srvNodesCnt = 2;
        clientNodesCnt = 3;

        startServerNodes();

        client = true;

        for (int n = 0; n < 2; n++) {
            startGridsMultiThreaded(srvNodesCnt, clientNodesCnt);

            checkTopology(gridCount());

            awaitPartitionMapExchange();

            // Explicitly create near cache for even client nodes
            for (int i = srvNodesCnt; i < gridCount(); i++)
                grid(i).createNearCache(null, new NearCacheConfiguration<>());

            final AtomicInteger threadsCnt = new AtomicInteger();

            IgniteInternalFuture<?> f = multithreadedAsync(
                    new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            int clientIdx = srvNodesCnt + threadsCnt.getAndIncrement();

                            Ignite node = grid(clientIdx);

                            assert node.configuration().isClientMode();

                            IgniteCache<Integer, Integer> cache = node.cache(null);

                            boolean isNearCacheNode = clientIdx % 2 == 0;

                            for (int i = 100 * clientIdx; i < 100 * (clientIdx + 1); i++)
                                cache.put(i, i);

                            for (int i = 100 * clientIdx; i < 100 * (clientIdx + 1); i++) {
                                assertEquals(i, (int) cache.get(i));

                                if (isNearCacheNode)
                                    assertEquals(i, (int) cache.localPeek(i, CachePeekMode.ONHEAP));
                            }

                            stopGrid(clientIdx);

                            return null;
                        }
                    },
                    clientNodesCnt
            );

            f.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void startServerNodes() throws Exception {
        client = false;

        for (int i = 0; i < srvNodesCnt; i++)
            startGrid(i);
    }

    /**
     * @throws Exception If failed.
     */
    private void stopServerNodes() throws Exception {
        for (int i = 0; i < srvNodesCnt; i++)
            stopGrid(i);
    }

    /**
     * Executes simple operation on the cache.
     *
     * @param cache Cache instance to use.
     */
    private void performSimpleOperationsOnCache(IgniteCache<Integer, Integer> cache) {
        for (int i = 100; i < 200; i++)
            cache.put(i, i);

        for (int i = 100; i < 200; i++)
            assertEquals(i, (int) cache.get(i));
    }
}