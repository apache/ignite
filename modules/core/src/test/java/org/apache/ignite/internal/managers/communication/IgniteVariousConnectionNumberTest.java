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

package org.apache.ignite.internal.managers.communication;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteVariousConnectionNumberTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 6;

    /** */
    private static Random rnd = new Random();

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        int connections = rnd.nextInt(10) + 1;

        log.info("Node connections [name=" + gridName + ", connections=" + connections + ']');

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setConnectionsPerNode(connections);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setUsePairedConnections(rnd.nextBoolean());
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        long seed = U.currentTimeMillis();

        rnd.setSeed(seed);

        log.info("Random seed: " + seed);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testVariousConnectionNumber() throws Exception {
        startGridsMultiThreaded(3);

        client = true;

        startGridsMultiThreaded(3, 3);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(REPLICATED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        ignite(0).createCache(ccfg);

        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            runOperations(5000);

            awaitPartitionMapExchange();

            int idx = ThreadLocalRandom.current().nextInt(NODES);

            Ignite node = ignite(idx);

            client = node.configuration().isClientMode();

            stopGrid(idx);

            startGrid(idx);
        }
    }

    /**
     * @param time Execution time.
     * @throws Exception If failed.
     */
    private void runOperations(final long time) throws Exception {
        final AtomicInteger idx = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignite node = ignite(idx.getAndIncrement() % NODES);

                IgniteCache cache = node.cache(null);

                long stopTime = U.currentTimeMillis() + time;

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (U.currentTimeMillis() < stopTime) {
                    cache.put(rnd.nextInt(10_000), 0);

                    node.compute().broadcast(new DummyJob());
                }

                return null;
            }
        }, NODES * 10, "test-thread");
    }

    /**
     *
     */
    private static class DummyJob implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            // No-op.
        }
    }
}
