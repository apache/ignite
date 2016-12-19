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

package org.apache.ignite;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 * Ups grid with 10 nodes and constantly restarts random two of them.
 * Finally leads to grid hang on exchange.
 */
public class GridExchangeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMetricsLogFrequency(0);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setCacheMode(CacheMode.LOCAL);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(new TcpDiscoverySharedFsIpFinder());

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * Ups grid with 10 nodes and constantly restarts random two of them.
     * Finally leads to grid hang on exchange.
     *
     * @throws Exception If failed.
     */
    public void testExchange() throws Exception {
        for (int i = 0; i < 10; i++)
            startGrid(i);

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                //noinspection InfiniteLoopStatement
                while (true) {
                    int nodeId1 = 0;
                    int nodeId2 = 0;

                    while (nodeId1 == 0 || nodeId2 == 0 || nodeId1 == nodeId2) {
                        nodeId1 = rnd.nextInt(10);
                        nodeId2 = rnd.nextInt(10);
                    }

                    shutDownGrid(nodeId1);
                    shutDownGrid(nodeId2);

                    Thread.sleep(500);

                    startGrid(nodeId1);
                    startGrid(nodeId2);

                    System.out.println(">> Restarting grids: [idx1=" + nodeId1 + ", idx2=" + nodeId2 + ']');
                }
            }
        });

        final IgniteCache<Integer, String> cache = grid(0).getOrCreateCache("testCache");

        //noinspection InfiniteLoopStatement
        while (true) {
            final IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < 10_000; i++)
                        cache.put(i, String.valueOf(i));

                    for (int i = 0; i < 10_000; i++)
                        cache.get(i);

                    for (int i = 0; i < 10_000; i++)
                        cache.remove(i);

                    return null;
                }
            });

            // On hang will throw IgniteFutureTimeoutCheckedException.
            fut.get(60, TimeUnit.SECONDS);

            System.out.println(">> Got result");
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60_000L; // Work til failure.
    }
}
