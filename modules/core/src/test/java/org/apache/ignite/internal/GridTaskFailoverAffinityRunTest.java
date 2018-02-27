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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 *
 */
public class GridTaskFailoverAffinityRunTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        boolean client = clientMode && gridName.equals(getTestGridName(0));

        if (client) {
            cfg.setClientMode(true);

            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        }

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setRebalanceMode(SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRestart() throws Exception {
        clientMode = false;

        nodeRestart();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRestartClient() throws Exception {
        clientMode = true;

        nodeRestart();
    }

    /**
     * @throws Exception If failed.
     */
    private void nodeRestart() throws Exception {
        startGridsMultiThreaded(4);

        assertEquals((Boolean)clientMode, grid(0).configuration().isClientMode());

        IgniteCompute comp = grid(0).compute().withAsync();

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicInteger gridIdx = new AtomicInteger(1);

        final long stopTime = System.currentTimeMillis() + 60_000;

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int grid = gridIdx.getAndIncrement();

                while (!stop.get() && System.currentTimeMillis() < stopTime) {
                    stopGrid(grid);

                    startGrid(grid);
                }

                return null;
            }
        }, 2, "restart-thread");

        try {
            while (System.currentTimeMillis() < stopTime) {
                Collection<IgniteFuture<?>> futs = new ArrayList<>(1000);

                for (int i = 0; i < 1000; i++) {
                    comp.affinityCall((String)null, i, new TestJob());

                    IgniteFuture<?> fut0 = comp.future();

                    assertNotNull(fut0);

                    futs.add(fut0);
                }

                for (IgniteFuture<?> fut0 : futs) {
                    try {
                        fut0.get();
                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            }
        }
        finally {
            stop.set(true);

            fut.get();
        }
    }

    /**
     *
     */
    private static class TestJob implements IgniteCallable<Object> {
        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            Thread.sleep(1000);

            return null;
        }
    }
}