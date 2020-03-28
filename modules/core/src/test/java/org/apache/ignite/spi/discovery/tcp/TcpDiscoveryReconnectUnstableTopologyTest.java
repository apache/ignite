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

package org.apache.ignite.spi.discovery.tcp;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.DiscoverySpiTestListener;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test scenario:
 *
 * 1. Create topology in specific order: srv1 srv2 client srv3 srv4
 * 2. Delay client reconnect.
 * 3. Trigger topology change by restarting srv2 (will trigger reconnect to next node), srv3, srv4
 * 4. Resume reconnect to node with empty EnsuredMessageHistory and wait for completion.
 * 5. Add new node to topology.
 *
 * Pass condition: new node successfully joins topology.
 */
public class TcpDiscoveryReconnectUnstableTopologyTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setAutoActivationEnabled(false);

        BlockTcpDiscoverySpi spi = new BlockTcpDiscoverySpi();

        // Guarantees client join to srv2.
        Field rndAddrsField = U.findField(BlockTcpDiscoverySpi.class, "skipAddrsRandomization");

        assertNotNull(rndAddrsField);

        rndAddrsField.set(spi, true);

        cfg.setDiscoverySpi(spi.setIpFinder(ipFinder));
        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectUnstableTopology() throws Exception {
        try {
            List<IgniteEx> nodes = new ArrayList<>();

            nodes.add(startGrid(0));

            nodes.add(startGrid(1));

            nodes.add(startClientGrid("client"));

            nodes.add(startGrid(2));

            nodes.add(startGrid(3));

            for (int i = 0; i < nodes.size(); i++) {
                IgniteEx ex = nodes.get(i);

                assertEquals(i + 1, ex.localNode().order());
            }

            DiscoverySpiTestListener lsnr = new DiscoverySpiTestListener();

            spi(grid("client")).setInternalListener(lsnr);

            lsnr.startBlockReconnect();

            CountDownLatch restartLatch = new CountDownLatch(1);

            IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
                stopGrid(1);
                stopGrid(2);
                stopGrid(3);
                try {
                    startGrid(1);
                    startGrid(2);
                    startGrid(3);
                }
                catch (Exception e) {
                    fail();
                }

                restartLatch.countDown();
            }, 1, "restarter");

            U.awaitQuiet(restartLatch);

            lsnr.stopBlockRestart();

            fut.get();

            doSleep(1500); // Wait for reconnect.

            startGrid(4);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ig Ignite.
     */
    private TcpDiscoverySpi spi(Ignite ig) {
        return (TcpDiscoverySpi)ig.configuration().getDiscoverySpi();
    }
}
