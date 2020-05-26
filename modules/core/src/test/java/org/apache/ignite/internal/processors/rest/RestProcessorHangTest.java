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

package org.apache.ignite.internal.processors.rest;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;


/**
 * Test for rest processor hanging on stop
 */
public class RestProcessorHangTest extends GridCommonAbstractTest {
    /**
     *
     */
    private static final TcpDiscoveryIpFinder sharedStaticIpFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration();

        cfg.setConnectorConfiguration(connectorConfiguration);

        TcpDiscoverySpi spi = (TcpDiscoverySpi) cfg.getDiscoverySpi();

        spi.setIpFinder(sharedStaticIpFinder);

        return cfg;
    }

    /**
     * Test that node doesn't hang if there are rest requests and discovery SPI failed
     */
    public void testNodeStopOnDiscoverySpiFailTest() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        String rejectorGridName = "rejector";

        IgniteConfiguration rejectorGridCfg = getConfiguration(rejectorGridName);

        // discovery spi that never allows connecting
        TestTcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi() {
            @Override
            protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res, long timeout) throws IOException {
                try {
                    // wait until request added to rest processor
                    latch.await();
                } catch (InterruptedException ignored) {
                }

                super.writeToSocket(msg, sock, 255, timeout);
            }
        };

        discoSpi.setIpFinder(sharedStaticIpFinder);

        rejectorGridCfg.setDiscoverySpi(discoSpi);

        startGrid(rejectorGridCfg);

        String hangGridName = "impossibleToJoin";

        IgniteConfiguration hangNodeCfg = getConfiguration(hangGridName);

        GridTestUtils.runAsync(() -> startGrid(hangNodeCfg));

        GridTestUtils.waitForCondition(() -> {
            try {
                IgniteKernal failingGrid = IgnitionEx.gridx(hangGridName);

                return failingGrid != null && failingGrid.context().rest() != null;
            } catch (Exception ignored) {
                return false;
            }
        }, 20_000);

        IgniteEx hangGrid = IgnitionEx.gridx(hangGridName);

        GridRestProcessor rest = hangGrid.context().rest();

        new Thread(() -> {
            GridRestProtocolHandler hnd = GridTestUtils.getFieldValue(rest, "protoHnd");

            GridRestCacheRequest req = new GridRestCacheRequest();

            req.cacheName(DEFAULT_CACHE_NAME);

            req.command(GridRestCommand.CACHE_GET);

            req.key("k1");

            latch.countDown();

            try {
                // submitting cache get request to node that didn't fully start
                // must hang
                hnd.handle(req);
            } catch (IgniteCheckedException ignored) {
            }
        }).start();

        latch.await();

        // node should stop correctly
        assertTrue(GridTestUtils.waitForCondition(() -> {
            List<Ignite> ignites = IgnitionEx.allGrids();
            return ignites.stream().noneMatch(ignite -> Objects.equals(ignite.name(), hangGridName));
        }, 20_000));

        stopGrid(rejectorGridName);
    }

}
