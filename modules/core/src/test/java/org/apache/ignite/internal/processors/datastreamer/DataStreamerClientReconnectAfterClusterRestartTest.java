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

import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests DataStreamer reconnect behaviour when client nodes arrives at the same or different topVer than it left.
 */
public class DataStreamerClientReconnectAfterClusterRestartTest extends GridCommonAbstractTest {
    /** */
    public static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(new CacheConfiguration<>("test"));

        cfg.setClientMode(clientMode);

        return cfg;
    }

    /** */
    public void testOneClient() throws Exception {
        clusterRestart(false, false);
    }

    /** */
    public void testOneClientAllowOverwrite() throws Exception {
        clusterRestart(false, true);
    }

    /** */
    public void testTwoClients() throws Exception {
        clusterRestart(true, false);
    }

    /** */
    public void testTwoClientsAllowOverwrite() throws Exception {
        clusterRestart(true, true);
    }

    /** */
    private void clusterRestart(boolean withAnotherClient, boolean allowOverwrite) throws Exception {
        try {
            startGrid(0);

            clientMode = true;

            Ignite client = startGrid(1);

            if (withAnotherClient) {
                startGrid(2);

                stopGrid(2);
            }

            clientMode = false;

            try (IgniteDataStreamer<String, String> streamer = client.dataStreamer("test")) {
                streamer.allowOverwrite(allowOverwrite);

                streamer.addData("k1", "v1");
            }

            stopGrid(0);

            U.sleep(2_000);

            startGrid(0);

            U.sleep(1_000);

            for (int i = 0; i < 3; i++) {
                try (IgniteDataStreamer<String, String> streamer = client.dataStreamer("test")) {
                    streamer.allowOverwrite(allowOverwrite);

                    streamer.addData("k2", "v2");

                    return;
                }
                catch (CacheException ce) {
                    assert ce.getCause() instanceof IgniteClientDisconnectedException;

                    ((IgniteClientDisconnectedException)ce.getCause()).reconnectFuture().get();
                }
                catch (IgniteClientDisconnectedException icde) {
                    icde.reconnectFuture().get();
                }
            }

            assert false : "Failed to wayt for reconnect!";
        }
        finally {
            stopAllGrids();
        }
    }
}
