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

import java.util.Timer;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for missed client heartbeats.
 */
public class TcpDiscoveryClientSuspensionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);
        disco.setHeartbeatFrequency(200);
        disco.setMaxMissedClientHeartbeats(10);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOneServer() throws Exception {
        doTestClientSuspension(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoServers() throws Exception {
        doTestClientSuspension(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreeServers() throws Exception {
        doTestClientSuspension(3);
    }

    /**
     * @param serverCnt Servers count.
     * @throws Exception If failed.
     */
    private void doTestClientSuspension(int serverCnt) throws Exception {
        startGrids(serverCnt);

        Ignition.setClientMode(true);

        Ignite client = startGrid("client");

        for (int i = 0; i < serverCnt; i++)
            assertEquals(1, grid(i).cluster().forClients().nodes().size());

        Thread.sleep(3000);

        for (int i = 0; i < serverCnt; i++)
            assertEquals(1, grid(i).cluster().forClients().nodes().size());

        suspendClientHeartbeats(client);

        Thread.sleep(3000);

        for (int i = 0; i < serverCnt; i++)
            assertEquals(0, grid(i).cluster().forClients().nodes().size());
    }

    /**
     * @param client Client.
     */
    private void suspendClientHeartbeats(Ignite client) {
        assert client.cluster().localNode().isClient();

        ClientImpl impl = U.field(client.configuration().getDiscoverySpi(), "impl");

        impl.simulateNodeFailure();

        //Timer timer = U.field(impl, "timer"); timer.cancel(); -- client node successfully reconnects
    }
}
