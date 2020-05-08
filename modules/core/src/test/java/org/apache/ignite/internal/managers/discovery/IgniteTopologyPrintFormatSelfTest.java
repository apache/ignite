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

package org.apache.ignite.internal.managers.discovery;

import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.log4j.Level;

/**
 *
 */
public class IgniteTopologyPrintFormatSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String ALIVE_NODES_MSG = ".*aliveNodes=\\[(TcpDiscoveryNode " +
        "\\[id=[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}," +
        " consistentId=.*, addrs=ArrayList \\[([0-9]{1,3}[\\.]){3}[0-9]{1,3}], sockAddrs=HashSet \\[.*]," +
        " discPort=.*, order=[1-9], intOrder=[1-9], lastExchangeTime=.*, loc=(false|true), ver=.*," +
        " isClient=(false|true)](, )?){%s,%s}]]";

    /** */
    private static final String NUMBER_SRV_NODES = ">>> Number of server nodes: %d";

    /** */
    private static final String CLIENT_NODES_COUNT = ">>> Number of client nodes: %d";

    /** */
    private static final String TOPOLOGY_MSG = "Topology snapshot [ver=%d, locNode=%s, servers=%d, clients=%d,";

    /** */
    private ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog);

        TcpDiscoverySpi disc = new TcpDiscoverySpi();
        disc.setIpFinder(IP_FINDER);

        if (igniteInstanceName.endsWith("client"))
            cfg.setClientMode(true);

        if (igniteInstanceName.endsWith("client_force_server")) {
            cfg.setClientMode(true);
            disc.setForceServerMode(true);
        }

        cfg.setDiscoverySpi(disc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Checks topology snaphot message with two server nodes in INFO log level.
     *
     * @throws Exception If failed.
     */
    public void testServerLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.INFO);

        doServerLogTest();
    }

    /**
     * Checks topology snaphot message with two server nodes in DEBUG log level.
     *
     * @throws Exception If failed.
     */
    public void testServerDebugLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        doServerLogTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doServerLogTest() throws Exception {
        String nodeId8;

        testLog = new ListeningTestLogger(log);

        Pattern ptrn = Pattern.compile(String.format(ALIVE_NODES_MSG, 1, 2));

        LogListener aliveNodesLsnr = LogListener.matches(ptrn).times(log.isDebugEnabled() ? 0 : 4).build();

        testLog.registerListener(aliveNodesLsnr);

        LogListener lsnr;
        LogListener lsnr2;

        try {
            Ignite srv = startGrid("server");

            nodeId8 = U.id8(srv.cluster().localNode().id());

            lsnr = LogListener.matches(String.format(TOPOLOGY_MSG, 2, nodeId8, 2, 0)).build();

            lsnr2 = LogListener.matches(s -> s.contains(String.format(NUMBER_SRV_NODES, 2))
                && s.contains(String.format(CLIENT_NODES_COUNT, 0))).build();

            testLog.registerListener(lsnr);
            testLog.registerListener(lsnr2);

            Ignite srv1 = startGrid("server1");

            waitForDiscovery(srv, srv1);
        }
        finally {
            stopAllGrids();
        }

        checkLogMessages(aliveNodesLsnr, lsnr, lsnr2);
    }

    /**
     * Checks topology snaphot message with server and client nodes in INFO log level.
     *
     * @throws Exception If failed.
     */
    public void testServerAndClientLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.INFO);

        doServerAndClientTest();
    }

    /**
     * Checks topology snaphot message with server and client nodes in DEBUG log level.
     *
     * @throws Exception If failed.
     */
    public void testServerAndClientDebugLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        doServerAndClientTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doServerAndClientTest() throws Exception {
        String nodeId8;

        testLog = new ListeningTestLogger(log);

        Pattern ptrn = Pattern.compile(String.format(ALIVE_NODES_MSG, 1, 4));

        LogListener aliveNodesLsnr = LogListener.matches(ptrn).times(log.isDebugEnabled() ? 0 : 16).build();

        testLog.registerListener(aliveNodesLsnr);

        LogListener lsnr;
        LogListener lsnr2;

        try {
            Ignite srv = startGrid("server");

            nodeId8 = U.id8(srv.cluster().localNode().id());

            lsnr = LogListener.matches(String.format(TOPOLOGY_MSG, 4, nodeId8, 2, 2)).build();

            lsnr2 = LogListener.matches(s -> s.contains(String.format(NUMBER_SRV_NODES, 2))
                && s.contains(String.format(CLIENT_NODES_COUNT, 2))).build();

            testLog.registerListener(lsnr);
            testLog.registerListener(lsnr2);

            Ignite srv1 = startGrid("server1");
            Ignite client1 = startGrid("first client");
            Ignite client2 = startGrid("second client");

            waitForDiscovery(srv, srv1, client1, client2);
        }
        finally {
            stopAllGrids();
        }

        checkLogMessages(aliveNodesLsnr, lsnr, lsnr2);
    }

    /**
     * Checks topology snaphot message with server, client and force client nodes in INFO log level.
     *
     * @throws Exception If failed.
     */
    public void testForceServerAndClientLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.INFO);

        doForceServerAndClientTest();
    }

    /**
     * Checks topology snaphot message with server, client and force client nodes in DEBUG log level.
     *
     * @throws Exception If failed.
     */
    public void testForceServerAndClientDebugLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        doForceServerAndClientTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doForceServerAndClientTest() throws Exception {
        String nodeId8;

        testLog = new ListeningTestLogger(log);

        Pattern ptrn = Pattern.compile(String.format(ALIVE_NODES_MSG, 1, 4));

        LogListener aliveNodesLsnr = LogListener.matches(ptrn).times(log.isDebugEnabled() ? 0 : 25).build();

        testLog.registerListener(aliveNodesLsnr);

        LogListener lsnr;
        LogListener lsnr2;

        try {
            Ignite srv = startGrid("server");

            nodeId8 = U.id8(srv.cluster().localNode().id());

            lsnr = LogListener.matches(String.format(TOPOLOGY_MSG, 5, nodeId8, 2, 3)).build();

            lsnr2 = LogListener.matches(s -> s.contains(String.format(NUMBER_SRV_NODES, 2))
                && s.contains(String.format(CLIENT_NODES_COUNT, 3))).build();

            testLog.registerListener(lsnr);
            testLog.registerListener(lsnr2);

            Ignite srv1 = startGrid("server1");
            Ignite client1 = startGrid("first client");
            Ignite client2 = startGrid("second client");
            Ignite forceServClnt3 = startGrid("third client_force_server");

            waitForDiscovery(srv, srv1, client1, client2, forceServClnt3);
        }
        finally {
            stopAllGrids();
        }

        checkLogMessages(aliveNodesLsnr, lsnr, lsnr2);
    }

    /**
     * Check nodes details in log.
     *
     * @param aliveNodesLsnr log listener.
     */
    private void checkLogMessages(LogListener aliveNodesLsnr, LogListener lsnr, LogListener lsnr2) {
        if (testLog.isDebugEnabled()) {
            assertTrue(aliveNodesLsnr.check());
            assertFalse(lsnr.check());
            assertTrue(lsnr2.check());
        }
        else {
            assertTrue(aliveNodesLsnr.check());
            assertTrue(lsnr.check());
            assertFalse(lsnr2.check());
        }
    }
}
