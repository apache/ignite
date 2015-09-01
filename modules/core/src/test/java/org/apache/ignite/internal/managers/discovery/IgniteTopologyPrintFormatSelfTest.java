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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.log4j.Level;

/**
 *
 */
public class IgniteTopologyPrintFormatSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String TOPOLOGY_SNAPSHOT = "Topology snapshot";

    /** */
    public static final String SERV_NODE = ">>> Number of server nodes";

    /** */
    public static final String CLIENT_NODE = ">>> Number of client nodes";

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disc = new TcpDiscoverySpi();
        disc.setIpFinder(IP_FINDER);

        if (gridName.endsWith("client"))
            cfg.setClientMode(true);

        if (gridName.endsWith("client_force_server")) {
            cfg.setClientMode(true);
            disc.setForceServerMode(true);
        }

        cfg.setDiscoverySpi(disc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (log instanceof MockLogger)
            ((MockLogger)log).clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerLogs() throws Exception {
        MockLogger log = new MockLogger();

        log.setLevel(Level.INFO);

        doServerLogTest(log);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerDebugLogs() throws Exception {
        MockLogger log = new MockLogger();

        log.setLevel(Level.DEBUG);

        doServerLogTest(log);
    }

    /**
     * @param log Logger.
     * @throws Exception If failed.
     */
    private void doServerLogTest(MockLogger log) throws Exception {
        try {
            Ignite server = startGrid("server");

            setLogger(log, server);

            Ignite server1 = startGrid("server1");

            waitForDiscovery(server, server1);
        }
        finally {
            stopAllGrids();
        }

        assertTrue(F.forAny(log.logs(), new IgnitePredicate<String>() {
            @Override public boolean apply(String s) {
                return s.contains("Topology snapshot [ver=2, servers=2, clients=0,")
                    || (s.contains(">>> Number of server nodes: 2") && s.contains(">>> Number of client nodes: 0"));
            }
        }));
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerAndClientLogs() throws Exception {
        MockLogger log = new MockLogger();

        log.setLevel(Level.INFO);

        doServerAndClientTest(log);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerAndClientDebugLogs() throws Exception {
        MockLogger log = new MockLogger();

        log.setLevel(Level.DEBUG);

        doServerAndClientTest(log);
    }

    /**
     * @param log Log.
     * @throws Exception If failed.
     */
    private void doServerAndClientTest(MockLogger log) throws Exception {
        try {
            Ignite server = startGrid("server");

            setLogger(log, server);

            Ignite server1 = startGrid("server1");
            Ignite client1 = startGrid("first client");
            Ignite client2 = startGrid("second client");

            waitForDiscovery(server, server1, client1, client2);
        }
        finally {
            stopAllGrids();
        }

        assertTrue(F.forAny(log.logs(), new IgnitePredicate<String>() {
            @Override public boolean apply(String s) {
                return s.contains("Topology snapshot [ver=4, servers=2, clients=2,")
                    || (s.contains(">>> Number of server nodes: 2") && s.contains(">>> Number of client nodes: 2"));
            }
        }));
    }

    /**
     * @throws Exception If failed.
     */
    public void testForceServerAndClientLogs() throws Exception {
        MockLogger log = new MockLogger();

        log.setLevel(Level.INFO);

        doForceServerAndClientTest(log);
    }

    /**
     * @throws Exception If failed.
     */
    public void testForceServerAndClientDebugLogs() throws Exception {
        MockLogger log = new MockLogger();

        log.setLevel(Level.DEBUG);

        doForceServerAndClientTest(log);
    }

    /**
     * @param log Log.
     * @throws Exception If failed.
     */
    private void doForceServerAndClientTest(MockLogger log) throws Exception {
        try {
            Ignite server = startGrid("server");

            setLogger(log, server);

            Ignite server1 = startGrid("server1");
            Ignite client1 = startGrid("first client");
            Ignite client2 = startGrid("second client");
            Ignite forceServClnt3 = startGrid("third client_force_server");

            waitForDiscovery(server, server1, client1, client2, forceServClnt3);
        }
        finally {
            stopAllGrids();
        }

        assertTrue(F.forAny(log.logs(), new IgnitePredicate<String>() {
            @Override public boolean apply(String s) {
                return s.contains("Topology snapshot [ver=5, servers=2, clients=3,")
                    || (s.contains(">>> Number of server nodes: 2") && s.contains(">>> Number of client nodes: 3"));
            }
        }));
    }

    /**
     * Set log.
     *
     * @param log Log.
     * @param server Ignite.
     */
    private void setLogger(MockLogger log, Ignite server) {
        IgniteKernal server0 = (IgniteKernal)server;

        GridDiscoveryManager discovery = server0.context().discovery();

        GridTestUtils.setFieldValue(discovery, GridManagerAdapter.class, "log", log);
    }

    /**
     *
     */
    private static class MockLogger extends GridTestLog4jLogger {
        /** */
        private List<String> logs = new ArrayList<>();

        /**  {@inheritDoc} */
        @Override public void debug(String msg) {
            if ((msg != null && !msg.isEmpty()) && (
                msg.contains(TOPOLOGY_SNAPSHOT)
                    || msg.contains(SERV_NODE)
                    || msg.contains(CLIENT_NODE)))
                logs.add(msg);

            super.debug(msg);
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            if ((msg != null && !msg.isEmpty()) && (
                msg.contains(TOPOLOGY_SNAPSHOT)
                || msg.contains(SERV_NODE)
                || msg.contains(CLIENT_NODE)))
                logs.add(msg);

            super.info(msg);
        }

        /**
         * @return Logs.
         */
        public List<String> logs() {
            return logs;
        }

        /** */
        public void clear() {
            logs.clear();
        }
    }
}