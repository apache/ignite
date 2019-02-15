/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.managers.discovery;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.log4j.Level;
import org.junit.Test;

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.endsWith("client"))
            cfg.setClientMode(true);

        if (igniteInstanceName.endsWith("client_force_server")) {
            cfg.setClientMode(true);
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        }

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
    @Test
    public void testServerLogs() throws Exception {
        MockLogger log = new MockLogger();

        log.setLevel(Level.INFO);

        doServerLogTest(log);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
        String nodeId8;

        try {
            Ignite server = startGrid("server");

            nodeId8 = U.id8(server.cluster().localNode().id());

            setLogger(log, server);

            Ignite server1 = startGrid("server1");

            waitForDiscovery(server, server1);
        }
        finally {
            stopAllGrids();
        }

        assertTrue(F.forAny(log.logs(), new IgnitePredicate<String>() {
            @Override public boolean apply(String s) {
                return s.contains("Topology snapshot [ver=2, locNode=" + nodeId8 + ", servers=2, clients=0,")
                    || (s.contains(">>> Number of server nodes: 2") && s.contains(">>> Number of client nodes: 0"));
            }
        }));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerAndClientLogs() throws Exception {
        MockLogger log = new MockLogger();

        log.setLevel(Level.INFO);

        doServerAndClientTest(log);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
        String nodeId8;

        try {
            Ignite server = startGrid("server");

            nodeId8 = U.id8(server.cluster().localNode().id());

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
                return s.contains("Topology snapshot [ver=4, locNode=" + nodeId8 + ", servers=2, clients=2,")
                    || (s.contains(">>> Number of server nodes: 2") && s.contains(">>> Number of client nodes: 2"));
            }
        }));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testForceServerAndClientLogs() throws Exception {
        MockLogger log = new MockLogger();

        log.setLevel(Level.INFO);

        doForceServerAndClientTest(log);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
        String nodeId8;

        try {
            Ignite server = startGrid("server");

            nodeId8 = U.id8(server.cluster().localNode().id());

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
                return s.contains("Topology snapshot [ver=5, locNode=" + nodeId8 + ", servers=2, clients=3,")
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
