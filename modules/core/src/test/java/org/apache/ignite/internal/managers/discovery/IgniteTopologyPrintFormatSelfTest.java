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
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
import org.junit.Test;

/**
 *
 */
public class IgniteTopologyPrintFormatSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String ALIVE_NODES_MSG = ".*aliveNodes=\\[(TcpDiscoveryNode " +
        "\\[id=[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}," +
        " consistentId=.*, isClient=(false|true), ver=.*](, )?){%s,%s}]]";

    /** */
    private static final String NUMBER_SRV_NODES = ">>> Number of server nodes: %d";

    /** */
    private static final String CLIENT_NODES_COUNT = ">>> Number of client nodes: %d";

    /** */
    private static final String TOPOLOGY_MSG = "Topology snapshot [ver=%d, locNode=%s, servers=%d, clients=%d,";

    /** */
    private ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** */
    public static final String COORDINATOR_CHANGED = "Coordinator changed";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog);
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
     * Checks topology snapshot message with two server nodes in INFO log level.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.INFO);

        doServerLogTest();
    }

    /**
     * Checks topology snapshot message with two server nodes in DEBUG log level.
     *
     * @throws Exception If failed.
     */
    @Test
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

        LogListener aliveNodesLsnr = LogListener.matches(ptrn).times(4).build();

        testLog.registerListener(aliveNodesLsnr);

        LogListener lsnr;
        LogListener lsnr2;

        try {
            Ignite srv = startGrid("server");

            nodeId8 = U.id8(srv.cluster().localNode().id());

            lsnr = LogListener.matches(String.format(TOPOLOGY_MSG, 2, nodeId8, 2, 0)).build();

            lsnr2 = LogListener.matches(s -> s.contains(String.format(NUMBER_SRV_NODES, 2))
                && s.contains(String.format(CLIENT_NODES_COUNT, 0))).build();

            testLog.registerAllListeners(lsnr, lsnr2);

            Ignite srv1 = startGrid("server1");

            waitForDiscovery(srv, srv1);
        }
        finally {
            stopAllGrids();
        }

        checkLogMessages(aliveNodesLsnr, lsnr, lsnr2);
    }

    /**
     * Checks topology snapshot message with server and client nodes in INFO log level.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerAndClientLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.INFO);

        doServerAndClientTest();
    }

    /**
     * Checks topology snapshot message with server and client nodes in DEBUG log level.
     *
     * @throws Exception If failed.
     */
    @Test
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

        LogListener aliveNodesLsnr = LogListener.matches(ptrn).times(16).build();

        testLog.registerListener(aliveNodesLsnr);

        LogListener lsnr;
        LogListener lsnr2;

        try {
            Ignite srv = startGrid("server");

            nodeId8 = U.id8(srv.cluster().localNode().id());

            lsnr = LogListener.matches(String.format(TOPOLOGY_MSG, 4, nodeId8, 2, 2)).build();

            lsnr2 = LogListener.matches(s -> s.contains(String.format(NUMBER_SRV_NODES, 2))
                && s.contains(String.format(CLIENT_NODES_COUNT, 2))).build();

            testLog.registerAllListeners(lsnr, lsnr2);

            Ignite srv1 = startGrid("server1");
            Ignite client1 = startClientGrid("first client");
            Ignite client2 = startClientGrid("second client");

            waitForDiscovery(srv, srv1, client1, client2);
        }
        finally {
            stopAllGrids();
        }

        checkLogMessages(aliveNodesLsnr, lsnr, lsnr2);
    }

    /**
     * Checks topology snapshot message with server, client and force client nodes in INFO log level.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testForceServerAndClientLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.INFO);

        doForceServerAndClientTest();
    }

    /**
     * Checks topology snapshot message with server, client and force client nodes in DEBUG log level.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testForceServerAndClientDebugLogs() throws Exception {
        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        doForceServerAndClientTest();
    }

    /** */
    @Test
    public void checkMessageOnCoordinatorChangeTest() throws Exception {
        startGrid(1);

        startClientGrid("client");

        Ignite server2 = startGrid(2);

        MockLogger log = new MockLogger();

        log.setLevel(Level.INFO);

        setLogger(log, server2);

        stopGrid(1);

        stopGrid("client");

        assertFalse(log.logs().stream().anyMatch(msg -> msg.contains("Coordinator changed") && msg.contains("isClient=true")));
    }

    /**
     * @throws Exception If failed.
     */
    private void doForceServerAndClientTest() throws Exception {
        String nodeId8;

        testLog = new ListeningTestLogger(log);

        Pattern ptrn = Pattern.compile(String.format(ALIVE_NODES_MSG, 1, 4));

        LogListener aliveNodesLsnr = LogListener.matches(ptrn).times(25).build();

        testLog.registerListener(aliveNodesLsnr);

        LogListener lsnr;
        LogListener lsnr2;

        try {
            Ignite srv = startGrid("server");

            nodeId8 = U.id8(srv.cluster().localNode().id());

            lsnr = LogListener.matches(String.format(TOPOLOGY_MSG, 5, nodeId8, 2, 3)).build();

            lsnr2 = LogListener.matches(s -> s.contains(String.format(NUMBER_SRV_NODES, 2))
                && s.contains(String.format(CLIENT_NODES_COUNT, 3))).build();

            testLog.registerAllListeners(lsnr, lsnr2);

            Ignite srv1 = startGrid("server1");
            Ignite client1 = startClientGrid("first client");
            Ignite client2 = startClientGrid("second client");
            Ignite client3 = startClientGrid("third client");

            waitForDiscovery(srv, srv1, client1, client2, client3);
        }
        finally {
            stopAllGrids();
        }

        checkLogMessages(aliveNodesLsnr, lsnr, lsnr2);
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
     * Check nodes details in log.
     *
     * @param aliveNodesLsnr log listener.
     */
    private void checkLogMessages(LogListener aliveNodesLsnr, LogListener lsnr, LogListener lsnr2) {
        assertTrue(aliveNodesLsnr.check());
        assertTrue(lsnr.check());
        assertFalse(lsnr2.check());
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
                msg.contains(TOPOLOGY_MSG)
                    || msg.contains(NUMBER_SRV_NODES)
                    || msg.contains(CLIENT_NODES_COUNT)))
                logs.add(msg);

            super.debug(msg);
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            if ((msg != null && !msg.isEmpty()) && (
                msg.contains(TOPOLOGY_MSG)
                || msg.contains(NUMBER_SRV_NODES)
                || msg.contains(CLIENT_NODES_COUNT))
                || msg.contains(COORDINATOR_CHANGED))
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
