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

import java.util.UUID;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests change coordinator event logging.
 */
public class GridDiscoveryManagerChangeCoordinatorTest extends GridCommonAbstractTest {
    /** Listener log messages. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** */
    private static final String CRD_CHANGE_MSG  = "Coordinator changed";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        listeningLog.clearListeners();

        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super
            .getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setClientMode("client".equals(igniteInstanceName))
            .setDaemon("daemon".equals(igniteInstanceName));
    }

    /**
     * Checks that there are no messages like "coordinator changed" after client left topology.
     *
     * @throws Exception If failed.
     */
    public void testChangeCoordinatorLogging() throws Exception {
        //Start 2 server nodes
        IgniteEx srv1 = (IgniteEx)startGrid("server1");
        IgniteEx client = (IgniteEx)startGrid("client");
        startGrid("server2");

        srv1.cluster().active();

        stopGrid("server1");
        startGrid("server1");

        LogListener lsnr = LogListener.matches(CRD_CHANGE_MSG).build();

        listeningLog.registerListener(lsnr);

        stopGrid("client");

        assertFalse(lsnr.check());
    }

    /**
     * Checks change coordinator event logging with daemon node in topology.
     *
     * @throws Exception If failed.
     */
    public void testChangeCoordinatorLoggingDaemonNodeInTopology() throws Exception {
        // Start 2 server nodes
        IgniteEx srv1 = (IgniteEx)startGrid("server1");
        IgniteEx srv2 = (IgniteEx)startGrid("server2");

        srv1.cluster().active();

        // Check coordinator = server1
        assertTrue(((TcpDiscoverySpi) srv1.context().config().getDiscoverySpi()).isLocalNodeCoordinator());

        // Add a client node, daemon node and 3rd server node, which is not in the baseline
        startGrid("client");
        IgniteEx daemon = (IgniteEx)startGrid("daemon");
        IgniteEx srv3 = (IgniteEx)startGrid("server3");

        LogListener lsnr = LogListener.matches(CRD_CHANGE_MSG).build();

        listeningLog.registerListener(lsnr);

        stopGrid("server1");

        UUID crdUUID = ((TcpDiscoverySpi)srv3.context().config().getDiscoverySpi()).getCoordinator();

        // Coordinator changed server1 -> server2
        assertEquals(srv2.localNode().id(), crdUUID);

        assertTrue(lsnr.check());

        lsnr.reset();

        srv1 = (IgniteEx)startGrid("server1");

        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();

        // Server2 is still the coordinator
        assertEquals(srv2.localNode().id(), crdUUID);

        assertFalse(lsnr.check());

        lsnr.reset();

        stopGrid("server2");

        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();

        // Coordinator changed server2 -> daemon
        assertEquals(daemon.localNode().id(), crdUUID);

        assertTrue(lsnr.check());

        lsnr.reset();

        stopGrid("client");

        crdUUID = ((TcpDiscoverySpi) srv3.context().config().getDiscoverySpi()).getCoordinator();

        // daemon is still coordinator
        assertEquals(daemon.localNode().id(), crdUUID);

        assertFalse(lsnr.check());

        lsnr.reset();

        stopGrid("server3");

        crdUUID = ((TcpDiscoverySpi) srv1.context().config().getDiscoverySpi()).getCoordinator();

        // daemon is still coordinator
        assertEquals(daemon.localNode().id(), crdUUID);

        assertFalse(lsnr.check());
    }
}
