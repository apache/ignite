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

package org.apache.ignite.spi.communication.tcp;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.noop;

/**
 * Tests case when connection is closed only for one side, when other is not notified.
 */
public class TcpCommunicationSpiHalfOpenedConnectionTest extends GridCommonAbstractTest {
    /** Paired connections. */
    private boolean pairedConnections;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setUsePairedConnections(pairedConnections);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnect() throws Exception {
        pairedConnections = false;

        checkReconnect(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectPaired() throws Exception {
        pairedConnections = true;

        checkReconnect(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReverseReconnect() throws Exception {
        pairedConnections = false;

        checkReconnect(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReverseReconnectPaired() throws Exception {
        pairedConnections = true;

        checkReconnect(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkReconnect(boolean reverseReconnect) throws Exception {
        Ignite srv = startGrid(0);
        Ignite client = startClientGrid(1);

        UUID srvNodeId = srv.cluster().localNode().id();
        UUID clientNodeId = client.cluster().localNode().id();

        System.out.println(">> Server ID: " + srvNodeId);
        System.out.println(">> Client ID: " + clientNodeId);

        ClusterGroup srvGrp = client.cluster().forNodeId(srvNodeId);
        ClusterGroup clientGrp = srv.cluster().forNodeId(clientNodeId);

        System.out.println(">> Send job");

        // Establish connection
        client.compute(srvGrp).run(noop());

        if (reverseReconnect)
            reconnect(srv, client, clientGrp);
        else
            reconnect(client, srv, srvGrp);
    }

    /**
     * Reconnects the {@code srcNode} to the {@code targetNode}.
     *
     * @param srcNode Source node.
     * @param targetNode Target node.
     * @param targetGrp Target cluster group.
     */
    private void reconnect(Ignite srcNode, Ignite targetNode, ClusterGroup targetGrp) {
        CommunicationSpi commSpi = srcNode.configuration().getCommunicationSpi();

        ConcurrentMap<UUID, GridCommunicationClient[]> clients = GridTestUtils.getFieldValue(commSpi, "clientPool", "clients");
        GridMetricManager metricMgr = GridTestUtils.getFieldValue(commSpi, "clientPool", "metricsMgr");
        ConcurrentMap<?, GridNioRecoveryDescriptor> recoveryDescs = GridTestUtils.getFieldValue(commSpi, "nioSrvWrapper", "recoveryDescs");
        ConcurrentMap<?, GridNioRecoveryDescriptor> outRecDescs = GridTestUtils.getFieldValue(commSpi, "nioSrvWrapper", "outRecDescs");
        ConcurrentMap<?, GridNioRecoveryDescriptor> inRecDescs = GridTestUtils.getFieldValue(commSpi, "nioSrvWrapper", "inRecDescs");
        GridNioServerListener<Message> lsnr = GridTestUtils.getFieldValue(commSpi, "nioSrvWrapper", "srvLsnr");

        Iterator<GridNioRecoveryDescriptor> it = F.concat(
            recoveryDescs.values().iterator(),
            outRecDescs.values().iterator(),
            inRecDescs.values().iterator()
        );

        while (it.hasNext()) {
            GridNioRecoveryDescriptor desc = it.next();

            // Need to simulate connection close in GridNioServer as it
            // releases descriptors on disconnect.
            desc.release();
        }

        // Remove client to avoid calling close(), in that case server
        // will close connection too, but we want to keep the server
        // uninformed and force ping old connection.
        GridCommunicationClient[] clients0 = clients.remove(targetNode.cluster().localNode().id());

        if (metricMgr != null) {
            Map<UUID, ?> metrics = GridTestUtils.getFieldValue(commSpi, "clientPool", "metrics");

            assert metrics != null;

            metrics.remove(targetNode.cluster().localNode().id());

            metricMgr.remove(ConnectionClientPool.nodeMetricsRegName(targetNode.cluster().localNode().id()));
        }

        for (GridCommunicationClient commClient : clients0)
            lsnr.onDisconnected(((GridTcpNioCommunicationClient)commClient).session(), new IOException("Test exception"));

        info(">> Removed client");

        // Reestablish connection
        srcNode.compute(targetGrp).run(noop());

        info(">> Sent second job");
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }
}
