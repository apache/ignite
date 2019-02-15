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

package org.apache.ignite.spi.communication.tcp;

import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests case when connection is closed only for one side, when other is not notified.
 */
@RunWith(JUnit4.class)
public class TcpCommunicationSpiHalfOpenedConnectionTest extends GridCommonAbstractTest {
    /** Client spi. */
    private TcpCommunicationSpi clientSpi;

    /** Paired connections. */
    private boolean pairedConnections;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.contains("client")) {
            cfg.setClientMode(true);

            clientSpi = (TcpCommunicationSpi)cfg.getCommunicationSpi();
        }

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

        checkReconnect();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectPaired() throws Exception {
        pairedConnections = true;

        checkReconnect();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkReconnect() throws Exception {
        Ignite srv = startGrid("server");
        Ignite client = startGrid("client");

        UUID nodeId = srv.cluster().localNode().id();

        System.out.println(">> Server ID: " + nodeId);

        ClusterGroup srvGrp = client.cluster().forNodeId(nodeId);

        System.out.println(">> Send job");

        // Establish connection
        client.compute(srvGrp).run(F.noop());

        ConcurrentMap<UUID, GridCommunicationClient[]> clients = U.field(clientSpi, "clients");
        ConcurrentMap<?, GridNioRecoveryDescriptor> recoveryDescs = U.field(clientSpi, "recoveryDescs");
        ConcurrentMap<?, GridNioRecoveryDescriptor> outRecDescs = U.field(clientSpi, "outRecDescs");
        ConcurrentMap<?, GridNioRecoveryDescriptor> inRecDescs = U.field(clientSpi, "inRecDescs");
        GridNioServerListener<Message> lsnr = U.field(clientSpi, "srvLsnr");

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
        GridCommunicationClient[] clients0 = clients.remove(nodeId);

        for (GridCommunicationClient commClient : clients0)
            lsnr.onDisconnected(((GridTcpNioCommunicationClient)commClient).session(), new IOException("Test exception"));

        info(">> Removed client");

        // Reestablish connection
        client.compute(srvGrp).run(F.noop());

        info(">> Sent second job");
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }
}
