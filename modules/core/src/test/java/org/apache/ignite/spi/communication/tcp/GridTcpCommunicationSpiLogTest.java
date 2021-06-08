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
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioRecoveryDescriptor;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Class for {@link TcpCommunicationSpi} logging tests.
 */
@Ignore("https://issues.apache.org/jira/browse/IGNITE-13723")
public class GridTcpCommunicationSpiLogTest extends GridCommonAbstractTest {
    /** Listener log messages. */
    private static ListeningTestLogger srvTestLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals(getTestIgniteInstanceName(0)))
            cfg.setGridLogger(srvTestLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        setRootLoggerDebugLevel();

        srvTestLog = new ListeningTestLogger(true, log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        srvTestLog.clearListeners();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientConnectDebugLogMessage() throws Exception {
        LogListener logLsnr0 = LogListener.matches("The node client is going to create a connection")
            .atLeast(1)
            .atMost(1)
            .build();

        LogListener logLsnr1 = LogListener.matches("The node client is going to reserve a connection")
            .atLeast(1)
            .build();

        srvTestLog.registerListener(logLsnr0);
        srvTestLog.registerListener(logLsnr1);

        Ignite srv = startGrid(0);
        Ignite client = startClientGrid(1);

        U.sleep(1000);

        assertTrue(logLsnr0.check());
        assertTrue(logLsnr1.check());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientDisconnectDebugLogMessage() throws Exception {
        LogListener logLsnr0 = LogListener.matches("The node was disconnected")
            .atLeast(1)
            .atMost(1)
            .build();

        srvTestLog.registerListener(logLsnr0);

        Ignite srv = startGrid(0);
        Ignite client = startGrid(1);

        client.close();

        U.sleep(1000);

        assertTrue(logLsnr0.check());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCloseClientConnectionsDebugLogMessage() throws Exception {
        LogListener logLsnr0 = LogListener.matches("The node client connections were closed")
            .atLeast(1)
            .atMost(1)
            .build();

        srvTestLog.registerListener(logLsnr0);

        IgniteEx srv = startGrid(0);
        Ignite client = startGrid(1);

        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)srv.context().config().getCommunicationSpi();

        ConnectionClientPool connPool = U.field(commSpi, "clientPool");

        connPool.forceCloseConnection(client.cluster().localNode().id());

        U.sleep(1000);

        assertTrue(logLsnr0.check());
    }

    /**
     * Test covers use case when communication timeout happens on client node,
     * but connection still alive on the server side. On the next get operation,
     * client node tries to establish connection to the server and receives "already connected" error.
     * After that server node executes {@link TcpCommunicationSpi#closeConnections(UUID)}}
     * and closes existing connection on the server side. As result client node successfully
     * establishes connection on the next retry. Test expects that "The session change request was offered"
     * and "The node client was replaced" messages would be printed in the log on the first attempt.
     * And after that "The session request will be processed" and "The client was removed"
     * would be printed during processing.
     * @throws Exception If failed.
     */
    @Test
    public void testClientHalfOpenedConnectionDebugLogMessage() throws Exception {
        LogListener logLsnr0 = LogListener.matches("The session change request was offered [req=NioOperationFuture [op=CLOSE")
            .atLeast(1)
            .atMost(1)
            .build();

        LogListener logLsnr1 = LogListener.matches("The session request will be processed [req=NioOperationFuture [op=CLOSE")
            .atLeast(1)
            .atMost(1)
            .build();

        LogListener logLsnr2 = LogListener.matches("The client was removed")
            .atLeast(1)
            .build();

        LogListener logLsnr3 = LogListener.matches("The node client was replaced")
            .atLeast(1)
            .atMost(1)
            .build();

        srvTestLog.registerListener(logLsnr0);
        srvTestLog.registerListener(logLsnr1);
        srvTestLog.registerListener(logLsnr2);
        srvTestLog.registerListener(logLsnr3);

        Ignite srv = startGrid(0);
        Ignite client = startClientGrid(1);

        IgniteCache<Object, Object> cache = client.getOrCreateCache("TEST");

        cache.put(1, "1");

        TcpCommunicationSpi clientSpi = (TcpCommunicationSpi) ((IgniteEx)client).context().config().getCommunicationSpi();

        ConcurrentMap<UUID, GridCommunicationClient[]> clients = GridTestUtils.getFieldValue(clientSpi, "clientPool", "clients");
        ConcurrentMap<?, GridNioRecoveryDescriptor> recoveryDescs =
            GridTestUtils.getFieldValue(clientSpi, "nioSrvWrapper", "recoveryDescs");
        ConcurrentMap<?, GridNioRecoveryDescriptor> outRecDescs = GridTestUtils.getFieldValue(clientSpi, "nioSrvWrapper", "outRecDescs");
        ConcurrentMap<?, GridNioRecoveryDescriptor> inRecDescs = GridTestUtils.getFieldValue(clientSpi, "nioSrvWrapper", "inRecDescs");
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
        GridCommunicationClient[] clients0 = clients.remove(srv.cluster().localNode().id());

        for (GridCommunicationClient commClient : clients0)
            lsnr.onDisconnected(((GridTcpNioCommunicationClient)commClient).session(), new IOException("Test exception"));

        cache.get(1);

        U.sleep(1000);

        assertTrue(logLsnr0.check());
        assertTrue(logLsnr1.check());
        assertTrue(logLsnr2.check());
        assertTrue(logLsnr3.check());
    }
}
