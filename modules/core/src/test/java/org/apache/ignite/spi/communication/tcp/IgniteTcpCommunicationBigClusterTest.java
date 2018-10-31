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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;

/**
 * Testing {@link TcpCommunicationSpi} under big cluster conditions (long DiscoverySpi delivery)
 */
public class IgniteTcpCommunicationBigClusterTest extends GridCommonAbstractTest {
    /** */
    private static final long COMMUNICATION_TIMEOUT = 2000;

    /** */
    private static final long DISCOVERY_MESSAGE_DELAY = 300;

    /** */
    private static final int CLUSTER_SIZE = 8;

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger();

    static {
        GridIoMessageFactory.registerCustom(GridTestMessage.DIRECT_TYPE, (CO<Message>)GridTestMessage::new);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (testLog != null)
            cfg.setGridLogger(testLog);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discoSpi = new SlowTcpDiscoverySpi();

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setConnectTimeout(COMMUNICATION_TIMEOUT);
        commSpi.setMaxConnectTimeout(2 * COMMUNICATION_TIMEOUT);
        commSpi.setReconnectCount(1);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testHandshakeNoHangOnNodeJoining() throws Exception {
        LogListener lsnr = LogListener.matches("Handshake timedout").times(0).build();

        testLog.registerListener(lsnr);

        AtomicInteger idx = new AtomicInteger();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    IgniteEx ignite = startGrid(idx.getAndIncrement());

                    GridIoManager ioMgr = ignite.context().io();

                    while (ignite.cluster().forServers().nodes().size() < CLUSTER_SIZE) {
                        Collection<ClusterNode> nodes = ignite.context().discovery().aliveServerNodes();

                        for (ClusterNode node : nodes)
                            ioMgr.sendToCustomTopic(node, "Test topic", new GridTestMessage(), PUBLIC_POOL);

                        U.sleep(100);
                    }
                }
                catch (Exception e) {
                    error("Unexpected exception: ", e);

                    fail("Unexpected exception (see details above): " + e.getMessage());
                }
            }
        }, CLUSTER_SIZE);

        fut.get();

        assertTrue("Handshake timeout has happened.", lsnr.check());
    }

    /** */
    private static class SlowTcpDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected boolean ensured(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                try {
                    U.sleep(DISCOVERY_MESSAGE_DELAY);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteSpiException("Thread has been interrupted.", e);
                }
            }

            return super.ensured(msg);
        }
    }
}
