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
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.nio.ssl.GridSslMeta;
import org.apache.ignite.spi.communication.tcp.internal.TcpHandshakeExecutor;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class TcpCommunicationHandshakeTimeoutTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureDetectionTimeout(1000);
        cfg.setSystemWorkerBlockedTimeout(3000);

        TcpCommunicationSpi spi = new TcpCommunicationSpi();
        spi.setIdleConnectionTimeout(100);

        cfg.setCommunicationSpi(spi);

        StopNodeFailureHandler hnd = new StopNodeFailureHandler();
        hnd.setIgnoredFailureTypes(new HashSet<>());
        cfg.setFailureHandler(hnd);

        return cfg;
    }

    /**
     * 1. Cluster from three nodes.
     * 2. Waiting when communication connection goes idle.
     * 4. Configure the delay during the communication connection handshake.
     * 5. Force establishing of new connection from node2 to node1 from timeout object processor thread
     * (it use compute in this test).
     * 6. Expected:  The frozen attempt of handshake would be successfully handled and a new connection
     * would be established.
     *
     * @throws Exception If fail.
     */
    @Test
    public void testSocketForcedClosedBecauseSlowReadFromSocket() throws Exception {
        //given: Two ordinary nodes.
        startGrid(0);
        IgniteEx g1 = startGrid(1);

        //and: One more node which communication connection can be delayed by demand.
        AtomicBoolean delayHandshakeUntilSocketClosed = new AtomicBoolean();
        IgniteEx g2 = startGrid(2, new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof TcpHandshakeExecutor) {
                    TcpHandshakeExecutor gridNioServer = (TcpHandshakeExecutor)instance;

                    return (T)new DelaydTcpHandshakeExecutor(gridNioServer, delayHandshakeUntilSocketClosed);
                }

                return instance;
            }
        });

        awaitPartitionMapExchange();

        AtomicBoolean result = new AtomicBoolean(false);

        //Wait for connections go idle.
        doSleep(1000);

        //when: Initiate communication connection from timeout object processor thread.
        g2.context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(0) {

            @Override public void onTimeout() {
                delayHandshakeUntilSocketClosed.set(true);

                g2.compute(g2.cluster().forNodes(Arrays.asList(g1.localNode()))).withNoFailover().call(() -> true);

                result.set(true);
            }
        });

        //then: Despite the first attempt of handshake would be frozen the compute should be handled well eventually.
        assertTrue("Compute should be successfully handled.", waitForCondition(result::get, 20_000));
    }

    /** TcpHandshakeExecutor which can be asked to delay until socket is closed. */
    static class DelaydTcpHandshakeExecutor extends TcpHandshakeExecutor {
        /**
         *
         */
        private final TcpHandshakeExecutor delegate;

        /** {@code true} if handshake should be delayd until socket closed. */
        private final AtomicBoolean needToDelayd;

        /**
         *
         */
        public DelaydTcpHandshakeExecutor(TcpHandshakeExecutor delegate, AtomicBoolean needToDelayd) {
            super(log, null, false);

            this.delegate = delegate;
            this.needToDelayd = needToDelayd;
        }

        /** {@inheritDoc} */
        @Override public long tcpHandshake(SocketChannel ch, UUID rmtNodeId, GridSslMeta sslMeta,
            HandshakeMessage msg) throws IgniteCheckedException, IOException {
            if (needToDelayd.get()) {
                needToDelayd.set(false);

                while (ch.isOpen() && !Thread.currentThread().isInterrupted())
                    LockSupport.parkNanos(10_000_000);
            }

            return delegate.tcpHandshake(ch, rmtNodeId, sslMeta, msg);
        }
    }
}
