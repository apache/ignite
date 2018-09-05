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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.IgniteExceptionRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Testing {@link TcpCommunicationSpi} under big cluster conditions (long DiscoverySpi delivery)
 */
public class IgniteTcpCommunicationBigClusterTest extends GridCommonAbstractTest {
    /** */
    private static final long COMMUNICATION_TIMEOUT = 1000;

    /** */
    private static final long DISCOVERY_MESSAGE_DELAY = 500;

    /** */
    private static final int CLUSTER_SIZE = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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
        AtomicInteger idx = new AtomicInteger();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    IgniteEx ignite = startGrid(idx.getAndIncrement());

                    while (ignite.cluster().forServers().nodes().size() < CLUSTER_SIZE) {
                        ignite.compute().broadcast(new IgniteRunnable() {
                            @Override public void run() {
                                // No-op.
                            }
                        });

                        U.sleep(10);
                    }
                }
                catch (Exception e) {
                    error("Test failed.", e);
                }
            }
        }, CLUSTER_SIZE);

        fut.get();

        final IgniteExceptionRegistry exReg = IgniteExceptionRegistry.get();

        for (IgniteExceptionRegistry.ExceptionInfo info : exReg.getErrors(0L)) {
            if (info.error() instanceof IgniteCheckedException
                && "HandshakeTimeoutException".equals(info.error().getClass().getSimpleName()))
                throw new IgniteCheckedException("Test failed because handshake hangs.", info.error());
        }
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
                    e.printStackTrace();
                }
            }

            return super.ensured(msg);
        }
    }
}
