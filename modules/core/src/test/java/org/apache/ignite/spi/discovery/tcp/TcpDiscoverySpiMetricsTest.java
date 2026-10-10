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

package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.DISCO_METRICS;

/** Tests {@link TcpDiscoverySpi} metrics registered in the {@code io.discovery} metric registry. */
public class TcpDiscoverySpiMetricsTest extends GridCommonAbstractTest {
    /** */
    private volatile CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(1_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testSocketReadTimeoutsMetric() throws Exception {
        checkMetric(true);
    }

    /** */
    @Test
    public void testSocketWriteTimeoutsMetric() throws Exception {
        checkMetric(false);
    }

    /** */
    private void checkMetric(boolean read) throws Exception {
        IgniteEx node0;

        if (read)
            node0 = startGrid(0);
        else {
            node0 = startGrid(0, cfg -> {
                BlockingWriteDiscoverySpi spi = new BlockingWriteDiscoverySpi();

                spi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());

                cfg.setDiscoverySpi(spi);
            });
        }

        IgniteEx node1 = startGrid(1);

        TestTcpDiscoverySpi spi1 = (TestTcpDiscoverySpi)node1.configuration().getDiscoverySpi();

        if (read)
            spi1.freeze();
        else
            latch = new CountDownLatch(1);

        IntMetric metric = node0.context().metric().registry(DISCO_METRICS)
            .findMetric(read ? "SocketReadTimeoutsCount" : "SocketWriteTimeoutsCount");

        assertEquals(0, metric.value());

        IgniteInternalFuture<?> pingFut = GridTestUtils.runAsync(() -> node0.cluster().pingNode(node1.cluster().localNode().id()));

        assertTrue(GridTestUtils.waitForCondition(() -> metric.value() >= 1, 15_000));

        if (read)
            spi1.unfreeze();
        else
            latch.countDown();

        pingFut.get(5_000);
    }

    /** */
    private class BlockingWriteDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override protected TcpDiscoveryIoSession openSession(
            Socket sock,
            InetSocketAddress remAddr,
            IgniteSpiOperationTimeoutHelper timeoutHelper
        ) throws IgniteSpiOperationTimeoutException, IgniteCheckedException, IOException {
            super.openSession(sock, remAddr, timeoutHelper);

            return new BlockingIoSession(ignite.context(), sock);
        }
    }

    /** */
    private class BlockingIoSession extends TcpDiscoveryIoSession {
        /** */
        BlockingIoSession(GridKernalContext ctx, Socket sock) {
            super(ctx, sock);
        }

        /** {@inheritDoc} */
        @Override synchronized void writeMessage(TcpDiscoveryAbstractMessage msg) throws IgniteCheckedException, IOException {
            if (msg instanceof TcpDiscoveryPingRequest && latch != null)
                U.awaitQuiet(latch);

            super.writeMessage(msg);
        }
    }
}
