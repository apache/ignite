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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashSet;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Class for {@link TcpDiscoverySpi} logging tests.
 */
public class TcpDiscoverySpiLogTest extends GridCommonAbstractTest {
    /**
     * Listener log messages
     */
    private static ListeningTestLogger testLog;

    /** */
    private static final ThreadLocal<TcpDiscoverySpi> nodeSpi = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = nodeSpi.get();

        if (spi == null)
            spi = new TcpDiscoverySpi();
        else
            nodeSpi.set(null);

        cfg.setDiscoverySpi(spi);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        testLog.clearListeners();

        super.afterTest();
    }

    /**
     * This test checks all the possible logs from socket connection after coordinator quiet close.
     * <p>
     * There are 3 nodes used in the test. After the coordinator closure, the 3rd node will try to check the connection
     * to the 2nd one. In other words the 3rd will try to connect to all effective addresses of the 2nd through socket,
     * which can be accessed with 3's Discovery SPI.
     * <p>
     * For us to get multiple effective addresses for connection, we override
     * {@link TcpDiscoverySpi#getEffectiveNodeAddresses(TcpDiscoveryNode)} method in
     * {@link TestCustomEffectiveNodeAddressesSpi} to return custom collection of {@link InetSocketAddress}.
     * We only need the custom collection once, when 3rd node is trying to probe the 2nd one. As the method is used in
     * different other scenarios throughout the test, we restrain the method to deviate from expected behaviour only
     * for the test case, that is when {@link TcpDiscoveryNode#discoveryPort()} is 47501.
     * <p>
     * By manipulating the collection order we can check the failing, successful and ignored connection logs at once.
     *
     * @throws Exception If failed.
     * @see TcpDiscoverySpi#getEffectiveNodeAddresses(TcpDiscoveryNode)
     * @see TestCustomEffectiveNodeAddressesSpi
     */
    @Test
    public void testMultipleSocketConnectionLogMessage() throws Exception {
        LogListener lsnr0 = LogListener.matches(s ->
                s.contains("Connection check to node") && s.contains("result=success"))
            .build();

        LogListener lsnr1 = LogListener.matches(s ->
                s.contains("Connection check to node") && s.contains("result=skipped"))
            .build();

        LogListener lsnr2 = LogListener.matches(s ->
                s.contains("Connection check to node") && s.contains("result=failed"))
            .build();

        LogListener lsnr3 = LogListener.matches(s ->
                s.contains("Connection check to previous node done"))
            .build();

        testLog.registerListener(lsnr0);
        testLog.registerListener(lsnr1);
        testLog.registerListener(lsnr2);
        testLog.registerListener(lsnr3);

        try {
            startGrid(0);
            startGrid(1);

            TestCustomEffectiveNodeAddressesSpi spi = new TestCustomEffectiveNodeAddressesSpi();
            nodeSpi.set(spi);

            startGrid(2);

            ((TcpDiscoverySpi)ignite(0).configuration().getDiscoverySpi()).brakeConnection();

            assertTrue(waitForCondition(lsnr0::check, 10_000L));
            assertTrue(waitForCondition(lsnr1::check, 10_000L));
            assertTrue(waitForCondition(lsnr2::check, 10_000L));
            assertTrue(waitForCondition(lsnr3::check, 10_000L));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * This test uses quiet closure of given {@link Socket} ignoring possible checked exception, which triggers
     * the previous node to check the connection to the following surviving one.
     *
     * @throws Exception If failed.
     * @see TcpDiscoverySpi#brakeConnection()
     */
    @Test
    public void testCheckBrakeConnectionSuccessSocketConnectionLogMessage() throws Exception {
        LogListener lsnr0 = LogListener.matches(s ->
                (s.contains("Connection check to node") && s.contains("result=success")))
            .build();

        LogListener lsnr1 = LogListener.matches(s ->
                s.contains("Connection check to previous node done"))
            .build();

        testLog.registerListener(lsnr0);
        testLog.registerListener(lsnr1);

        try {
            startGridsMultiThreaded(3);

            ((TcpDiscoverySpi)ignite(0).configuration().getDiscoverySpi()).brakeConnection();

            assertTrue(waitForCondition(lsnr0::check, 10_000L));
            assertTrue(waitForCondition(lsnr1::check, 10_000L));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * This test uses node failure by stopping service threads, which makes the node unresponsive and results in
     * failing connection to the server.
     *
     * @throws Exception If failed.
     * @see TcpDiscoverySpi#simulateNodeFailure()
     */
    @Test
    public void testCheckNodeFailureSocketConnectionLogMessage() throws Exception {
        LogListener lsnr0 = LogListener.matches(s ->
                s.contains("Connection check to node") && s.contains("result=failed"))
            .build();

        LogListener lsnr1 = LogListener.matches(s ->
                s.contains("Connection check to previous node failed"))
            .build();

        testLog.registerListener(lsnr0);
        testLog.registerListener(lsnr1);

        try {
            startGridsMultiThreaded(3);
            ((TcpDiscoverySpi)ignite(0).configuration().getDiscoverySpi()).simulateNodeFailure();

            assertTrue(waitForCondition(lsnr0::check, 10_000L));
            assertTrue(waitForCondition(lsnr1::check, 10_000L));
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    private static class TestCustomEffectiveNodeAddressesSpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        private static final LinkedHashSet<InetSocketAddress> customAddrs = new LinkedHashSet<>();

        static {
            customAddrs.add(new InetSocketAddress("127.0.0.1", 47505));
            customAddrs.add(new InetSocketAddress("127.0.0.1", 47501));
            customAddrs.add(new InetSocketAddress("127.0.0.1", 47503));
            customAddrs.add(new InetSocketAddress("127.0.0.1", 47504));
        }

        /** {@inheritDoc} */
        @Override LinkedHashSet<InetSocketAddress> getEffectiveNodeAddresses(TcpDiscoveryNode node) {
            if (node.discoveryPort() == 47501)
                return customAddrs;

            return super.getEffectiveNodeAddresses(node);
        }
    }
}
