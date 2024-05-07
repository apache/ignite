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
import java.util.ArrayList;
import java.util.Collection;
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
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
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
        ListeningTestLogger testLog = new ListeningTestLogger(log);

        String startLogMsg = "Checking connection to node";

        Collection<LogListener> lsnrs = new ArrayList<>();

        lsnrs.add(LogListener.matches(startLogMsg).andMatches("result=success").times(1).build());
        lsnrs.add(LogListener.matches(startLogMsg).andMatches("result=skipped").times(2).build());
        lsnrs.add(LogListener.matches(startLogMsg).andMatches("result=failed").times(1).build());
        lsnrs.add(LogListener.matches("Connection check to previous node done").times(1).build());

        lsnrs.forEach(testLog::registerListener);

        TcpDiscoverySpi spi0 = new TcpDiscoverySpi();

        startGrid(getTestConfigWithSpi(spi0, "ignite-0"));
        startGrid(getTestConfigWithSpi(new TcpDiscoverySpi(), "ignite-1"));

        IgniteConfiguration cfg = getTestConfigWithSpi(new TestCustomEffectiveNodeAddressesSpi(), "ignite-2");
        cfg.setGridLogger(testLog);
        startGrid(cfg);

        spi0.brakeConnection();

        for (LogListener lsnr : lsnrs)
            waitForCondition(lsnr::check, getTestTimeout());

        testLog.clearListeners();
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
        ListeningTestLogger testLog = new ListeningTestLogger(log);

        Collection<LogListener> lsnrs = new ArrayList<>();

        lsnrs.add(LogListener.matches("Checking connection to node").andMatches("result=failed").times(1).build());
        lsnrs.add(LogListener.matches("Connection check to previous node failed").times(1).build());

        lsnrs.forEach(testLog::registerListener);

        TcpDiscoverySpi spi0 = new TcpDiscoverySpi();

        startGrid(getTestConfigWithSpi(spi0, "ignite-0"));

        IgniteConfiguration cfg1 = getTestConfigWithSpi(new TcpDiscoverySpi(), "ignite-1");
        cfg1.setGridLogger(testLog);

        startGrid(cfg1);

        startGrid(getTestConfigWithSpi(new TcpDiscoverySpi(), "ignite-2"));

        spi0.simulateNodeFailure();

        for (LogListener lsnr : lsnrs)
            waitForCondition(lsnr::check, getTestTimeout());

        testLog.clearListeners();
    }


    /**
     * Returns default {@link IgniteConfiguration} with specified ignite instance name and {@link TcpDiscoverySpi}.
     * @param spi {@link TcpDiscoverySpi}
     * @param igniteInstanceName ignite instance name
     * @return {@link IgniteConfiguration}
     * @throws Exception If failed.
     */
    private IgniteConfiguration getTestConfigWithSpi(TcpDiscoverySpi spi, String igniteInstanceName) throws Exception {
        return getConfiguration(igniteInstanceName).setDiscoverySpi(spi);
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