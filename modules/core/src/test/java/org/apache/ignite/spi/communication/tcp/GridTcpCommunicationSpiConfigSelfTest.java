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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TCP_COMM_SET_ATTR_HOST_NAMES;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.util.IgniteUtils.spiAttribute;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_HOST_NAMES;
import static org.apache.ignite.testframework.GridTestUtils.getFreeCommPort;

/**
 * TCP communication SPI config test.
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
public class GridTcpCommunicationSpiConfigSelfTest extends GridSpiAbstractConfigTest<TcpCommunicationSpi> {
    /**
     * Set value to {@link IgniteConfiguration#setLocalHost} after
     * {@link GridAbstractTest#optimize}.
     */
    private String locHost = "0.0.0.0";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration optimize(IgniteConfiguration cfg) throws IgniteCheckedException {
        return super.optimize(cfg).setLocalHost(locHost);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPort", 1023);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPort", 65636);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPortRange", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "idleConnectionTimeout", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketReceiveBuffer", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketSendBuffer", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "messageQueueLimit", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "reconnectCount", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "selectorsCount", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "maxConnectTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketWriteTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "ackSendThreshold", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "ackSendThreshold", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "unacknowledgedMessagesBufferSize", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", Integer.MAX_VALUE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalPortRange() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setLocalPortRange(0);
        commSpi.setLocalPort(getFreeCommPort());

        cfg.setCommunicationSpi(commSpi);

        startGrid(cfg);
    }

    /**
     * Test checks that attribute {@link TcpCommunicationSpi#ATTR_HOST_NAMES}
     * is empty only if IP(don't wildcard and loopback) is set to {@link IgniteConfiguration#setLocalHost}
     * and property {@link IgniteSystemProperties#IGNITE_TCP_COMM_SET_ATTR_HOST_NAMES} == {@code false}
     * (default value).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEmptyHostNameAttrByDefault() throws Exception {
        assertFalse(getBoolean(IGNITE_TCP_COMM_SET_ATTR_HOST_NAMES, false));

        IgniteBiTuple<Collection<String>, Collection<String>> addrs = U.resolveLocalAddresses(
            new InetSocketAddress(0).getAddress()
        );

        String host = addrs.get2().iterator().next();

        String ip = null;

        for (String addr : addrs.get1()) {
            InetAddress inetAddr = U.resolveLocalHost(addr);

            if (!inetAddr.isLoopbackAddress() && !inetAddr.isAnyLocalAddress())
                ip = addr;
        }

        assertNotNull("addrs=" + addrs, ip);

        log.info("Testing ip=" + ip + " host=" + host);

        int nodeIdx = 0;

        locHost = ip;
        checkHostNamesAttr(startGrid(nodeIdx++), false, true);

        locHost = host;
        checkHostNamesAttr(startGrid(nodeIdx++), false, false);

        locHost = null;
        checkHostNamesAttr(startGrid(nodeIdx++), true, false);

        locHost = "0.0.0.0";
        checkHostNamesAttr(startGrid(nodeIdx++), false, false);

        stopAllGrids();

        locHost = "127.0.0.1";
        checkHostNamesAttr(startGrid(nodeIdx++), false, true);
    }

    /**
     * Test checks that attribute {@link TcpCommunicationSpi#ATTR_HOST_NAMES} is not empty.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_TCP_COMM_SET_ATTR_HOST_NAMES, value = "true")
    public void testNotEmptyHostNameAttr() throws Exception {
        InetSocketAddress inetSockAddr = new InetSocketAddress(0);

        String ip = inetSockAddr.getHostName();
        String host = U.resolveLocalAddresses(inetSockAddr.getAddress()).get2().iterator().next();

        log.info("Testing ip=" + ip + " host=" + host);

        int nodeIdx = 0;

        locHost = ip;
        checkHostNamesAttr(startGrid(nodeIdx++), false, false);

        locHost = host;
        checkHostNamesAttr(startGrid(nodeIdx++), false, false);

        locHost = null;
        checkHostNamesAttr(startGrid(nodeIdx++), true, false);
    }

    /**
     * Checking local host on node.
     *
     * @param node Node.
     * @param emptyLocHost Check whether {@link
     *      IgniteConfiguration#getLocalHost} is empty.
     * @param emptyHostNamesAttr Check whether {@link
     *      TcpCommunicationSpi#ATTR_HOST_NAMES} attribute is empty.
     */
    private void checkHostNamesAttr(IgniteEx node, boolean emptyLocHost, boolean emptyHostNamesAttr) {
        requireNonNull(node);

        IgniteConfiguration cfg = node.configuration();
        assertEquals(emptyLocHost, isNull(cfg.getLocalHost()));

        TcpCommunicationSpi spi = (TcpCommunicationSpi)cfg.getCommunicationSpi();
        assertEquals(
            emptyHostNamesAttr,
            ((Collection<String>)node.localNode().attribute(spiAttribute(spi, ATTR_HOST_NAMES))).isEmpty()
        );
    }
}
