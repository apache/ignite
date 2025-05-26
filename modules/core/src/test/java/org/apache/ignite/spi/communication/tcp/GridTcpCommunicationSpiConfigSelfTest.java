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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TCP_COMM_SET_ATTR_HOST_NAMES;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
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

    /** */
    private final Collection<IgniteTestResources> resourcesToClean = new ArrayList<>();

    /** */
    private final Collection<CommunicationSpi> spisToStop = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        for (IgniteTestResources itr : resourcesToClean) {
            itr.stopThreads();
        }

        for (CommunicationSpi commSpi : spisToStop) {
            commSpi.onContextDestroyed();

            commSpi.setListener(null);

            commSpi.spiStop();
        }

        resourcesToClean.clear();
        spisToStop.clear();

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
     * Verifies that TcpCommunicationSpi starts messaging protocol only when fully initialized.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-12982">IGNITE-12982</a>
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK", value = "true")
    public void testSendToNonInitializedTcpCommSpi() throws Exception {
        ListeningTestLogger listeningLog = new ListeningTestLogger(log);
        LogListener npeLsnr = LogListener.matches("NullPointerException")
            .andMatches("InboundConnectionHandler.onMessageSent").build();

        listeningLog.registerListener(npeLsnr);

        GridTestNode sendingNode = new GridTestNode();
        sendingNode.order(0);
        GridSpiTestContext sendingCtx = initSpiContext();

        TcpCommunicationSpi sendingSpi = initializeSpi(sendingCtx, sendingNode, listeningLog, false);
        spisToStop.add(sendingSpi);

        sendingSpi.onContextInitialized(sendingCtx);

        GridTestNode receiverNode = new GridTestNode();
        receiverNode.order(1);
        GridSpiTestContext receiverCtx = initSpiContext();

        /*
         * This is a dirty hack to intervene into TcpCommunicationSpi#onContextInitialized0 method
         * and add a delay before injecting metrics listener into its clients (like InboundConnectionHandler).
         * The purpose of the delay is to make race between sending a message and initializing TcpCommSpi visible.
         *
         * This solution heavily depends on current code structure of onContextInitialized0 method.
         * If any modifications are made to it, this logic could break and the test starts failing.
         *
         * In that case try to rewrite the test or delete it as this race is really hard to test.
         */
        receiverCtx.metricsRegistryProducer((name) -> {
            try {
                Thread.sleep(100);
            }
            catch (Exception ignored) {
                // No-op.
            }

            return new MetricRegistryImpl(name, null, null, new NullLogger());
        });

        TcpCommunicationSpi receiverSpi = initializeSpi(receiverCtx, receiverNode, listeningLog, true);
        spisToStop.add(receiverSpi);

        receiverCtx.remoteNodes().add(sendingNode);
        sendingCtx.remoteNodes().add(receiverNode);

        IgniteInternalFuture sendFut = GridTestUtils.runAsync(() -> {
            Message msg = new GridTestMessage(sendingNode.id(), 0, 0);

            sendingSpi.sendMessage(receiverNode, msg);
        });

        IgniteInternalFuture initFut = GridTestUtils.runAsync(() -> {
            try {
                receiverSpi.onContextInitialized(receiverCtx);
            }
            catch (Exception ignored) {
                // No-op.
            }
        });

        assertFalse("Check test logs, NPE was found",
            GridTestUtils.waitForCondition(npeLsnr::check, 3_000));

        initFut.get();
        sendFut.get();
    }

    /**
     * Initializes TcpCommunicationSpi with given context, node, logger and clientMode flag.
     */
    private TcpCommunicationSpi initializeSpi(GridSpiTestContext ctx,
                                              GridTestNode node,
                                              IgniteLogger log,
                                              boolean clientMode) throws Exception {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(2000);

        IgniteConfiguration cfg = new IgniteConfiguration()
            .setGridLogger(log)
            .setClientMode(clientMode);

        IgniteTestResources rsrcs = new IgniteTestResources(cfg);

        resourcesToClean.add(rsrcs);

        cfg.setMBeanServer(rsrcs.getMBeanServer());

        node.setId(rsrcs.getNodeId());

        MessageFactoryProvider testMsgFactory = factory -> factory.register(GridTestMessage.DIRECT_TYPE, GridTestMessage::new);

        ctx.messageFactory(new IgniteMessageFactoryImpl(new MessageFactoryProvider[]{new GridIoMessageFactory(), testMsgFactory}));

        ctx.setLocalNode(node);

        rsrcs.inject(spi);

        spi.spiStart(getTestIgniteInstanceName() + node.order());

        node.setAttributes(spi.getNodeAttributes());
        node.setAttribute(ATTR_MACS, F.concat(U.allLocalMACs(), ", "));

        return spi;
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

        String host = findHostName(addrs.get2());
        String ip = findIpAddr(addrs.get1());

        assertNotNull("addrs=" + addrs, ip);

        log.info("Testing ip=" + ip + " host=" + host);

        int nodeIdx = 0;

        locHost = ip;
        checkHostNamesAttr(startGrid(nodeIdx++), false, true);

        // If found host name, then check it.
        if (host != null) {
            locHost = host;

            checkHostNamesAttr(startGrid(nodeIdx++), false, false);
        }

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
        IgniteBiTuple<Collection<String>, Collection<String>> addrs = U.resolveLocalAddresses(
            new InetSocketAddress(0).getAddress()
        );

        String host = findHostName(addrs.get2());
        String ip = findIpAddr(addrs.get1());

        log.info("Testing ip=" + ip + " host=" + host);

        int nodeIdx = 0;

        locHost = ip;
        checkHostNamesAttr(startGrid(nodeIdx++), false, false);

        // If found host name, then check it.
        if (host != null) {
            locHost = host;
            checkHostNamesAttr(startGrid(nodeIdx++), false, false);
        }

        locHost = null;
        checkHostNamesAttr(startGrid(nodeIdx++), true, false);
    }

    /** @return Host name, or {@code null} if all addresses are IPs. */
    private @Nullable String findHostName(Collection<String> addrs) {
        return addrs.stream()
            .filter(addr -> {
                try {
                    InetAddress inet = InetAddress.getByName(addr);

                    return !(inet instanceof Inet6Address) && !(inet instanceof Inet4Address);
                }
                catch (Exception e) {
                    return true;  // Failed to parse, then not an IP address.
                }
            })
            .findFirst()
            .orElse(null);
    }

    /** */
    @Test
    public void testFindingAddresses() throws Exception {
        Collection<String> addrs = F.asList(
            "0.0.0.0",
            "127.0.0.1",
            "::1",
            "192.168.1.1",
            "fe80::1%lo0",
            "2001:db8::ff00:42:8329",
            "abcd"
        );

        assertEquals("192.168.1.1", findIpAddr(addrs));
        assertEquals("abcd", findHostName(addrs));
    }

    /** @return Non-loopback IP. */
    private String findIpAddr(Collection<String> addrs) throws Exception {
        for (String addr : addrs) {
            InetAddress inetAddr = U.resolveLocalHost(addr);

            if (!inetAddr.isLoopbackAddress() && !inetAddr.isAnyLocalAddress())
                return addr;
        }

        throw new IllegalArgumentException("No IP address in the list: " + addrs);
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
