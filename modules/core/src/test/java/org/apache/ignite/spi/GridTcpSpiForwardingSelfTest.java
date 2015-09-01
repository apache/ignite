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

package org.apache.ignite.spi;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi} and {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}.
 */
public class GridTcpSpiForwardingSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int locPort1 = 47500;

    /** */
    private static final int locPort2 = 48500;

    /** */
    private static final int extPort1 = 10000;

    /** */
    private static final int extPort2 = 20000;

    /** */
    private static final int commLocPort1 = 47100;

    /** */
    private static final int commLocPort2 = 48100;

    /** */
    private static final int commExtPort1 = 10100;

    /** */
    private static final int commExtPort2 = 20100;

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "deprecation"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:" + extPort1, "127.0.0.1:" + extPort2));

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        final int locPort;
        final int extPort;
        final int commExtPort;
        final int commLocPort;

        if (getTestGridName(0).equals(gridName)) {
            locPort = locPort1;
            extPort = extPort1;
            commLocPort = commLocPort1;
            commExtPort = commExtPort1;
        }
        else if (getTestGridName(1).equals(gridName)) {
            locPort = locPort2;
            extPort = extPort2;
            commLocPort = commLocPort2;
            commExtPort = commExtPort2;
        }
        else
            throw new IllegalArgumentException("Unknown grid name");

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        spi.setLocalPort(locPort);
        spi.setLocalPortRange(1);
        cfg.setDiscoverySpi(spi);
        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi() {
            @Override protected GridCommunicationClient createTcpClient(ClusterNode node) throws IgniteCheckedException {
                Map<String, Object> attrs = new HashMap<>(node.attributes());
                attrs.remove(createSpiAttributeName(ATTR_PORT));

                ((TcpDiscoveryNode)node).setAttributes(attrs);

                return super.createTcpClient(node);
            }
        };

        commSpi.setLocalAddress("127.0.0.1");
        commSpi.setLocalPort(commLocPort);
        commSpi.setLocalPortRange(1);
        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        final Map<InetSocketAddress, ? extends Collection<InetSocketAddress>> mp = F.asMap(
            new InetSocketAddress("127.0.0.1", locPort), F.asList(new InetSocketAddress("127.0.0.1", extPort)),
            new InetSocketAddress("127.0.0.1", commLocPort), F.asList(new InetSocketAddress("127.0.0.1", commExtPort))
        );

        cfg.setAddressResolver(new AddressResolver() {
            @Override public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress addr) {
                return mp.get(addr);
            }
        });

        return cfg;
    }

    /**
     * @throws Exception If any error occurs.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void testForward() throws Exception {
        InetAddress locHost = InetAddress.getByName("127.0.0.1");

        try (
            GridTcpForwarder tcpForward1 = new GridTcpForwarder(locHost, extPort1, locHost, locPort1, log);
            GridTcpForwarder tcpForward2 = new GridTcpForwarder(locHost, extPort2, locHost, locPort2, log);
            GridTcpForwarder tcpForward3 = new GridTcpForwarder(locHost, commExtPort1, locHost, commLocPort1, log);
            GridTcpForwarder tcpForward4 = new GridTcpForwarder(locHost, commExtPort2, locHost, commLocPort2, log);

            Ignite g1 = startGrid(0);
            Ignite g2 = startGrid(1)
        ) {
            assertEquals(2, grid(0).cluster().nodes().size());
            assertEquals(2, grid(1).cluster().nodes().size());

            Collection<Integer> t = g1.compute().broadcast(new IgniteCallable<Integer>() {
                @Override public Integer call() throws Exception {
                    return 13;
                }
            });

            assertEquals(F.asList(13, 13), t);
        }
    }
}