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
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.BasicAddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for {@link TcpDiscoverySpi} and {@link TcpCommunicationSpi}.
 */
public class GridTcpSpiForwardingSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int LOC_PORT_1 = 47500;

    /** */
    private static final int LOC_PORT_2 = 48500;

    /** */
    private static final int EXT_PORT_1 = 10000;

    /** */
    private static final int EXT_PORT_2 = 20000;

    /** */
    private static final int COMM_LOC_PORT_1 = 47100;

    /** */
    private static final int COMM_LOC_PORT_2 = 48100;

    /** */
    private static final int COMM_EXT_PORT_1 = 10100;

    /** */
    private static final int COMM_EXT_PORT_2 = 20100;

    /** */
    private AddressResolver rslvr;

    /** */
    private boolean ipFinderUseLocPorts;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        if (ipFinderUseLocPorts)
            ipFinder.setAddresses(Arrays.asList("127.0.0.1:" + LOC_PORT_1, "127.0.0.1:" + LOC_PORT_2));
        else
            ipFinder.setAddresses(Arrays.asList("127.0.0.1:" + EXT_PORT_1, "127.0.0.1:" + EXT_PORT_2));

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        final int locPort;
        final int commLocPort;

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName)) {
            locPort = LOC_PORT_1;
            commLocPort = COMM_LOC_PORT_1;
        }
        else if (getTestIgniteInstanceName(1).equals(igniteInstanceName)) {
            locPort = LOC_PORT_2;
            commLocPort = COMM_LOC_PORT_2;
        }
        else
            throw new IllegalArgumentException("Unknown Ignite instance name");

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        spi.setLocalPort(locPort);
        spi.setLocalPortRange(1);
        cfg.setDiscoverySpi(spi);
        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi() {
            @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx)
                throws IgniteCheckedException {
                Map<String, Object> attrs = new HashMap<>(node.attributes());

                attrs.remove(createSpiAttributeName(ATTR_PORT));

                ((TcpDiscoveryNode)node).setAttributes(attrs);

                return super.createTcpClient(node, connIdx);
            }
        };

        commSpi.setLocalAddress("127.0.0.1");
        commSpi.setLocalPort(commLocPort);
        commSpi.setLocalPortRange(1);
        commSpi.setSharedMemoryPort(-1);
        commSpi.setConnectionsPerNode(1);

        cfg.setCommunicationSpi(commSpi);

        assert rslvr != null;

        cfg.setAddressResolver(rslvr);

        return cfg;
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    public void testCustomResolver() throws Exception {
        final Map<InetSocketAddress, Collection<InetSocketAddress>> map = new HashMap<>();

        map.put(new InetSocketAddress("127.0.0.1", LOC_PORT_1), F.asList(new InetSocketAddress("127.0.0.1", EXT_PORT_1)));
        map.put(new InetSocketAddress("127.0.0.1", COMM_LOC_PORT_1), F.asList(new InetSocketAddress("127.0.0.1", COMM_EXT_PORT_1)));
        map.put(new InetSocketAddress("127.0.0.1", LOC_PORT_2), F.asList(new InetSocketAddress("127.0.0.1", EXT_PORT_2)));
        map.put(new InetSocketAddress("127.0.0.1", COMM_LOC_PORT_2), F.asList(new InetSocketAddress("127.0.0.1", COMM_EXT_PORT_2)));

        rslvr = new AddressResolver() {
            @Override public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress addr) {
                return map.get(addr);
            }
        };

        doTestForward();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicResolverMapPorts() throws Exception {
        Map<String, String> map = new HashMap<>();

        map.put("127.0.0.1:" + LOC_PORT_1, "127.0.0.1:" + EXT_PORT_1);
        map.put("127.0.0.1:" + COMM_LOC_PORT_1, "127.0.0.1:" + COMM_EXT_PORT_1);
        map.put("127.0.0.1:" + LOC_PORT_2, "127.0.0.1:" + EXT_PORT_2);
        map.put("127.0.0.1:" + COMM_LOC_PORT_2, "127.0.0.1:" + COMM_EXT_PORT_2);

        rslvr = new BasicAddressResolver(map);

        doTestForward();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicResolverMapAddress() throws Exception {
        Map<String, String> map = new HashMap<>();

        map.put("127.0.0.1", "127.0.0.1");

        rslvr = new BasicAddressResolver(map);

        ipFinderUseLocPorts = true;

        doTestForward();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicResolverErrors() throws Exception {
        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return new BasicAddressResolver(null);
                }
            },
            IllegalArgumentException.class,
            "At least one address mapping is required."
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return new BasicAddressResolver(new HashMap<String, String>());
                }
            },
            IllegalArgumentException.class,
            "At least one address mapping is required."
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Map<String, String> map = new HashMap<>();

                    map.put("from", null);

                    return new BasicAddressResolver(map);
                }
            },
            IllegalArgumentException.class,
            "Invalid address mapping: from=null"
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Map<String, String> map = new HashMap<>();

                    map.put("from", "");

                    return new BasicAddressResolver(map);
                }
            },
            IllegalArgumentException.class,
            "Invalid address mapping: from="
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Map<String, String> map = new HashMap<>();

                    map.put(null, "to");

                    return new BasicAddressResolver(map);
                }
            },
            IllegalArgumentException.class,
            "Invalid address mapping: null=to"
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Map<String, String> map = new HashMap<>();

                    map.put("", "to");

                    return new BasicAddressResolver(map);
                }
            },
            IllegalArgumentException.class,
            "Invalid address mapping: =to"
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Map<String, String> map = new HashMap<>();

                    map.put("from", "to:1111");

                    return new BasicAddressResolver(map);
                }
            },
            IllegalArgumentException.class,
            "Invalid address mapping: from=to:1111"
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Map<String, String> map = new HashMap<>();

                    map.put("from:1111", "to");

                    return new BasicAddressResolver(map);
                }
            },
            IllegalArgumentException.class,
            "Invalid address mapping: from:1111=to"
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Map<String, String> map = new HashMap<>();

                    map.put("from:1111:2222", "to:1111");

                    return new BasicAddressResolver(map);
                }
            },
            IllegalArgumentException.class,
            "Invalid address mapping: from:1111:2222=to:1111"
        );

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Map<String, String> map = new HashMap<>();

                    map.put("from:1111", "to:1111:2222");

                    return new BasicAddressResolver(map);
                }
            },
            IllegalArgumentException.class,
            "Invalid address mapping: from:1111=to:1111:2222"
        );
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestForward() throws Exception {
        InetAddress locHost = InetAddress.getByName("127.0.0.1");

        try (
            GridTcpForwarder tcpForward1 = new GridTcpForwarder(locHost, EXT_PORT_1, locHost, LOC_PORT_1, log);
            GridTcpForwarder tcpForward2 = new GridTcpForwarder(locHost, EXT_PORT_2, locHost, LOC_PORT_2, log);
            GridTcpForwarder tcpForward3 = new GridTcpForwarder(locHost, COMM_EXT_PORT_1, locHost, COMM_LOC_PORT_1, log);
            GridTcpForwarder tcpForward4 = new GridTcpForwarder(locHost, COMM_EXT_PORT_2, locHost, COMM_LOC_PORT_2, log);

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
