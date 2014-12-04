/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.internal.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Test for {@link GridTcpDiscoverySpi} and {@link GridTcpCommunicationSpi}.
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
        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:" + extPort1, "127.0.0.1:" + extPort2));

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

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
        cfg.setRestEnabled(false);
        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        GridTcpCommunicationSpi commSpi = new GridTcpCommunicationSpi() {
            @Override protected GridCommunicationClient createTcpClient(ClusterNode node) throws GridException {
                Map<String, Object> attrs = new HashMap<>(node.attributes());
                attrs.remove(createSpiAttributeName(ATTR_PORT));

                ((GridTcpDiscoveryNode)node).setAttributes(attrs);

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

        cfg.setAddressResolver(new GridAddressResolver() {
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
            assertEquals(2, grid(0).nodes().size());
            assertEquals(2, grid(1).nodes().size());

            Collection<Integer> t = g1.compute().broadcast(new Callable<Integer>() {
                @Override public Integer call() throws Exception {
                    return 13;
                }
            });

            assertEquals(F.asList(13, 13), t);
        }
    }
}
