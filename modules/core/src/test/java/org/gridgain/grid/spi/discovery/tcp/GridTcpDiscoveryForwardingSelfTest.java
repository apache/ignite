/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.net.*;
import java.util.*;

/**
 * Test for {@link org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi}.
 */
public class GridTcpDiscoveryForwardingSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int localPort1 = 47500;

    /** */
    private static final int localPort2 = 48500;

    /** */
    private static final int extPort1 = 10000;

    /** */
    private static final int extPort2 = 20000;

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "deprecation"})
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:" + extPort1));

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        final int locPort;
        final int extPort;

        if (getTestGridName(0).equals(gridName)) {
            locPort = localPort1;
            extPort = extPort1;
        }
        else if (getTestGridName(1).equals(gridName)) {
            locPort = localPort2;
            extPort = extPort2;
        }
        else
            throw new IllegalArgumentException("Unknown grid name");

        spi.setIpFinder(ipFinder);

        GridConfiguration cfg = super.getConfiguration(gridName);

        spi.setLocalPort(locPort);
        spi.setLocalPortRange(1);
        cfg.setDiscoverySpi(spi);
        cfg.setLocalHost("127.0.0.1");
        cfg.setRestEnabled(false);
        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        cfg.setAddressResolver(new GridAddressResolver() {
            @Override public Collection<InetSocketAddress> getExternalAddresses(InetSocketAddress addr) {
                return addr.equals(new InetSocketAddress("127.0.0.1", locPort)) ?
                    F.asList(new InetSocketAddress("127.0.0.1", extPort)) : null;
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
            GridTcpForwardServer tcpForwardSrv1 = new GridTcpForwardServer(locHost, extPort1, locHost, localPort1);
            GridTcpForwardServer tcpForwardSrv2 = new GridTcpForwardServer(locHost, extPort2, locHost, localPort2);
            Grid g1 = startGrid(0);
            Grid g2 = startGrid(1)
        ) {
            assertEquals(2, getTopologySize(2));
        }
    }

    /**
     * @param cnt Nodes count.
     * @return Size of topology with waiting about 10 seconds or while size does not equals to {@code cnt}.
     *      In first case result will be equals to topology size discovered for first node.
     * @throws Exception If any error occurs.
     */
    @SuppressWarnings("BusyWait")
    private int getTopologySize(int cnt) throws Exception {
        assert cnt > 0;

        for (int j = 0; j < 10; j++) {
            boolean isOk = true;

            for (int i = 0; i < cnt; i++)
                if (cnt != grid(i).nodes().size()) {
                    isOk = false;

                    break;
                }

            if (isOk)
                return cnt;

            Thread.sleep(1000);
        }

        return grid(0).nodes().size();
    }
}
