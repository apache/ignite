/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Test for {@link TcpDiscoverySpi}.
 */
public class GridTcpDiscoveryConcurrentStartTest extends GridCommonAbstractTest {
    /** */
    private static final int TOP_SIZE = 1;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static volatile boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(gridName);

        if (client) {
            TcpDiscoveryVmIpFinder clientIpFinder = new TcpDiscoveryVmIpFinder();

            String addr = new ArrayList<>(ipFinder.getRegisteredAddresses()).iterator().next().toString();

            if (addr.startsWith("/"))
                addr = addr.substring(1);

            clientIpFinder.setAddresses(Arrays.asList(addr));

            TcpClientDiscoverySpi discoSpi = new TcpClientDiscoverySpi();

            discoSpi.setIpFinder(clientIpFinder);

            cfg.setDiscoverySpi(discoSpi);
        }
        else {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(discoSpi);
        }

        cfg.setLocalHost("127.0.0.1");

        cfg.setCacheConfiguration();

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStart() throws Exception {
        for (int i = 0; i < 50; i++) {
            try {
                startGridsMultiThreaded(TOP_SIZE);
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartClients() throws Exception {
        for (int i = 0; i < 50; i++) {
            try {
                client = false;

                startGrid();

                client = true;

                startGridsMultiThreaded(TOP_SIZE);
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }
}
