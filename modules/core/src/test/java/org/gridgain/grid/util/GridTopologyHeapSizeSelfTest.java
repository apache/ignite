/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Tests for calculation logic for topology heap size.
 */
public class GridTopologyHeapSizeSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTopologyHeapSizeInOneJvm() throws Exception {
        try {
            ClusterNode node1 = startGrid(1).cluster().node();
            ClusterNode node2 = startGrid(2).cluster().node();

            double allSize = U.heapSize(F.asList(node1, node2), 10);

            double size1 = U.heapSize(node1, 10);

            assertEquals(size1, allSize, 1E-5);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public void testTopologyHeapSizeForNodesWithDifferentPids() {
        GridTestNode node1 = getNode("123456789ABC", 1000);
        GridTestNode node2 = getNode("123456789ABC", 1001);

        double size1 = U.heapSize(node1, 10);
        double size2 = U.heapSize(node2, 10);

        double allSize = U.heapSize(F.asList((ClusterNode)node1, node2), 10);

        assertEquals(size1 + size2, allSize, 1E-5);
    }

    /** */
    public void testTopologyHeapSizeForNodesWithDifferentMacs() {
        GridTestNode node1 = getNode("123456789ABC", 1000);
        GridTestNode node2 = getNode("CBA987654321", 1000);

        double size1 = U.heapSize(node1, 10);
        double size2 = U.heapSize(node2, 10);

        double allSize = U.heapSize(F.asList((ClusterNode)node1, node2), 10);

        assertEquals(size1 + size2, allSize, 1E-5);
    }

    /**
     * Creates test node with specified attributes.
     *
     * @param mac Node mac addresses.
     * @param pid Node PID.
     * @return Node.
     */
    private GridTestNode getNode(String mac, int pid) {
        DiscoveryNodeMetricsAdapter metrics = new DiscoveryNodeMetricsAdapter();

        metrics.setHeapMemoryMaximum(1024 * 1024 * 1024);
        metrics.setHeapMemoryInitialized(1024 * 1024 * 1024);

        GridTestNode node = new GridTestNode(UUID.randomUUID(), metrics);

        node.addAttribute(ATTR_MACS, mac);
        node.addAttribute(ATTR_JVM_PID, pid);

        return node;
    }
}
