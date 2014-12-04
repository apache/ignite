/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.weightedrandom;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;
import java.util.*;

/**
 * Weighted random load balancing SPI.
 */
@GridSpiTest(spi = GridWeightedRandomLoadBalancingSpi.class, group = "Load Balancing SPI")
public class GridWeightedRandomLoadBalancingSpiSelfTest extends
    GridSpiAbstractTest<GridWeightedRandomLoadBalancingSpi> {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    public void testSingleNode() throws Exception {
        List<ClusterNode> nodes = Collections.singletonList((ClusterNode)new GridTestNode(UUID.randomUUID()));

        ClusterNode node = getSpi().getBalancedNode(new GridTestTaskSession(), nodes, new GridTestJob());

        assert nodes.contains(node);

        // Verify that same instance is returned every time.
        ClusterNode balancedNode = getSpi().getBalancedNode(new GridTestTaskSession(), nodes, new GridTestJob());

        assert node == balancedNode;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodes() throws Exception {
        List<ClusterNode> nodes = new ArrayList<>();

        for (int i = 0; i < 10; i++)
            nodes.add(new GridTestNode(UUID.randomUUID()));

        // Seal it.
        nodes = Collections.unmodifiableList(nodes);

        ClusterNode node = getSpi().getBalancedNode(new GridTestTaskSession(), nodes, new GridTestJob());

        assert node != null;
        assert nodes.contains(node);
    }
}
