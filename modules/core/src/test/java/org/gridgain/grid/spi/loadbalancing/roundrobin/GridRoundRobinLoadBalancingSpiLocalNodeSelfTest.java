/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.roundrobin;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.junits.spi.*;
import java.util.*;

/**
 * Tests Round Robin load balancing for single node.
 */
@GridSpiTest(spi = GridRoundRobinLoadBalancingSpi.class, group = "Load Balancing SPI", triggerDiscovery = true)
public class GridRoundRobinLoadBalancingSpiLocalNodeSelfTest extends
    GridSpiAbstractTest<GridRoundRobinLoadBalancingSpi> {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    public void testLocalNode() throws Exception {
        assert getDiscoverySpi().getRemoteNodes().isEmpty();

        ClusterNode locNode = getDiscoverySpi().getLocalNode();

        ClusterNode node = getSpi().getBalancedNode(new GridTestTaskSession(IgniteUuid.randomUuid()),
            Collections.singletonList(locNode), new GridTestJob());

        assert  node == locNode;

        // Double check.
        node = getSpi().getBalancedNode(new GridTestTaskSession(IgniteUuid.randomUuid()),
            Collections.singletonList(locNode), new GridTestJob());

        assert node == locNode;
    }
}
