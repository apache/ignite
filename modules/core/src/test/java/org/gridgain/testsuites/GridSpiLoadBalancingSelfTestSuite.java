/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.apache.ignite.spi.loadbalancing.adaptive.*;
import org.apache.ignite.spi.loadbalancing.roundrobin.*;
import org.apache.ignite.spi.loadbalancing.weightedrandom.*;

/**
 * Load balancing SPI self-test suite.
 */
public final class GridSpiLoadBalancingSelfTestSuite {
    /**
     * Enforces singleton.
     */
    private GridSpiLoadBalancingSelfTestSuite() {
        // No-op.
    }

    /**
     * @return Test suite.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("Grid Load Balancing Test Suite");

        // Random.
        suite.addTest(new TestSuite(GridWeightedRandomLoadBalancingSpiSelfTest.class));
        suite.addTest(new TestSuite(GridWeightedRandomLoadBalancingSpiWeightedSelfTest.class));
        suite.addTest(new TestSuite(GridWeightedRandomLoadBalancingSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridWeightedRandomLoadBalancingSpiConfigSelfTest.class));

        // Round-robin.
        suite.addTest(new TestSuite(GridRoundRobinLoadBalancingSpiLocalNodeSelfTest.class));
        suite.addTest(new TestSuite(GridRoundRobinLoadBalancingSpiMultipleNodesSelfTest.class));
        suite.addTest(new TestSuite(GridRoundRobinLoadBalancingSpiTopologyChangeSelfTest.class));
        suite.addTest(new TestSuite(GridRoundRobinLoadBalancingSpiNotPerTaskSelfTest.class));
        suite.addTest(new TestSuite(GridRoundRobinLoadBalancingSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridRoundRobinLoadBalancingNotPerTaskMultithreadedSelfTest.class));

        // Adaptive.
        suite.addTest(new TestSuite(GridAdaptiveLoadBalancingSpiSelfTest.class));
        suite.addTest(new TestSuite(GridAdaptiveLoadBalancingSpiMultipleNodeSelfTest.class));
        suite.addTest(new TestSuite(GridAdaptiveLoadBalancingSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridAdaptiveLoadBalancingSpiConfigSelfTest.class));

        return suite;
    }
}
