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

package org.apache.ignite.testsuites;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.ignite.spi.loadbalancing.adaptive.GridAdaptiveLoadBalancingSpiConfigSelfTest;
import org.apache.ignite.spi.loadbalancing.adaptive.GridAdaptiveLoadBalancingSpiMultipleNodeSelfTest;
import org.apache.ignite.spi.loadbalancing.adaptive.GridAdaptiveLoadBalancingSpiSelfTest;
import org.apache.ignite.spi.loadbalancing.adaptive.GridAdaptiveLoadBalancingSpiStartStopSelfTest;
import org.apache.ignite.spi.loadbalancing.internal.GridInternalTasksLoadBalancingSelfTest;
import org.apache.ignite.spi.loadbalancing.roundrobin.GridRoundRobinLoadBalancingNotPerTaskMultithreadedSelfTest;
import org.apache.ignite.spi.loadbalancing.roundrobin.GridRoundRobinLoadBalancingSpiLocalNodeSelfTest;
import org.apache.ignite.spi.loadbalancing.roundrobin.GridRoundRobinLoadBalancingSpiMultipleNodesSelfTest;
import org.apache.ignite.spi.loadbalancing.roundrobin.GridRoundRobinLoadBalancingSpiNotPerTaskSelfTest;
import org.apache.ignite.spi.loadbalancing.roundrobin.GridRoundRobinLoadBalancingSpiStartStopSelfTest;
import org.apache.ignite.spi.loadbalancing.roundrobin.GridRoundRobinLoadBalancingSpiTopologyChangeSelfTest;
import org.apache.ignite.spi.loadbalancing.weightedrandom.GridWeightedRandomLoadBalancingSpiConfigSelfTest;
import org.apache.ignite.spi.loadbalancing.weightedrandom.GridWeightedRandomLoadBalancingSpiSelfTest;
import org.apache.ignite.spi.loadbalancing.weightedrandom.GridWeightedRandomLoadBalancingSpiStartStopSelfTest;
import org.apache.ignite.spi.loadbalancing.weightedrandom.GridWeightedRandomLoadBalancingSpiWeightedSelfTest;

/**
 * Load balancing SPI self-test suite.
 */
public final class IgniteSpiLoadBalancingSelfTestSuite {
    /**
     * Enforces singleton.
     */
    private IgniteSpiLoadBalancingSelfTestSuite() {
        // No-op.
    }

    /**
     * @return Test suite.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("Ignite Load Balancing Test Suite");

        // Random.
        suite.addTestSuite(GridWeightedRandomLoadBalancingSpiSelfTest.class);
        suite.addTestSuite(GridWeightedRandomLoadBalancingSpiWeightedSelfTest.class);
        suite.addTestSuite(GridWeightedRandomLoadBalancingSpiStartStopSelfTest.class);
        suite.addTestSuite(GridWeightedRandomLoadBalancingSpiConfigSelfTest.class);

        // Round-robin.
        suite.addTestSuite(GridRoundRobinLoadBalancingSpiLocalNodeSelfTest.class);
        suite.addTestSuite(GridRoundRobinLoadBalancingSpiMultipleNodesSelfTest.class);
        suite.addTestSuite(GridRoundRobinLoadBalancingSpiTopologyChangeSelfTest.class);
        suite.addTestSuite(GridRoundRobinLoadBalancingSpiNotPerTaskSelfTest.class);
        suite.addTestSuite(GridRoundRobinLoadBalancingSpiStartStopSelfTest.class);
        suite.addTestSuite(GridRoundRobinLoadBalancingNotPerTaskMultithreadedSelfTest.class);

        // Adaptive.
        suite.addTestSuite(GridAdaptiveLoadBalancingSpiSelfTest.class);
        suite.addTestSuite(GridAdaptiveLoadBalancingSpiMultipleNodeSelfTest.class);
        suite.addTestSuite(GridAdaptiveLoadBalancingSpiStartStopSelfTest.class);
        suite.addTestSuite(GridAdaptiveLoadBalancingSpiConfigSelfTest.class);

        // Load balancing for internal tasks.
        suite.addTestSuite(GridInternalTasksLoadBalancingSelfTest.class);

        return suite;
    }
}
