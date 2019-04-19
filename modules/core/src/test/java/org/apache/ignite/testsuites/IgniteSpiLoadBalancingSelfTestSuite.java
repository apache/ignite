/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Load balancing SPI self-test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridWeightedRandomLoadBalancingSpiSelfTest.class,
    GridWeightedRandomLoadBalancingSpiWeightedSelfTest.class,
    GridWeightedRandomLoadBalancingSpiStartStopSelfTest.class,
    GridWeightedRandomLoadBalancingSpiConfigSelfTest.class,

    // Round-robin.
    GridRoundRobinLoadBalancingSpiLocalNodeSelfTest.class,
    GridRoundRobinLoadBalancingSpiMultipleNodesSelfTest.class,
    GridRoundRobinLoadBalancingSpiTopologyChangeSelfTest.class,
    GridRoundRobinLoadBalancingSpiNotPerTaskSelfTest.class,
    GridRoundRobinLoadBalancingSpiStartStopSelfTest.class,
    GridRoundRobinLoadBalancingNotPerTaskMultithreadedSelfTest.class,

    // Adaptive.
    GridAdaptiveLoadBalancingSpiSelfTest.class,
    GridAdaptiveLoadBalancingSpiMultipleNodeSelfTest.class,
    GridAdaptiveLoadBalancingSpiStartStopSelfTest.class,
    GridAdaptiveLoadBalancingSpiConfigSelfTest.class,

    // Load balancing for internal tasks.
    GridInternalTasksLoadBalancingSelfTest.class
})
public final class IgniteSpiLoadBalancingSelfTestSuite {
}
