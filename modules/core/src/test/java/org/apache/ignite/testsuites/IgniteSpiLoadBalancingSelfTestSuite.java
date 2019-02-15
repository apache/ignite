/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Load balancing SPI self-test suite.
 */
@RunWith(AllTests.class)
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
        suite.addTest(new JUnit4TestAdapter(GridWeightedRandomLoadBalancingSpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridWeightedRandomLoadBalancingSpiWeightedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridWeightedRandomLoadBalancingSpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridWeightedRandomLoadBalancingSpiConfigSelfTest.class));

        // Round-robin.
        suite.addTest(new JUnit4TestAdapter(GridRoundRobinLoadBalancingSpiLocalNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridRoundRobinLoadBalancingSpiMultipleNodesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridRoundRobinLoadBalancingSpiTopologyChangeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridRoundRobinLoadBalancingSpiNotPerTaskSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridRoundRobinLoadBalancingSpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridRoundRobinLoadBalancingNotPerTaskMultithreadedSelfTest.class));

        // Adaptive.
        suite.addTest(new JUnit4TestAdapter(GridAdaptiveLoadBalancingSpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridAdaptiveLoadBalancingSpiMultipleNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridAdaptiveLoadBalancingSpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridAdaptiveLoadBalancingSpiConfigSelfTest.class));

        // Load balancing for internal tasks.
        suite.addTest(new JUnit4TestAdapter(GridInternalTasksLoadBalancingSelfTest.class));

        return suite;
    }
}
