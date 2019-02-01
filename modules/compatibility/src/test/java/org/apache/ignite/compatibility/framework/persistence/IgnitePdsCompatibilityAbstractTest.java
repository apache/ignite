/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.framework.persistence;

import org.apache.ignite.compatibility.framework.IgniteCompatibilityTestConfig;
import org.apache.ignite.compatibility.framework.IgniteCompatibilityTwoVersionsAbstractTest;
import org.apache.ignite.compatibility.framework.node.IgniteCompatibilityRemoteNode;
import org.apache.ignite.compatibility.persistence.api.ConfigureCommand;
import org.apache.ignite.compatibility.persistence.api.LatestNodeScenarioRunner;
import org.apache.ignite.compatibility.persistence.api.LegacyNodeScenarioRunner;
import org.apache.ignite.compatibility.persistence.api.PdsCompatibilityLatestNodeScenario;
import org.apache.ignite.compatibility.persistence.api.PdsCompatibilityLegacyNodeScenario;
import org.apache.ignite.compatibility.start.IgniteCompatibilityStartNodeClosure;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteOutClosure;
import org.junit.Test;

/**
 *
 */
public abstract class IgnitePdsCompatibilityAbstractTest extends IgniteCompatibilityTwoVersionsAbstractTest {
    /** Scenario execution timeout in milliseconds. */
    private static final int SCENARIO_EXECUTION_TIMEOUT_MS = 2 * 60 * 1000; // 2 minutes.

    /** Consistent node id. */
    private static final String CONSISTENT_NODE_ID = "persistent-node";

    /** Implement this method to provide legacy node scenario. */
    protected abstract PdsCompatibilityLegacyNodeScenario legacyScenario();

    /** Implement this method to provide latest node scenario. */
    protected abstract PdsCompatibilityLatestNodeScenario latestScenario();

    /**
     *
     */
    @Test
    public void testCompatibility() throws Exception {
        final PdsCompatibilityLegacyNodeScenario legacyScenario = legacyScenario();

        final PdsCompatibilityLatestNodeScenario latestScenario = latestScenario();

        if (legacyScenario == null || latestScenario == null) {
            log.warning("Test skipped due to empty legacy/latest scenario " +
                "[legacy=" + legacyScenario + ", latest=" + latestScenario + "]");

            return;
        }

        IgniteCompatibilityRemoteNode oldNode = startNode(prevVer, new ConfigureCommand(legacyScenario));

        oldNode.activate();

        oldNode.run(new LegacyNodeScenarioRunner(legacyScenario), SCENARIO_EXECUTION_TIMEOUT_MS);

        oldNode.stop();

        IgniteCompatibilityRemoteNode newNode = startNode(curVer, new ConfigureCommand(latestScenario));

        newNode.activate();

        newNode.run(new LatestNodeScenarioRunner(latestScenario), SCENARIO_EXECUTION_TIMEOUT_MS);

        newNode.stop();
    }

    /**
     * @param ver Nodes version.
     * @return Started nodes.
     * @throws Exception If failed.
     */
    protected IgniteCompatibilityRemoteNode startNode(String ver, IgniteOutClosure<IgniteConfiguration> configurationClojure) throws Exception {
        assert !F.isEmpty(ver);

        IgniteCompatibilityStartNodeClosure startNodeClojure = IgniteCompatibilityTestConfig.get()
            .startNodeClosure().get()
            .nodeName(CONSISTENT_NODE_ID)
            .enableStorage(true)
            .preConfigure(configurationClojure);

        IgniteCompatibilityRemoteNode node = IgniteCompatibilityRemoteNode.startVersion(
            ver,
            startNodeClojure
        );

        return node;
    }
}
