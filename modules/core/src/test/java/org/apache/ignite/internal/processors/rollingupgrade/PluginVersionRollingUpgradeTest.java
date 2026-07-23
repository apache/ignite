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

package org.apache.ignite.internal.processors.rollingupgrade;

import org.junit.Test;

/** */
public class PluginVersionRollingUpgradeTest extends AbstractRollingUpgradeTest {
    /** */
    @Test
    public void testUpgradeDisabledJoinWithExtraComponent() throws Exception {
        startGrid(0, "2.19.0");

        checkJoinFailed(1, "2.19.0 | 1.0.0", VER_NOT_EQUAL_ERR);
    }

    /** */
    @Test
    public void testUpgradeDisabledJoinWithMissingComponent() throws Exception {
        startGrid(0, "2.19.0 | 1.0.0");

        checkJoinFailed(1, "2.19.0", false, VER_NOT_EQUAL_ERR);

        checkJoinFailed(1, "2.19.0 | 2.0.0", VER_NOT_EQUAL_ERR);

        checkJoinSuccess(1, "2.19.0 | 1.0.0", false);
        checkJoinSuccess(2, "2.19.0", true);
    }

    /** */
    @Test
    public void testNewPluginActivation() throws Exception {
        startCluster("2.19.0");

        ru(1).enableVersionUpgrade();

        checkVersionUpgradeInProgress("2.19.0 | null", "null | null");

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.0 | null", "2.19.0 | 1.0.0"));

        finalizeClusterVersion(1, "2.19.0 | 1.0.0");
    }

    /** */
    @Test
    public void testPluginVersionUpgrade() throws Exception {
        startCluster("2.19.0 | 1.0.0");

        ru(1).enableVersionUpgrade();

        checkVersionUpgradeInProgress("2.19.0 | 1.0.0", "null | null");

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.0 | 1.0.0", "2.19.0 | 2.0.0"));

        finalizeClusterVersion(1, "2.19.0 | 2.0.0");

        ru(1).enableVersionUpgrade();

        checkVersionUpgradeInProgress("2.19.0 | 2.0.0", "null | null");

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.0 | 2.0.0", "2.19.0 | 3.0.0"));

        finalizeClusterVersion(1, "2.19.0 | 3.0.0");
    }

    /** */
    @Test
    public void testPluginAndIgniteVersionUpgrade() throws Exception {
        startCluster("2.19.0 | 1.0.0");

        ru(1).enableVersionUpgrade();

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.0 | 1.0.0", "2.20.0 | 2.0.0"));

        finalizeClusterVersion(1, "2.20.0 | 2.0.0");
    }

    /** */
    @Test
    public void testComponentVersionValidationDuringFinalization() throws Exception {
        startGrid(0, "2.19.0 | 1.0.0");
        startGrid(1, "2.19.0 | 1.0.0");
        startClientGrid(2, "2.19.0");

        checkVersionUpgradeInactive("2.19.0 | 1.0.0");

        ru(1).enableVersionUpgrade();

        startClientGrid(3, "2.19.0 | 2.0.0");
        checkVersionFinalizationFailed(0, MULTIPLE_VER_IN_TOP_ERR);
        stopGrid(3);

        startGrid(4, "2.19.0 | 2.0.0");
        checkVersionFinalizationFailed(0, MULTIPLE_VER_IN_TOP_ERR);
        stopGrid(4);

        startClientGrid(3, "2.19.0 | 1.0.0");
        startGrid(4, "2.19.0 | 1.0.0");

        finalizeClusterVersion(1, "2.19.0 | 1.0.0");
    }

    /** */
    @Test
    public void testNodeValidationDuringUpgrade() throws Exception {
        startCluster("2.19.0 | 1.0.0");

        ru(1).enableVersionUpgrade();

        checkJoinFailed(3, "2.20.0", false, "Some components active in the cluster are not configured on the joining server node");
        checkJoinSuccess(3, "2.20.0", true);

        checkJoinFailed(4, "2.20.0 | 3.0.0", RU_UNAVAILABLE_BETWEEN_VER_ERR);

        checkJoinSuccess(4, "2.20.0 | 2.0.0", true);

        checkJoinFailed(5, "2.20.0 | 3.0.0", VER_INCOMPATIBLE_ERR);

        upgradeNodeVersion(0, "2.19.0 | 1.0.0", "2.20.0 | 2.0.0");
        upgradeNodeVersion(1, "2.19.0 | 1.0.0", "2.20.0 | 2.0.0");
        upgradeNodeVersion(2, "2.19.0 | 1.0.0", "2.20.0 | 2.0.0");
        upgradeNodeVersion(4, "2.19.0 | 1.0.0", "2.20.0 | 2.0.0");

        finalizeClusterVersion(1, "2.20.0 | 2.0.0");
    }
}
