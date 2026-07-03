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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.rollingupgrade.RollingUpgradeProcessor.RollingUpgradeState;
import org.apache.ignite.internal.processors.rollingupgrade.feature.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.internal.IgniteVersionUtils.semanticVersion;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_19_2.VER_2_19_2_ID_1_FEATURE;
import static org.apache.ignite.internal.processors.security.NodeSecurityContextPropagationTest.discoveryRingMessageWorkerQueue;
import static org.apache.ignite.internal.processors.security.NodeSecurityContextPropagationTest.wrapRingMessageWorkerQueue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class ClusterVersionsRollingUpgradeTest extends AbstractRollingUpgradeTest {
    /** */
    private static final String NODE_IS_INCOMPATIBLE_WIH_RU_ERR =
        "The joining node version is incompatible with the current state of the cluster version Rolling Upgrade being in progress";

    /** */
    private static final String MULTIPLE_VER_IN_TOP_ERR =
        "Cluster version finalization failed. The topology contains nodes running multiple different versions";

    /** */
    @Test
    public void testVersionUpgradeDisabledSingleServer() throws Exception {
        startGrid(0);

        checkVersionUpgradeInactive("2.19.0");
    }

    /** */
    @Test
    public void testVersionUpgradeDisabledNodeJoin() throws Exception {
        startCluster();

        checkVersionUpgradeInactive(TEST_DEFAULT_VER);

        String msg = "The joining node version differs from the version of the cluster";

        checkJoinFailed(3, "2.18.0", msg);
        checkJoinFailed(3, "2.19.1", msg);
    }

    /** */
    @Test
    public void testVersionUpgradeDisabledFinalization() throws Exception {
        startCluster();

        ru(1).finalizeClusterVersion();

        checkVersionUpgradeInactive(TEST_DEFAULT_VER);
    }

    /** */
    @Test
    public void testVersionsFinalizationNoVersionUpgrade() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        ru(1).finalizeClusterVersion();

        checkVersionUpgradeInactive(TEST_DEFAULT_VER);

        restartNode(1);
        restartNode(2);
    }

    /** */
    @Test
    public void testVersionUpgradeCommandsIdempotency() throws Exception {
        startCluster();

        ru(0).enableVersionUpgrade();
        ru(1).enableVersionUpgrade();

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        ru(0).finalizeClusterVersion();
        ru(0).finalizeClusterVersion();

        checkVersionUpgradeInactive(TEST_DEFAULT_VER);
    }

    /** */
    @Test
    public void testVersionUpgradeEnabledNodeJoin() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        restartNode(2);
        restartNode(0);

        checkJoinSuccess(3, false);
        checkJoinSuccess(4, true);

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        checkJoinFailed(5, "2.18.0", NODE_IS_INCOMPATIBLE_WIH_RU_ERR);

        upgradeNodeVersion(0, "2.19.1");
        upgradeNodeVersion(2, "2.19.1");

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, "2.19.1");

        checkJoinFailed(5, "2.19.2", NODE_IS_INCOMPATIBLE_WIH_RU_ERR);

        restartNode(3);
        restartNode(4);

        restartNode(0);
        restartNode(2);

        upgradeNodeVersion(1, "2.19.1");
        upgradeNodeVersion(3, "2.19.1");
        upgradeNodeVersion(4, "2.19.1");

        finalizeClusterVersion(1, "2.19.1");
    }

    /** */
    @Test
    public void testClusterVersionUpgrade() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(1, "2.19.2");

        checkVersionFinalizationFailed(1, MULTIPLE_VER_IN_TOP_ERR);

        upgradeNodeVersion(0, "2.19.2");

        checkVersionFinalizationFailed(1, MULTIPLE_VER_IN_TOP_ERR);

        upgradeNodeVersion(2, "2.19.2");

        finalizeClusterVersion(1, "2.19.2");
    }

    /** */
    @Test
    public void testJoinAfterClusterVersionFinalization() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2");
        upgradeNodeVersion(2, "2.19.2");
        stopGrid(1);

        finalizeClusterVersion(0, "2.19.2");

        checkJoinSuccess(1, "2.19.2", false);
        checkJoinSuccess(4, "2.19.2", true);

        checkVersionUpgradeInactive("2.19.2");
    }

    /** */
    @Test
    public void testFeatureActivationListener() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        CountDownLatch featureActivationLatch = new CountDownLatch(3);

        forAllNodes(nodeIdx -> {
            upgradeNodeVersion(nodeIdx, "2.19.2");
            checkFeatureActivationSubscription(nodeIdx, VER_2_19_2_ID_1_FEATURE, featureActivationLatch);
        });

        finalizeClusterVersion(1, "2.19.2");

        assertTrue(featureActivationLatch.await(getTestTimeout(), MILLISECONDS));
    }

    /** */
    @Test
    public void testIterativeClusterVersionUpgrade() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.2"));

        upgradeNodeVersion(0, TEST_DEFAULT_VER, "2.19.2", "2.19.3");

        stopGrid(0);

        checkJoinSuccess(3, TEST_DEFAULT_VER, false);
        checkJoinSuccess(4, TEST_DEFAULT_VER, true);

        checkUpgradeFailed(2, "2.19.3", NODE_IS_INCOMPATIBLE_WIH_RU_ERR);
        checkUpgradeFailed(1, "2.19.3", NODE_IS_INCOMPATIBLE_WIH_RU_ERR);

        upgradeNodeVersion(3, "2.19.2");
        upgradeNodeVersion(4, "2.19.2");
        checkJoinSuccess(0, "2.19.3", false);

        checkUpgradeFailed(2, TEST_DEFAULT_VER, NODE_IS_INCOMPATIBLE_WIH_RU_ERR);
        checkUpgradeFailed(1, TEST_DEFAULT_VER, NODE_IS_INCOMPATIBLE_WIH_RU_ERR);

        restartNode(1);
        restartNode(2);

        upgradeNodeVersion(2, TEST_DEFAULT_VER, "2.19.2", "2.19.3");
        upgradeNodeVersion(1, TEST_DEFAULT_VER, "2.19.2", "2.19.3");
        upgradeNodeVersion(3, TEST_DEFAULT_VER, "2.19.2", "2.19.3");
        // After all cluster nodes have been upgraded, the Rolling Upgrade source version becomes equal to the cluster logical version.
        upgradeNodeVersion(4, TEST_DEFAULT_VER, TEST_DEFAULT_VER, "2.19.3");

        checkJoinSuccess(5, "2.19.2", false);
        checkJoinSuccess(6, "2.19.2", false);

        upgradeNodeVersion(5, TEST_DEFAULT_VER, "2.19.2", "2.19.3");
        // After all cluster nodes have been upgraded, the Rolling Upgrade source version becomes equal to the cluster logical version.
        upgradeNodeVersion(6, TEST_DEFAULT_VER, TEST_DEFAULT_VER, "2.19.3");

        finalizeClusterVersion(1, "2.19.3");
    }

    /** */
    @Test
    public void testMultipleClusterVersionUpgrades() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();
        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.2"));
        finalizeClusterVersion(1, "2.19.2");

        ru(1).enableVersionUpgrade();
        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.2", "2.20.0"));
        finalizeClusterVersion(1, "2.20.0");

        ru(1).enableVersionUpgrade();
        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.20.0", "2.21.0"));
        finalizeClusterVersion(1, "2.21.0");
    }

    /** */
    @Test
    public void testProductVersionChangeDuringClusterVersionUpgrade() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2");

        upgradeNodeVersion(0, "2.19.3");

        checkUpgradeFailed(1, "2.19.2", NODE_IS_INCOMPATIBLE_WIH_RU_ERR);
        checkUpgradeFailed(2, "2.19.2", NODE_IS_INCOMPATIBLE_WIH_RU_ERR);

        upgradeNodeVersion(1, "2.19.3");
        upgradeNodeVersion(2, "2.19.3");

        finalizeClusterVersion(1, "2.19.3");
    }

    /** */
    @Test
    public void testUpgradeBetweenVersionsWithCherryPicks() throws Exception {
        startCluster("2.19.3");

        ru(1).enableVersionUpgrade();

        checkJoinFailed(3, "2.20.0",
            "Rolling Upgrade is not available between the current cluster logical version and the joining node product version");

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.3", "2.20.1"));

        finalizeClusterVersion(1, "2.20.1");
    }

    /** */
    @Test
    public void testUpgradeBetweenVersionWithCherryPicksAndContinuousRange() throws Exception {
        startCluster("2.20.1");

        ru(1).enableVersionUpgrade();

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.20.1", "2.21.1"));

        finalizeClusterVersion(1, "2.21.1");
    }

    /** */
    @Test
    public void testConcurrentFinalization() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        try {
            IgniteInternalFuture<Object> firstFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(1, TEST_DEFAULT_VER));

            spi(grid(1)).waitForBlocked();

            IgniteInternalFuture<Object> secondFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(0, TEST_DEFAULT_VER));

            spi(grid(1)).waitForBlocked(2);

            spi(grid(1)).stopBlock();

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> secondFut.get(getTestTimeout(), MILLISECONDS),
                IgniteCheckedException.class,
                "Cluster version finalization process is already in progress"
            );

            firstFut.get(getTestTimeout(), MILLISECONDS);

            checkVersionUpgradeInactive(TEST_DEFAULT_VER);
        }
        finally {
            spi(grid(1)).stopBlock();
        }
    }

    /** */
    @Test
    public void testConcurrentFinalizationFromSameNode() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        try {
            IgniteInternalFuture<Object> firstFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(0, TEST_DEFAULT_VER));

            spi(grid(1)).waitForBlocked();

            IgniteInternalFuture<Object> secondFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(0, TEST_DEFAULT_VER));

            U.sleep(1000);

            spi(grid(1)).stopBlock();

            secondFut.get(getTestTimeout(), MILLISECONDS);

            firstFut.get(getTestTimeout(), MILLISECONDS);

            checkVersionUpgradeInactive(TEST_DEFAULT_VER);
        }
        finally {
            spi(grid(1)).stopBlock();
        }
    }

    /** */
    @Test
    public void testNodeJoinDuringFinalization() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        try {
            IgniteInternalFuture<Object> finalizationFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(1, TEST_DEFAULT_VER));

            spi(grid(1)).waitForBlocked();

            checkJoinFailed(3, TEST_DEFAULT_VER, "Node joins are not allowed during cluster version finalization");

            spi(grid(1)).stopBlock();

            finalizationFut.get(getTestTimeout(), MILLISECONDS);

            checkVersionUpgradeInactive(TEST_DEFAULT_VER);
        }
        finally {
            spi(grid(1)).stopBlock();
        }
    }

    /** */
    @Test
    public void testValidatedJoiningNodesAccountedDuringFinalization() throws Exception {
        wrapRingMessageWorkerQueue(startGrid(0, TEST_DEFAULT_VER));
        startGrid(1, TEST_DEFAULT_VER);
        startClientGrid(2, TEST_DEFAULT_VER);

        ru(1).enableVersionUpgrade();

        TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch = new CountDownLatch(1);
        TestRollingUpgradeProcessor.nodeJoinUnblockedLatch = new CountDownLatch(1);

        try {
            IgniteInternalFuture<IgniteEx> startFut = GridTestUtils.runAsync(() -> startGrid(3, "2.19.2"));

            assertTrue(TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch.await(getTestTimeout(), MILLISECONDS));

            discoveryRingMessageWorkerQueue(grid(0)).block();

            IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(1, TEST_DEFAULT_VER));

            waitForBlockedDiscoveryMessages(grid(0), 1, InitMessage.class);

            TestRollingUpgradeProcessor.nodeJoinUnblockedLatch.countDown();

            discoveryRingMessageWorkerQueue(grid(0)).unblock();

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> finalizeFut.get(getTestTimeout(), MILLISECONDS),
                IgniteCheckedException.class,
                MULTIPLE_VER_IN_TOP_ERR
            );

            startFut.get(getTestTimeout(), MILLISECONDS);

            checkVersionUpgradeInProgress(TEST_DEFAULT_VER, "2.19.2");
        }
        finally {
            TestRollingUpgradeProcessor.nodeJoinUnblockedLatch.countDown();
        }
    }

    /** */
    @Test
    public void testCoordinatorChangeAfterVersionFinalizationFailedOnCoordinator() throws Exception {
        startCluster();
        startGrid(3, TEST_DEFAULT_VER);

        ru(1).enableVersionUpgrade();

        TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch = new CountDownLatch(1);
        TestNodeValidationFailedEventListener.nodeValidationFailedEventUnblockedLatch = new CountDownLatch(1);

        try {
            IgniteConfiguration cfg = getConfiguration(4, "2.19.1")
                .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));

            IgniteInternalFuture<IgniteEx> startFut = GridTestUtils.runAsync(() -> startGrid(cfg));

            assertTrue(TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch.await(getTestTimeout(), MILLISECONDS));

            IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(1, TEST_DEFAULT_VER));

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> finalizeFut.get(getTestTimeout(), MILLISECONDS),
                IgniteCheckedException.class,
                MULTIPLE_VER_IN_TOP_ERR
            );

            TestNodeValidationFailedEventListener.nodeValidationFailedEventUnblockedLatch.countDown();

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> startFut.get(getTestTimeout(), MILLISECONDS),
                IgniteCheckedException.class,
                "Local node's binary configuration is not equal to remote node's binary configuration"
            );
        }
        finally {
            TestNodeValidationFailedEventListener.nodeValidationFailedEventUnblockedLatch.countDown();
        }

        stopGrid(coordinatorIndex());

        forAllNodes(n -> upgradeNodeVersion(n, "2.19.1"));

        finalizeClusterVersion(1, "2.19.1");
    }

    /** */
    @Test
    public void testConcurrentFinalizationErrorPreserveNodeFence() throws Exception {
        wrapRingMessageWorkerQueue(startGrid(0, TEST_DEFAULT_VER));
        startGrid(1, TEST_DEFAULT_VER);
        startGrid(2, TEST_DEFAULT_VER);

        startClientGrid(3, TEST_DEFAULT_VER);

        ru(1).enableVersionUpgrade();

        discoveryRingMessageWorkerQueue(grid(0)).block();

        try {
            IgniteInternalFuture<Object> firstFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(1, TEST_DEFAULT_VER));

            waitForBlockedDiscoveryMessages(grid(0), 1, InitMessage.class);

            IgniteInternalFuture<Object> secondFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(3, TEST_DEFAULT_VER));

            waitForBlockedDiscoveryMessages(grid(0), 2, InitMessage.class);

            UUID activeFinalizeProcId = extractActiveFinalizeProcessId(1);

            spi(grid(2)).blockMessages((node, msg) -> {
                if (!(msg instanceof SingleNodeMessage<?> singleNodeMsg))
                    return false;

                return singleNodeMsg.processId().equals(activeFinalizeProcId);
            });

            discoveryRingMessageWorkerQueue(grid(0)).unblock();

            spi(grid(2)).waitForBlocked();

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> secondFut.get(getTestTimeout(), MILLISECONDS),
                IgniteCheckedException.class,
                "Cluster version finalization process is already in progress"
            );

            checkJoinFailed(4, TEST_DEFAULT_VER, "Node joins are not allowed during cluster version finalization");

            spi(grid(2)).stopBlock();

            firstFut.get(getTestTimeout(), MILLISECONDS);
        }
        finally {
            discoveryRingMessageWorkerQueue(grid(0)).unblock();
            spi(grid(2)).stopBlock();
        }
    }

    /** */
    @Test
    public void testClientNodeDisconnectDuringFinalization() throws Exception {
        startCluster();

        ru(2).enableVersionUpgrade();

        spi(grid(2)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        try {
            IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> ru(2).finalizeClusterVersion());

            spi(grid(2)).waitForBlocked();

            grid(0).context().discovery().failNode(grid(2).context().localNodeId(), "test");

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> finalizeFut.get(getTestTimeout(), MILLISECONDS),
                IgniteException.class,
                "Client node has disconnected");

            assertTrue(waitForCondition(() -> !ru(1).isVersionUpgradeEnabled(), getTestTimeout()));
        }
        finally {
            spi(grid(2)).stopBlock();
        }
    }

    /** */
    @Test
    public void testClientNodeClearsActiveFinalizationProcessOnDisconnect() throws Exception {
        startCluster();
        wrapRingMessageWorkerQueue(startClientGrid(3, TEST_DEFAULT_VER));

        CountDownLatch latch = new CountDownLatch(1);

        grid(3).events().localListen(evt -> {
            latch.countDown();

            return true;
        }, EVT_CLIENT_NODE_RECONNECTED);

        ru(1).enableVersionUpgrade();

        spi(grid(3)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> ru(1).finalizeClusterVersion());

        spi(grid(3)).waitForBlocked();

        discoveryRingMessageWorkerQueue(grid(3)).block();

        grid(0).context().discovery().failNode(grid(3).context().localNodeId(), "test");

        finalizeFut.get(getTestTimeout(), MILLISECONDS);

        spi(grid(3)).stopBlock();
        discoveryRingMessageWorkerQueue(grid(3)).unblock();

        assertTrue(latch.await(getTestTimeout(), MILLISECONDS));

        ru(1).enableVersionUpgrade();

        finalizeClusterVersion(1, TEST_DEFAULT_VER);
    }

    /** */
    @Test
    public void testFailedNodeDoesNotAffectFinalization() throws Exception {
        startCluster();

        ru(1).enableVersionUpgrade();

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.1"));

        IgniteConfiguration cfg = getConfiguration(3, TEST_DEFAULT_VER)
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGrid(cfg),
            IgniteSpiException.class,
            "Local node's binary configuration is not equal to remote node's binary configuration"
        );

        finalizeClusterVersion(2, "2.19.1");
    }

    /** */
    @Test
    public void testAbortNoFinalizationInProgress() throws Exception {
        startCluster();

        ru(1).abortClusterVersionFinalization();

        ru(2).enableVersionUpgrade();

        ru(0).abortClusterVersionFinalization();

        startGrid(3);

        finalizeClusterVersion(2, TEST_DEFAULT_VER);
    }

    /** */
    @Test
    public void testConcurrentAbortAndPrepareFinalization() throws Exception {
        wrapRingMessageWorkerQueue(startGrid(0, TEST_DEFAULT_VER));
        startGrid(1, TEST_DEFAULT_VER);
        startClientGrid(2, TEST_DEFAULT_VER);

        ru(2).enableVersionUpgrade();

        discoveryRingMessageWorkerQueue(grid(0)).block();

        IgniteInternalFuture<Object> abortFut = GridTestUtils.runAsync(() -> ru(0).abortClusterVersionFinalization());

        waitForBlockedDiscoveryMessages(grid(0), 1, InitMessage.class);

        IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> ru(1).finalizeClusterVersion());

        waitForBlockedDiscoveryMessages(grid(0), 2, InitMessage.class);

        discoveryRingMessageWorkerQueue(grid(0)).unblock();

        abortFut.get(getTestTimeout(), MILLISECONDS);
        finalizeFut.get(getTestTimeout(), MILLISECONDS);
    }

    /** */
    @Test
    public void testAbortAfterFinalizationPrepare() throws Exception {
        wrapRingMessageWorkerQueue(startGrid(0, TEST_DEFAULT_VER));
        startGrid(1, TEST_DEFAULT_VER);
        startClientGrid(2, TEST_DEFAULT_VER);

        ru(2).enableVersionUpgrade();

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> ru(1).finalizeClusterVersion());

        spi(grid(1)).waitForBlocked();

        discoveryRingMessageWorkerQueue(grid(0)).block();

        IgniteInternalFuture<Object> abortFut = GridTestUtils.runAsync(() -> ru(2).abortClusterVersionFinalization());

        waitForBlockedDiscoveryMessages(grid(0), 1, InitMessage.class);

        discoveryRingMessageWorkerQueue(grid(0)).unblock();
        spi(grid(1)).stopBlock();

        abortFut.get(getTestTimeout(), MILLISECONDS);

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> finalizeFut.get(getTestTimeout(), MILLISECONDS),
            IgniteException.class,
            "Operation has been aborted by administrator");

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        startGrid(3, TEST_DEFAULT_VER);

        finalizeClusterVersion(2, TEST_DEFAULT_VER);
    }

    /** */
    @Test
    public void testAbortBeforeFinalizationComplete() throws Exception {
        wrapRingMessageWorkerQueue(startGrid(0, TEST_DEFAULT_VER));
        startGrid(1, TEST_DEFAULT_VER);
        startClientGrid(2, TEST_DEFAULT_VER);

        ru(2).enableVersionUpgrade();

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> ru(1).finalizeClusterVersion());

        spi(grid(1)).waitForBlocked();

        UUID activeFinalizeProcId = extractActiveFinalizeProcessId(1);

        CountDownLatch finalizeCompleteStartedLatch = new CountDownLatch(1);
        AtomicReference<TcpDiscoveryAbstractMessage> finalizeCompleteStartMsg = new AtomicReference<>();

        discoveryRingMessageWorkerQueue(grid(0)).startMessageIntercepting(m -> {
            if ((m instanceof TcpDiscoveryCustomEventMessage customMsg)
                && (customMsg.message() instanceof InitMessage<?> initMsg)
                && initMsg.processId().equals(activeFinalizeProcId)
            ) {
                finalizeCompleteStartMsg.set(m);
                finalizeCompleteStartedLatch.countDown();

                return false;
            }

            return true;
        });

        spi(grid(1)).stopBlock();

        assertTrue(finalizeCompleteStartedLatch.await(getTestTimeout(), MILLISECONDS));

        discoveryRingMessageWorkerQueue(grid(0)).block();

        IgniteInternalFuture<Object> abortFut = GridTestUtils.runAsync(() -> ru(2).abortClusterVersionFinalization());

        waitForBlockedDiscoveryMessages(grid(0), 1, InitMessage.class);

        discoveryRingMessageWorkerQueue(grid(0)).addLast(finalizeCompleteStartMsg.get());

        discoveryRingMessageWorkerQueue(grid(0)).unblock();

        abortFut.get(getTestTimeout(), MILLISECONDS);

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> finalizeFut.get(getTestTimeout(), MILLISECONDS),
            IgniteException.class,
            "Operation has been aborted by administrator");

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        startGrid(3, TEST_DEFAULT_VER);

        finalizeClusterVersion(2, TEST_DEFAULT_VER);
    }

    /** */
    @Test
    public void testAbortedAfterFinalizationComplete() throws Exception {
        wrapRingMessageWorkerQueue(startGrid(0, TEST_DEFAULT_VER));
        startGrid(1, TEST_DEFAULT_VER);
        startGrid(2, TEST_DEFAULT_VER);
        startClientGrid(3, TEST_DEFAULT_VER);

        ru(2).enableVersionUpgrade();

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> ru(1).finalizeClusterVersion());

        spi(grid(1)).waitForBlocked();

        discoveryRingMessageWorkerQueue(grid(0)).block();

        spi(grid(1)).stopBlock();

        waitForBlockedDiscoveryMessages(grid(0), 1, FullMessage.class);

        spi(grid(1)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        discoveryRingMessageWorkerQueue(grid(0)).unblock();

        spi(grid(1)).waitForBlocked();

        IgniteInternalFuture<Object> abortFut = GridTestUtils.runAsync(() -> ru(2).abortClusterVersionFinalization());

        spi(grid(1)).stopBlock();

        abortFut.get(getTestTimeout(), MILLISECONDS);

        finalizeFut.get(getTestTimeout(), MILLISECONDS);

        checkVersionUpgradeInactive(TEST_DEFAULT_VER);

        ru(2).enableVersionUpgrade();

        startGrid(4, TEST_DEFAULT_VER);

        finalizeClusterVersion(2, TEST_DEFAULT_VER);
    }

    /** */
    @Test
    public void testAbortedAfterFinalizationFinished() throws Exception {
        startCluster();

        ru(2).enableVersionUpgrade();

        finalizeClusterVersion(2, TEST_DEFAULT_VER);

        ru(1).abortClusterVersionFinalization();

        ru(2).enableVersionUpgrade();

        startGrid(3, TEST_DEFAULT_VER);

        finalizeClusterVersion(2, TEST_DEFAULT_VER);
    }

    /** */
    @Test
    public void testConcurrentNodeJoinAndFinalization() throws Exception {
        wrapRingMessageWorkerQueue(startGrid(0, TEST_DEFAULT_VER));
        startGrid(1, TEST_DEFAULT_VER);
        startClientGrid(2, TEST_DEFAULT_VER);

        ru(1).enableVersionUpgrade();

        TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch = new CountDownLatch(1);
        TestRollingUpgradeProcessor.nodeJoinUnblockedLatch = new CountDownLatch(1);

        try {
            IgniteInternalFuture<IgniteEx> startFut = GridTestUtils.runAsync(() -> startGrid(3, TEST_DEFAULT_VER));

            assertTrue(TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch.await(getTestTimeout(), MILLISECONDS));

            discoveryRingMessageWorkerQueue(grid(0)).block();

            IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(1, TEST_DEFAULT_VER));

            waitForBlockedDiscoveryMessages(grid(0), 1, InitMessage.class);

            TestRollingUpgradeProcessor.nodeJoinUnblockedLatch.countDown();

            waitForBlockedDiscoveryMessages(grid(0), 1, TcpDiscoveryNodeAddedMessage.class);

            discoveryRingMessageWorkerQueue(grid(0)).unblock();

            finalizeFut.get(getTestTimeout(), MILLISECONDS);

            startFut.get(getTestTimeout(), MILLISECONDS);
        }
        finally {
            TestRollingUpgradeProcessor.nodeJoinUnblockedLatch.countDown();
        }
    }

    /** */
    @Test
    public void testConcurrentFinalizationAndNodeJoin() throws Exception {
        wrapRingMessageWorkerQueue(startGrid(0, TEST_DEFAULT_VER));
        startGrid(1, TEST_DEFAULT_VER);
        startClientGrid(2, TEST_DEFAULT_VER);

        ru(1).enableVersionUpgrade();

        discoveryRingMessageWorkerQueue(grid(0)).block();

        IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> ru(1).finalizeClusterVersion());

        waitForBlockedDiscoveryMessages(grid(0), 1, InitMessage.class);

        IgniteInternalFuture<IgniteEx> startFut = GridTestUtils.runAsync(() -> startGrid(3, TEST_DEFAULT_VER));

        waitForBlockedDiscoveryMessages(grid(0), 1, TcpDiscoveryJoinRequestMessage.class);

        discoveryRingMessageWorkerQueue(grid(0)).unblock();

        finalizeFut.get(getTestTimeout(), MILLISECONDS);

        try {
            startFut.get(getTestTimeout(), MILLISECONDS);
        }
        catch (IgniteCheckedException e) {
            assertTrue(X.hasCause(e, "Node joins are not allowed during cluster version finalization", IgniteSpiException.class));
        }
    }

    /** */
    private void waitForBlockedDiscoveryMessages(
        IgniteEx ignite,
        int expCnt,
        Class<?> msgCls
    ) throws Exception {
        assertTrue(waitForCondition(() -> queuedDiscoveryMessageCountMatches(ignite, expCnt, msgCls), getTestTimeout()));
    }

    /** */
    private boolean queuedDiscoveryMessageCountMatches(IgniteEx ignite, int expCnt, Class<?> msgCls) {
        int cnt = 0;

        for (TcpDiscoveryAbstractMessage msg : discoveryRingMessageWorkerQueue(ignite)) {
            Class<?> queuedMsgCls = (msg instanceof TcpDiscoveryCustomEventMessage customMsg)
                ? customMsg.message().getClass()
                : msg.getClass();

            if (msgCls.isAssignableFrom(queuedMsgCls))
                cnt++;
        }

        return expCnt == cnt;
    }

    /** */
    private void startCluster() throws Exception {
        startCluster(TEST_DEFAULT_VER);
    }

    /** */
    private void startCluster(String ver) throws Exception {
        startGrid(0, ver);
        startGrid(1, ver);
        startClientGrid(2, ver);
    }

    /** */
    private UUID extractActiveFinalizeProcessId(int nodeIdx) {
        Object enable = U.field(grid(nodeIdx).context().rollingUpgrade(), "finalizeProc");

        return U.field(enable, "locInitOpId");
    }

    /** */
    private void checkVersionFinalizationFailed(int initiatorNodeIdx, String msg) {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                ru(initiatorNodeIdx).finalizeClusterVersion();

                return null;
            },
            IgniteCheckedException.class,
            msg
        );
    }

    /** */
    private void checkJoinSuccess(int nodeIdx, boolean isClient) throws Exception {
        checkJoinSuccess(nodeIdx, TEST_DEFAULT_VER, isClient);
    }

    /** */
    private void checkJoinSuccess(int nodeIdx, String ver, boolean isClient) throws Exception {
        int clusterSize = clusterNode().cluster().nodes().size();

        startGrid(nodeIdx, ver, isClient);

        assertEquals(clusterSize + 1, clusterNode().cluster().nodes().size());
    }

    /** */
    private void checkJoinFailed(int nodeIdx, String ver, String msg) {
        int expClusterSize = clusterNode().cluster().nodes().size();

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(nodeIdx, ver ), IgniteSpiException.class, msg);
        GridTestUtils.assertThrowsAnyCause(log, () -> startClientGrid(nodeIdx, ver), IgniteSpiException.class, msg);

        assertEquals(expClusterSize, clusterNode().cluster().nodes().size());
    }

    /** */
    private IgniteEx clusterNode() {
        return (IgniteEx)Ignition.allGrids().stream().filter(i -> !i.cluster().localNode().isClient()).findFirst().orElseThrow();
    }

    /** */
    private void checkVersionUpgradeInProgress(String srcVer, String targetVer) throws Exception {
        checkVersionUpgradeInProgress(srcVer, srcVer, targetVer);
    }

    /** */
    private void checkVersionUpgradeInProgress(String logicalVer, String srcVer, String targetVer) throws Exception {
        checkVersionUpgradeEnabledStatus(true);
        checkRollingUpgradeState(logicalVer, srcVer, targetVer);

        checkProductFeaturesActive(logicalVer);

        if (targetVer != null)
            checkFeaturesNotActive(newFeatures(logicalVer, targetVer));
    }

    /** */
    private void checkVersionUpgradeInactive(String expVer) throws Exception {
        checkVersionUpgradeEnabledStatus(false);
        checkProductFeaturesActive(expVer);
    }

    /** */
    private void checkVersionUpgradeEnabledStatus(boolean enabled) {
        List<Ignite> cluster = Ignition.allGrids();

        for (Ignite ignite : cluster)
            assertEquals(enabled, ru(ignite).isVersionUpgradeEnabled());
    }

    /** */
    private void checkProductFeaturesActive(String ver) throws Exception {
        Collection<IgniteFeature> features = readDeclaredFeatures(ver);

        List<Ignite> cluster = Ignition.allGrids();

        AtomicInteger validatedFeatures = new AtomicInteger();

        for (Ignite ignite : cluster) {
            for (IgniteFeature feature : features) {
                assertTrue(isFeatureActive(ignite, feature));
                listenFeatureActivation(ignite, feature, validatedFeatures::incrementAndGet);
            }
        }

        assertEquals(cluster.size() * features.size(), validatedFeatures.get());
    }

    /** */
    private void checkFeaturesNotActive(Collection<IgniteFeature> features) {
        assertFalse(F.isEmpty(features));

        for (Ignite ignite : Ignition.allGrids()) {
            for (IgniteFeature feature : features)
                assertFalse(isFeatureActive(ignite, feature));
        }
    }

    /** */
    private Collection<IgniteFeature> newFeatures(String srcVer, String targetVer) throws Exception {
        Collection<IgniteFeature> srcFeatures = readDeclaredFeatures(srcVer);
        Collection<IgniteFeature> targetFeatures = readDeclaredFeatures(targetVer);

        List<IgniteFeature> res = new ArrayList<>();

        for (IgniteFeature targetFeature : targetFeatures) {
            if (!srcFeatures.contains(targetFeature))
                res.add(targetFeature);
        }

        return res;
    }

    /** */
    private void checkFeatureActivationSubscription(int nodeIdx, IgniteFeature feature, CountDownLatch featureActivationLatch) {
        long expCnt = featureActivationLatch.getCount();

        assertFalse(isFeatureActive(grid(nodeIdx), feature));
        listenFeatureActivation(grid(nodeIdx), feature, featureActivationLatch::countDown);
        assertEquals(expCnt, featureActivationLatch.getCount());
    }

    /** */
    private void upgradeNodeVersion(int nodeIdx, String targetVer) throws Exception {
        upgradeNodeVersion(nodeIdx, TEST_DEFAULT_VER, targetVer);
    }

    /** */
    private void upgradeNodeVersion(int nodeIdx, String srcVer, String targetVer) throws Exception {
        upgradeNodeVersion(nodeIdx, srcVer, srcVer, targetVer);
    }

    /** */
    private void upgradeNodeVersion(int nodeIdx, String logicalVer, String srcVer, String targetVer) throws Exception {
        boolean isClient = grid(nodeIdx).context().clientNode();

        stopGrid(nodeIdx);
        checkJoinSuccess(nodeIdx, targetVer, isClient);
        checkVersionUpgradeInProgress(logicalVer, srcVer, targetVer);
    }

    /** */
    private void finalizeClusterVersion(int nodeIdx, String expVer) throws Exception {
        ru(nodeIdx).finalizeClusterVersion();

        checkVersionUpgradeInactive(expVer);
    }

    /** */
    private void restartNode(int nodeIdx) throws Exception {
        String ver = semanticVersion(grid(nodeIdx).context().discovery().localNode().version());
        boolean isClient = grid(nodeIdx).context().clientNode();

        stopGrid(nodeIdx);
        checkJoinSuccess(nodeIdx, ver, isClient);
    }

    /** */
    private void forAllNodes(ConsumerX<Integer> nodeProcessor) throws Exception {
        for (Ignite ignite : Ignition.allGrids())
            nodeProcessor.accept(getTestIgniteInstanceIndex(ignite.name()));
    }

    /** */
    private void checkUpgradeFailed(int nodeIdx, String targetVer, String errMsg) throws Exception {
        String srcVer = semanticVersion(grid(nodeIdx).context().discovery().localNode().version());
        boolean isClient = grid(nodeIdx).context().clientNode();

        stopGrid(nodeIdx);

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(nodeIdx, targetVer, isClient), IgniteSpiException.class, errMsg);

        startGrid(nodeIdx, srcVer, isClient);
    }

    /** */
    private void checkRollingUpgradeState(String expLogicalVer, String expSrcVer, String expTargetVer) {
        for (Ignite ignite : Ignition.allGrids()) {
            assertTrue(ru(ignite).isVersionUpgradeEnabled());
            assertEquals(expLogicalVer, semanticVersion(ru(ignite).features().activeFeatures().version()));

            RollingUpgradeState state = ru(ignite).state();

            assertEquals(expSrcVer, semanticVersion(state.srcVer));
            assertEquals(expTargetVer, state.targetVer == null ? null : semanticVersion(state.targetVer));
        }
    }
    
    /** */
    private RollingUpgradeProcessor ru(int nodeIdx) {
        return ru(grid(nodeIdx));
    }

    /** */
    private static RollingUpgradeProcessor ru(Ignite ignite) {
        return ((IgniteEx)ignite).context().rollingUpgrade();
    }

    /** */
    private int coordinatorIndex() {
        for (Ignite grid : IgnitionEx.allGridsx()) {
            if (U.isLocalNodeCoordinator(((IgniteEx)grid).context().discovery()))
                return getTestIgniteInstanceIndex(grid.name());
        }

        fail("Failed to resolve coordinator node");

        return -1;
    }
}
