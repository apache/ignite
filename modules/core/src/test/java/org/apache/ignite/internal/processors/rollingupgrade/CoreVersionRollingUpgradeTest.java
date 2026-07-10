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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
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
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_19_2.VER_2_19_2_ID_1_FEATURE;
import static org.apache.ignite.internal.processors.security.NodeSecurityContextPropagationTest.discoveryRingMessageWorkerQueue;
import static org.apache.ignite.internal.processors.security.NodeSecurityContextPropagationTest.wrapRingMessageWorkerQueue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CoreVersionRollingUpgradeTest extends AbstractRollingUpgradeTest {
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

        String msg = VER_NOT_EQUAL_ERR;

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

        checkJoinFailed(5, "2.18.0", VER_INCOMPATIBLE_ERR);

        upgradeNodeVersion(0, "2.19.1");
        upgradeNodeVersion(2, "2.19.1");

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, "2.19.1");

        checkJoinFailed(5, "2.19.2", VER_INCOMPATIBLE_ERR);

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

        checkUpgradeFailed(2, "2.19.3", VER_INCOMPATIBLE_ERR);
        checkUpgradeFailed(1, "2.19.3", VER_INCOMPATIBLE_ERR);

        upgradeNodeVersion(3, "2.19.2");
        upgradeNodeVersion(4, "2.19.2");
        checkJoinSuccess(0, "2.19.3", false);

        checkUpgradeFailed(2, TEST_DEFAULT_VER, VER_INCOMPATIBLE_ERR);
        checkUpgradeFailed(1, TEST_DEFAULT_VER, VER_INCOMPATIBLE_ERR);

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

        checkUpgradeFailed(1, "2.19.2", VER_INCOMPATIBLE_ERR);
        checkUpgradeFailed(2, "2.19.2", VER_INCOMPATIBLE_ERR);

        upgradeNodeVersion(1, "2.19.3");
        upgradeNodeVersion(2, "2.19.3");

        finalizeClusterVersion(1, "2.19.3");
    }

    /** */
    @Test
    public void testUpgradeBetweenVersionsWithCherryPicks() throws Exception {
        startCluster("2.19.3");

        ru(1).enableVersionUpgrade();

        checkJoinFailed(3, "2.20.0", RU_UNAVAILABLE_BETWEEN_VER_ERR);

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

            UUID startedFinalizeProcId = extractFinalizeProcessId(1);

            spi(grid(2)).blockMessages((node, msg) -> {
                if (!(msg instanceof SingleNodeMessage<?> singleNodeMsg))
                    return false;

                return singleNodeMsg.processId().equals(startedFinalizeProcId);
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

        UUID startedFinalizeProcId = extractFinalizeProcessId(1);

        CountDownLatch finalizeCompleteStartedLatch = new CountDownLatch(1);
        AtomicReference<TcpDiscoveryAbstractMessage> finalizeCompleteStartMsg = new AtomicReference<>();

        discoveryRingMessageWorkerQueue(grid(0)).startMessageIntercepting(m -> {
            if ((m instanceof TcpDiscoveryCustomEventMessage customMsg)
                && (customMsg.message() instanceof InitMessage<?> initMsg)
                && initMsg.processId().equals(startedFinalizeProcId)
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
}
