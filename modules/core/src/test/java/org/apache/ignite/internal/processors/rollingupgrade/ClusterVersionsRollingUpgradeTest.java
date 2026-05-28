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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rollingupgrade.feature.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_VALIDATION_FAILED;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_19_2.VER_2_19_2_ID_1_FEATURE;
import static org.apache.ignite.internal.processors.security.NodeSecurityContextPropagationTest.discoveryRingMessageWorker;

/** */
public class ClusterVersionsRollingUpgradeTest extends AbstractRollingUpgradeTest {
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

        checkVersionUpgradeInactive("2.19.0");

        String msg = "The joining node version differs from the version of the cluster";

        checkJoinFailed(3, "2.18.0", msg);
        checkJoinFailed(3, "2.19.1", msg);
    }

    /** */
    @Test
    public void testVersionsFinalizationNoTopologyChanges() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        grid(1).context().rollingUpgrade().finalizeClusterVersion();

        checkVersionUpgradeInactive(TEST_DEFAULT_VER);

        restartNode(0);
    }

    /** */
    @Test
    public void testDoubledControlCommands() throws Exception {
        startCluster();

        grid(0).context().rollingUpgrade().enableVersionUpgrade();
        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        grid(0).context().rollingUpgrade().finalizeClusterVersion();
        grid(0).context().rollingUpgrade().finalizeClusterVersion();

        checkVersionUpgradeInactive(TEST_DEFAULT_VER);
    }

    /** */
    @Test
    public void testVersionUpgradeEnabledNodeJoin() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        restartNode(2);
        restartNode(0);

        checkJoinSuccess(3, false);

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        checkJoinFailed(6, "2.18.0",
            "The joining node is incompatible with the current state of the cluster version rolling upgrade being in progress");

        upgradeNodeVersion(0, "2.19.1");
        upgradeNodeVersion(2, "2.19.1");
        checkJoinSuccess(4, "2.19.1", false);
        checkJoinSuccess(5, "2.19.1", true);

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, "2.19.2");

        checkJoinFailed(6, "2.19.2",
            "The joining node is incompatible with the current state of the cluster version rolling upgrade being in progress");

        restartNode(1);
        checkJoinSuccess(7, false);
        checkJoinSuccess(8, true);
        checkJoinSuccess(9, "2.19.1", false);
        checkJoinSuccess(10, "2.19.1", true);
    }

    /** */
    @Test
    public void testClusterVersionUpgrade() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        upgradeNodeVersion(1, "2.19.2");

        checkVersionFinalizationFailed(
            1,
            "Cluster version finalization failed. The topology contains nodes running multiple different versions"
        );

        upgradeNodeVersion(0, "2.19.2");

        checkVersionFinalizationFailed(
            1,
            "Cluster version finalization failed. The topology contains nodes running multiple different versions"
        );

        upgradeNodeVersion(2, "2.19.2");

        finalizeClusterVersion(1, "2.19.2");
    }

    /** */
    @Test
    public void testJoinAfterClusterVersionFinalization() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

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

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

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

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2");
        upgradeNodeVersion(1, "2.19.2");
        upgradeNodeVersion(2, "2.19.2");

        upgradeNodeVersion(0, "2.19.3");

        checkUpgradeFailed(2, TEST_DEFAULT_VER,
            "The joining node is incompatible with the current state of the cluster version rolling upgrade being in progress");
        checkUpgradeFailed(1, TEST_DEFAULT_VER,
            "The joining node is incompatible with the current state of the cluster version rolling upgrade being in progress");

        upgradeNodeVersion(2, "2.19.3");
        upgradeNodeVersion(1, "2.19.3");

        finalizeClusterVersion(1, "2.19.3");
    }

    /** */
    @Test
    public void testMultipleClusterVersionUpgrades() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();
        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.2"));
        finalizeClusterVersion(1, "2.19.2");

        grid(1).context().rollingUpgrade().enableVersionUpgrade();
        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.20.0"));
        finalizeClusterVersion(1, "2.20.0");

        grid(1).context().rollingUpgrade().enableVersionUpgrade();
        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.21.0"));
        finalizeClusterVersion(1, "2.21.0");
    }

    /** */
    @Test
    public void testProductVersionChangeDuringClusterVersionUpgrade() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2");
        upgradeNodeVersion(0, "2.19.3");

        checkUpgradeFailed(1, "2.19.2",
            "The joining node is incompatible with the current state of the cluster version rolling upgrade being in progress");
        checkUpgradeFailed(2, "2.19.2",
            "The joining node is incompatible with the current state of the cluster version rolling upgrade being in progress");

        upgradeNodeVersion(1, "2.19.3");
        upgradeNodeVersion(2, "2.19.3");

        finalizeClusterVersion(1, "2.19.3");
    }

    /** */
    @Test
    public void testUpgradeBetweenVersionsWithCherryPicks() throws Exception {
        startCluster("2.19.3");

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        checkJoinFailed(3, "2.20.0",
            "Rolling Upgrade is not available between the current cluster logical version and the joining node product version");

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.20.1"));

        finalizeClusterVersion(1, "2.20.1");
    }

    /** */
    @Test
    public void testUpgradeBetweenVersionsWithCherryPicksAndContinuousRange() throws Exception {
        startCluster("2.20.1");

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.21.1"));

        finalizeClusterVersion(1, "2.21.1");
    }

    /** */
    @Test
    public void testConcurrentFinalizationFromDifferentNodes() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

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
                "Cluster version finalization procedure is already in progress"
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

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

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
    public void testConcurrentNodeJoinAndFinalization() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

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
    public void testJoiningNodesAccountedDuringFinalization() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch = new CountDownLatch(1);
        TestRollingUpgradeProcessor.nodeJoinUnblockedLatch = new CountDownLatch(1);

        try {
            IgniteInternalFuture<IgniteEx> startFut = GridTestUtils.runAsync(() -> startGrid(3, "2.19.2", false));

            assertTrue(TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch.await(getTestTimeout(), MILLISECONDS));

            IgniteInternalFuture<Object> finalizeFut = GridTestUtils.runAsync(() -> finalizeClusterVersion(1, TEST_DEFAULT_VER));

            LinkedBlockingDeque<TcpDiscoveryAbstractMessage> discoMsgQueue = U.field(discoveryRingMessageWorker(grid(0)), "queue");

            assertTrue(GridTestUtils.waitForCondition(
                () -> discoMsgQueue.stream().anyMatch(m ->
                    m instanceof TcpDiscoveryCustomEventMessage && ((TcpDiscoveryCustomEventMessage)m).message() instanceof InitMessage),
                getTestTimeout()));

            TestRollingUpgradeProcessor.nodeJoinUnblockedLatch.countDown();

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> finalizeFut.get(getTestTimeout(), MILLISECONDS),
                IgniteCheckedException.class,
                "Cluster version finalization failed. The topology contains nodes running multiple different versions"
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
    public void testNodeValidationFailed() throws Exception {
        startCluster();

        grid(1).context().rollingUpgrade().enableVersionUpgrade();

        CountDownLatch nodeValidationFailedEvtListenedLatch = new CountDownLatch(2);

        grid(0).events().localListen(evt -> {
            nodeValidationFailedEvtListenedLatch.countDown();

            return true;
        }, EVT_NODE_VALIDATION_FAILED);

        TestRollingUpgradeProcessor.isNodeValidationFailureImitationEnabled.set(true);

        checkJoinFailed(3, "2.19.2", "expected validation error");

        checkVersionUpgradeInProgress(TEST_DEFAULT_VER, null);

        TestRollingUpgradeProcessor.isNodeValidationFailureImitationEnabled.set(false);

        assertTrue(nodeValidationFailedEvtListenedLatch.await(getTestTimeout(), MILLISECONDS));

        forAllNodes(nodeIdx -> upgradeNodeVersion(nodeIdx, "2.19.3"));

        finalizeClusterVersion(1, "2.19.3");
    }

    /** */
    private void startCluster() throws Exception {
        startCluster(TEST_DEFAULT_VER);
    }

    /** */
    private void startCluster(String ver) throws Exception {
        startGrid(0, ver, false);
        startGrid(1, ver, false);
        startGrid(2, ver, true);
    }

    /** */
    private void checkVersionFinalizationFailed(int initiatorNodeIdx, String msg) {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                grid(initiatorNodeIdx).context().rollingUpgrade().finalizeClusterVersion();

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

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(nodeIdx, ver, false), IgniteSpiException.class, msg);
        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(nodeIdx, ver, true), IgniteSpiException.class, msg);

        assertEquals(expClusterSize, clusterNode().cluster().nodes().size());
    }

    /** */
    private IgniteEx clusterNode() {
        return (IgniteEx)Ignition.allGrids().stream().filter(i -> !i.cluster().localNode().isClient()).findFirst().orElseThrow();
    }

    /** */
    private void checkVersionUpgradeInProgress(String srcVer, String targetVer) throws Exception {
        checkVersionUpgradeEnabledStatus(true);
        checkProductFeaturesActive(srcVer);

        if (targetVer != null)
            checkFeaturesNotActive(newFeatures(srcVer, targetVer));
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
            assertEquals(enabled, ((IgniteEx)ignite).context().rollingUpgrade().isVersionUpgradeEnabled());
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
        String srcVer = grid(nodeIdx).context().rollingUpgrade().features().activeFeatures().version().shortName();
        boolean isClient = grid(nodeIdx).context().clientNode();

        stopGrid(nodeIdx);
        checkJoinSuccess(nodeIdx, targetVer, isClient);
        checkVersionUpgradeInProgress(srcVer, targetVer);
    }

    /** */
    private void finalizeClusterVersion(int nodeIdx, String expVer) throws Exception {
        grid(nodeIdx).context().rollingUpgrade().finalizeClusterVersion();

        checkVersionUpgradeInactive(expVer);
    }

    /** */
    private void restartNode(int nodeIdx) throws Exception {
        boolean isClient = grid(nodeIdx).context().clientNode();
        String ver = grid(nodeIdx).context().discovery().localNode().version().shortName();

        stopGrid(nodeIdx);
        checkJoinSuccess(nodeIdx, ver, isClient);
    }

    /** */
    public void forAllNodes(ConsumerX<Integer> nodeProcessor) throws Exception {
        for (Ignite ignite : Ignition.allGrids())
            nodeProcessor.accept(getTestIgniteInstanceIndex(ignite.name()));
    }

    /** */
    public void checkUpgradeFailed(int nodeIdx, String targetVer, String msg) throws Exception {
        String srcVer = grid(nodeIdx).context().discovery().localNode().version().shortName();
        boolean isClient = grid(nodeIdx).context().clientNode();

        stopGrid(nodeIdx);

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(nodeIdx, targetVer, isClient), IgniteSpiException.class, msg);

        startGrid(nodeIdx, srcVer, isClient);
    }
}
