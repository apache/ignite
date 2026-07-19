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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteComponentFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteComponentFeatureSetProvider;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteCoreFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteCoreFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestIgniteReleaseFeatures_2_18_0;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestPluginComponentFeatureSetProvider;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestPluginFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.TestPluginReleaseFeatures_1_0_0;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestBlockingTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jspecify.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_VALIDATION_FAILED;
import static org.apache.ignite.internal.IgniteVersionUtils.semanticVersion;

/**
 * Provides the ability to override a node's version and supported {@link IgniteFeature}s in order to
 * simulate a Rolling Upgrade procedure.
 *
 * <p>For testing purposes, the following "fake" Ignite versions and their corresponding
 * {@link IgniteFeature}s have been introduced. These versions are used solely for testing and do not
 * correspond to any actual Ignite releases.
 *
 * <table border="1">
 *   <tr>
 *     <th>Version</th>
 *     <th>Features</th>
 *   </tr>
 *   <tr>
 *     <td>2.18.0</td>
 *     <td>not supported</td>
 *   </tr>
 *   <tr>
 *     <td>2.19.0</td>
 *     <td>{@code IgniteFeatureSet [0]}</td>
 *   </tr>
 *   <tr>
 *     <td>2.19.1</td>
 *     <td>{@code IgniteFeatureSet [0, 1]}</td>
 *   </tr>
 *   <tr>
 *     <td>2.19.2</td>
 *     <td>{@code IgniteFeatureSet [0 -> 2]}</td>
 *   </tr>
 *   <tr>
 *     <td>2.19.3</td>
 *     <td>{@code IgniteFeatureSet [0 -> 2, 6]}</td>
 *   </tr>
 *   <tr>
 *     <td>2.20.0</td>
 *     <td>{@code IgniteFeatureSet [0 -> 4]}</td>
 *   </tr>
 *   <tr>
 *     <td>2.20.1</td>
 *     <td>{@code IgniteFeatureSet [0 -> 4, 6]}</td>
 *   </tr>
 *   <tr>
 *     <td>2.21.0</td>
 *     <td>{@code IgniteFeatureSet [3 -> 6]}</td>
 *   </tr>
 *   <tr>
 *     <td>2.21.1</td>
 *     <td>{@code IgniteFeatureSet [3 -> 7]}</td>
 *   </tr>
 * </table>
 */
public abstract class AbstractRollingUpgradeTest extends GridCommonAbstractTest {
    /** */
    protected static final String TEST_DEFAULT_VER = "2.19.0";

    /** */
    protected static final String VER_INCOMPATIBLE_ERR =
        "Joining node is not allowed to join the cluster because it is running a component with an incompatible version";

    /** */
    protected static final String MULTIPLE_VER_IN_TOP_ERR =
        "Cluster version finalization failed. The cluster contains nodes running different versions of one or more components";

    /** */
    protected static final String VER_NOT_EQUAL_ERR =
        "One or more component versions on the joining node differ from the corresponding versions active in the cluster";

    /** */
    protected static final String RU_UNAVAILABLE_BETWEEN_VER_ERR = "Ignite component Rolling Upgrade is not supported" +
        " between the component version active in the cluster and the version running on the joining node";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return getConfiguration(igniteInstanceName, TEST_DEFAULT_VER);
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch = null;
        TestRollingUpgradeProcessor.nodeJoinUnblockedLatch = null;
        TestNodeValidationFailedEventListener.nodeValidationFailedEventUnblockedLatch = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    protected IgniteConfiguration getConfiguration(int idx, String ver) throws Exception {
        return getConfiguration(getTestIgniteInstanceName(idx), ver);
    }

    /** */
    protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        TestVersions testVersions = TestVersions.parse(ver);

        IgniteCoreFeatureSet testCoreFeatures = new IgniteCoreFeatureSet(
            IgniteProductVersion.fromString(testVersions.coreVersion()),
            IgniteFeatureSet.buildFrom(readDeclaredCoreFeatures(testVersions.coreVersion()))
        );

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "test-rolling-upgrade-processor-provider";
            }

            /** {@inheritDoc} */
            @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
                if (testVersions.containsPlugin()) {
                    registry.registerExtension(
                        IgniteComponentFeatureSetProvider.class,
                        new TestPluginComponentFeatureSetProvider(testVersions.pluginVersion()));
                }
            }

            /** {@inheritDoc} */
            @Override public @Nullable <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (cls.isAssignableFrom(DiscoveryNodeValidationProcessor.class))
                    return (T)new TestRollingUpgradeProcessor(((IgniteEx)ctx.grid()).context(), testCoreFeatures);

                return null;
            }
        });

        TcpDiscoverySpi discoSpi = new TestBlockingTcpDiscoverySpi(cfg) {
            @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ignored) {
                super.setNodeAttributes(attrs, IgniteProductVersion.fromString(testVersions.coreVersion()));
            }
        };

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** */
    protected IgniteEx startGrid(int idx, String ver) throws Exception {
        return startGrid(getConfiguration(idx, ver));
    }

    /** */
    protected IgniteEx startClientGrid(int idx, String ver) throws Exception {
        return startClientGrid(getConfiguration(idx, ver));
    }

    /** */
    protected IgniteEx startGrid(int idx, String ver, boolean isClient) throws Exception {
        return isClient ? startClientGrid(idx, ver) : startGrid(idx, ver);
    }

    /** */
    public static Collection<IgniteFeature> readDeclaredCoreFeatures(String ver) throws Exception {
        Class<?> cls = Class.forName(
            TestIgniteReleaseFeatures_2_18_0.class.getPackageName() + ".TestIgniteReleaseFeatures_" + ver.replace(".", "_"));

        return IgniteFeatureSet.readDeclaredFeatures(cls);
    }

    /** */
    public static Collection<IgniteFeature> readDeclaredPluginFeatures(String ver) throws Exception {
        Class<?> cls = Class.forName(
            TestPluginReleaseFeatures_1_0_0.class.getPackageName() + ".TestPluginReleaseFeatures_" + ver.replace(".", "_"));

        return IgniteFeatureSet.readDeclaredFeatures(cls);
    }

    /** */
    protected boolean isFeatureActive(Ignite ignite, IgniteFeature feature) {
        return ((IgniteEx)ignite).context().rollingUpgrade().features().isActive(feature);
    }

    /** */
    protected void listenFeatureActivation(Ignite ignite, IgniteFeature feature, IgniteRunnable lsnr) {
        ((IgniteEx)ignite).context().rollingUpgrade().features().listenActivation(feature, lsnr);
    }

    /** */
    protected void startCluster() throws Exception {
        startCluster(TEST_DEFAULT_VER);
    }

    /** */
    protected void startCluster(String ver) throws Exception {
        startGrid(0, ver);
        startGrid(1, ver);
        startClientGrid(2, ver);

        checkVersionUpgradeInactive(ver);
    }

    /** */
    protected UUID extractFinalizeProcessId(int nodeIdx) {
        Object proc = U.field(grid(nodeIdx).context().rollingUpgrade(), "finalizeProc");

        return U.field(proc, "locInitOpId");
    }

    /** */
    protected void checkVersionFinalizationFailed(int initiatorNodeIdx, String msg) {
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
    protected void checkJoinSuccess(int nodeIdx, boolean isClient) throws Exception {
        checkJoinSuccess(nodeIdx, TEST_DEFAULT_VER, isClient);
    }

    /** */
    protected void checkJoinSuccess(int nodeIdx, String ver, boolean isClient) throws Exception {
        int clusterSize = clusterNode().cluster().nodes().size();

        startGrid(nodeIdx, ver, isClient);

        assertEquals(clusterSize + 1, clusterNode().cluster().nodes().size());
    }

    /** */
    protected void checkJoinFailed(int nodeIdx, String ver, String msg) {
        checkJoinFailed(nodeIdx, ver, true, msg);
    }

    /** */
    protected void checkJoinFailed(int nodeIdx, String ver, boolean checkClientNode, String msg) {
        int expClusterSize = clusterNode().cluster().nodes().size();

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(nodeIdx, ver), IgniteSpiException.class, msg);

        if (checkClientNode)
            GridTestUtils.assertThrowsAnyCause(log, () -> startClientGrid(nodeIdx, ver), IgniteSpiException.class, msg);

        assertEquals(expClusterSize, clusterNode().cluster().nodes().size());
    }

    /** */
    protected IgniteEx clusterNode() {
        return (IgniteEx)Ignition.allGrids().stream().filter(i -> !i.cluster().localNode().isClient()).findFirst().orElseThrow();
    }

    /** */
    protected void checkVersionUpgradeInProgress(String srcVer, String targetVer) throws Exception {
        checkVersionUpgradeInProgress(srcVer, srcVer, targetVer);
    }

    /** */
    protected void checkVersionUpgradeInProgress(String logicalVer, String srcVer, String targetVer) throws Exception {
        checkVersionUpgradeEnabledStatus(true);
        checkRollingUpgradeState(logicalVer, srcVer, targetVer);

        checkFeaturesActive(logicalVer);

        TestVersions logicalVersions = TestVersions.parse(logicalVer);
        TestVersions targetVersions = TestVersions.parse(targetVer);

        checkFeaturesNotActive(newFeatures(
            logicalVersions.coreVersion() == null ? Collections.emptyList() : readDeclaredCoreFeatures(logicalVersions.coreVersion()),
            targetVersions.coreVersion() == null ? Collections.emptyList() : readDeclaredCoreFeatures(targetVersions.coreVersion()))
        );

        checkFeaturesNotActive(newFeatures(
            logicalVersions.containsPlugin() ? readDeclaredPluginFeatures(logicalVersions.pluginVersion()) : Collections.emptyList(),
            targetVersions.containsPlugin() ? readDeclaredPluginFeatures(targetVersions.pluginVersion()) : Collections.emptyList())
        );
    }

    /** */
    protected void checkVersionUpgradeInactive(String expVer) throws Exception {
        checkVersionUpgradeEnabledStatus(false);
        checkVersionUpgradeFutureCompleted();
        checkFeaturesActive(expVer);
    }

    /** */
    protected void checkVersionUpgradeFutureCompleted() {
        for (Ignite ignite : Ignition.allGrids())
            assertTrue(ru(ignite).features().isLocalVersionFeaturesActive());
    }

    /** */
    protected void checkVersionUpgradeEnabledStatus(boolean enabled) {
        for (Ignite ignite : Ignition.allGrids())
            assertEquals(enabled, ru(ignite).isVersionUpgradeEnabled());
    }

    /** */
    protected void checkFeaturesActive(String ver) throws Exception {
        TestVersions versions = TestVersions.parse(ver);

        checkFeaturesActive(readDeclaredCoreFeatures(versions.coreVersion()));

        if (versions.containsPlugin())
            checkFeaturesActive(readDeclaredPluginFeatures(versions.pluginVersion()));
    }

    /** */
    protected void checkFeaturesActive(Collection<IgniteFeature> features) {
        List<Ignite> cluster = Ignition.allGrids();

        AtomicInteger activationNotifiedCnt = new AtomicInteger();
        int expFeaturesCnt = 0;

        for (Ignite ignite : cluster) {
            for (IgniteFeature feature : features) {
                if (
                    ignite.configuration().isClientMode()
                    && !ru(ignite).features().localVersionFeatures().components().contains(feature.componentName())
                )
                    continue;

                assertTrue(isFeatureActive(ignite, feature));
                listenFeatureActivation(ignite, feature, activationNotifiedCnt::incrementAndGet);

                ++expFeaturesCnt;
            }
        }

        assertEquals(expFeaturesCnt, activationNotifiedCnt.get());
    }

    /** */
    protected void checkFeaturesNotActive(Collection<IgniteFeature> features) {
        if (F.isEmpty(features))
            return;

        for (Ignite ignite : Ignition.allGrids()) {
            for (IgniteFeature feature : features)
                assertFalse(isFeatureActive(ignite, feature));
        }
    }

    /** */
    protected Collection<IgniteFeature> newFeatures(Collection<IgniteFeature> srcFeatures, Collection<IgniteFeature> targetFeatures) {
        List<IgniteFeature> res = new ArrayList<>();

        for (IgniteFeature targetFeature : targetFeatures) {
            if (!srcFeatures.contains(targetFeature))
                res.add(targetFeature);
        }

        return res;
    }

    /** */
    protected void checkFeatureActivationSubscription(int nodeIdx, IgniteFeature feature, CountDownLatch featureActivationLatch) {
        long expCnt = featureActivationLatch.getCount();

        assertFalse(isFeatureActive(grid(nodeIdx), feature));
        listenFeatureActivation(grid(nodeIdx), feature, featureActivationLatch::countDown);
        assertEquals(expCnt, featureActivationLatch.getCount());
    }

    /** */
    protected void upgradeNodeVersion(int nodeIdx, String targetVer) throws Exception {
        upgradeNodeVersion(nodeIdx, TEST_DEFAULT_VER, targetVer);
    }

    /** */
    protected void upgradeNodeVersion(int nodeIdx, String srcVer, String targetVer) throws Exception {
        upgradeNodeVersion(nodeIdx, srcVer, srcVer, targetVer);
    }

    /** */
    protected void upgradeNodeVersion(int nodeIdx, String logicalVer, String srcVer, String targetVer) throws Exception {
        boolean isClient = grid(nodeIdx).context().clientNode();

        stopGrid(nodeIdx);
        checkJoinSuccess(nodeIdx, targetVer, isClient);
        checkVersionUpgradeInProgress(logicalVer, srcVer, targetVer);
    }

    /** */
    protected void finalizeClusterVersion(int nodeIdx, String expVer) throws Exception {
        ru(nodeIdx).finalizeClusterVersion();

        checkVersionUpgradeInactive(expVer);
    }

    /** */
    protected void restartNode(int nodeIdx) throws Exception {
        String ver = resolveNodeCompoundVersion(nodeIdx);
        boolean isClient = grid(nodeIdx).context().clientNode();

        stopGrid(nodeIdx);
        checkJoinSuccess(nodeIdx, ver, isClient);
    }

    /** */
    protected void forAllNodes(ConsumerX<Integer> nodeProcessor) throws Exception {
        for (Ignite ignite : Ignition.allGrids())
            nodeProcessor.accept(getTestIgniteInstanceIndex(ignite.name()));
    }

    /** */
    protected void checkUpgradeFailed(int nodeIdx, String targetVer, String errMsg) throws Exception {
        String srcVer = resolveNodeCompoundVersion(nodeIdx);
        boolean isClient = grid(nodeIdx).context().clientNode();

        stopGrid(nodeIdx);

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(nodeIdx, targetVer, isClient), IgniteSpiException.class, errMsg);

        startGrid(nodeIdx, srcVer, isClient);
    }

    /** */
    String resolveNodeCompoundVersion(int nodeIdx) {
        return Arrays.stream(ru(nodeIdx).features().localVersionFeatures().values())
            .sorted(Comparator.comparing(IgniteComponentFeatureSet::componentName))
            .map(f -> semanticVersion(f.version()))
            .collect(Collectors.joining("|"));
    }

    /** */
    protected void checkRollingUpgradeState(String expLogicalVer, String expSrcVer, String expTargetVer) {
        TestVersions expLogicalVersions = TestVersions.parse(expLogicalVer);
        TestVersions expSrcVersions = TestVersions.parse(expSrcVer);
        TestVersions expTargetVersions = TestVersions.parse(expTargetVer);

        for (Ignite ignite : Ignition.allGrids()) {
            assertTrue(ru(ignite).isVersionUpgradeEnabled());

            expLogicalVersions.cmpVersions.forEach((cmp, ver) -> {
                IgniteComponentFeatureSet cmpFeatures = ru(ignite).features().activeComponentFeatures(cmp);

                assertEquals(ver, cmpFeatures == null ? null : semanticVersion(cmpFeatures.version()));
            });

            expSrcVersions.cmpVersions.forEach((cmp, ver) -> {
                IgniteComponentUpgradeState state = ru(ignite).state(cmp);

                assertEquals(ver, state.srcVer == null ? null : semanticVersion(state.srcVer));
            });

            expTargetVersions.cmpVersions.forEach((cmp, ver) -> {
                IgniteComponentUpgradeState state = ru(ignite).state(cmp);

                String expVer = Objects.equals(expSrcVersions.cmpVersions.get(cmp), ver) ? null : ver;

                assertEquals(expVer, state.targetVer == null ? null : semanticVersion(state.targetVer));
            });
        }
    }

    /** */
    protected int coordinatorIndex() {
        for (Ignite grid : IgnitionEx.allGridsx()) {
            if (U.isLocalNodeCoordinator(((IgniteEx)grid).context().discovery()))
                return getTestIgniteInstanceIndex(grid.name());
        }

        fail("Failed to resolve coordinator node");

        return -1;
    }

    /** */
    protected RollingUpgradeProcessor ru(int nodeIdx) {
        return ru(grid(nodeIdx));
    }

    /** */
    protected static RollingUpgradeProcessor ru(Ignite ignite) {
        return ((IgniteEx)ignite).context().rollingUpgrade();
    }

    /** */
    protected static class TestVersions {
        /** */
        private final Map<String, String> cmpVersions = new HashMap<>();

        /** */
        public TestVersions(List<String> cmpVersions) {
            assertTrue(!cmpVersions.isEmpty());
            assertTrue(cmpVersions.size() <= 2);

            this.cmpVersions.put(IgniteCoreFeature.COMPONENT_NAME, cmpVersions.get(0));

            if (cmpVersions.size() > 1)
                this.cmpVersions.put(TestPluginFeature.COMPONENT_NAME, cmpVersions.get(1));
        }

        /** */
        public static TestVersions parse(String ver) {
            if (ver == null)
                return new TestVersions(Collections.singletonList(null));

            return new TestVersions(Arrays.stream(ver.split("\\|"))
                .map(String::trim)
                .map(v -> "null".equals(v) ? null : v)
                .collect(Collectors.toList()));
        }

        /** */
        public String pluginVersion() {
            return cmpVersions.get(TestPluginFeature.COMPONENT_NAME);
        }

        /** */
        public String coreVersion() {
            return cmpVersions.get(IgniteCoreFeature.COMPONENT_NAME);
        }

        /** */
        public boolean containsPlugin() {
            return cmpVersions.get(TestPluginFeature.COMPONENT_NAME) != null;
        }
    }

    /** */
    protected static class TestRollingUpgradeProcessor extends RollingUpgradeProcessor {
        /** */
        public static CountDownLatch nodeJoinUnblockedLatch;

        /** */
        public static CountDownLatch nodeJoinValidationCompletedLatch;

        /** */
        public TestRollingUpgradeProcessor(GridKernalContext ctx, IgniteCoreFeatureSet testCoreFeatures) {
            super(ctx, testCoreFeatures);

            ctx.event().addLocalEventListener(new TestNodeValidationFailedEventListener(), EVT_NODE_VALIDATION_FAILED);
        }

        /** {@inheritDoc} */
        @Override @Nullable public IgniteNodeValidationResult validateNode(ClusterNode joiningNode) {
            IgniteNodeValidationResult res = super.validateNode(joiningNode);

            if (res != null)
                return res;

            if (nodeJoinValidationCompletedLatch != null)
                nodeJoinValidationCompletedLatch.countDown();

            if (nodeJoinUnblockedLatch != null) {
                try {
                    assertTrue(nodeJoinUnblockedLatch.await(5_000, MILLISECONDS));
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    log.error(e.getMessage(), e);

                    return new IgniteNodeValidationResult(joiningNode.id(), e.getMessage());
                }
            }

            return null;
        }
    }

    /** */
    protected static class TestNodeValidationFailedEventListener implements GridLocalEventListener, HighPriorityListener {
        /** */
        public static CountDownLatch nodeValidationFailedEventUnblockedLatch;

        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            try {
                if (nodeValidationFailedEventUnblockedLatch != null)
                    assertTrue(nodeValidationFailedEventUnblockedLatch.await(5_000, MILLISECONDS));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                log.error(e.getMessage(), e);

                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return 0;
        }
    }
}
