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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.rollingupgrade.RollingUpgradeProcessor;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jspecify.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/** */
public abstract class AbstractRollingUpgradeTest extends GridCommonAbstractTest {
    /** */
    protected static final String TEST_DEFAULT_VER = "2.19.0";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return getConfiguration(igniteInstanceName, TEST_DEFAULT_VER);
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        TestRollingUpgradeProcessor.nodeJoinValidationCompletedLatch = null;
        TestRollingUpgradeProcessor.nodeJoinUnblockedLatch = null;
        TestRollingUpgradeProcessor.isNodeValidationFailureImitationEnabled.set(false);
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

        IgniteProductFeatures testNodeVerFeatures = new IgniteProductFeatures(
            IgniteProductVersion.fromString(ver),
            IgniteFeatureSet.buildFrom(readDeclaredFeatures(ver)));

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "test-rolling-upgrade-processor-provider";
            }

            /** {@inheritDoc} */
            @Override public @Nullable <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (cls.isAssignableFrom(DiscoveryNodeValidationProcessor.class))
                    return (T)new TestRollingUpgradeProcessor(((IgniteEx)ctx.grid()).context(), testNodeVerFeatures);

                return null;
            }
        });

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ignored) {
                super.setNodeAttributes(attrs, IgniteProductVersion.fromString(ver));
            }
        };

        discoSpi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** */
    public IgniteEx startGrid(int idx, String ver, boolean isClient) throws Exception {
        if (isClient)
            return startClientGrid(getConfiguration(idx, ver));

        return startGrid(getConfiguration(idx, ver));
    }

    /** */
    protected Collection<IgniteFeature> readDeclaredFeatures(String ver) throws Exception {
        Class<?> cls = Class.forName(
            TestIgniteReleaseFeatures_2_18_0.class.getPackageName() + ".TestIgniteReleaseFeatures_" + ver.replace(".", "_"));

        return IgniteFeatureSet.readDeclaredFeatures(cls);
    }

    /** */
    protected static class TestRollingUpgradeProcessor extends RollingUpgradeProcessor {
        /** */
        public static CountDownLatch nodeJoinUnblockedLatch;

        /** */
        public static CountDownLatch nodeJoinValidationCompletedLatch;

        /** */
        public static final AtomicBoolean isNodeValidationFailureImitationEnabled = new AtomicBoolean();

        /** */
        public TestRollingUpgradeProcessor(GridKernalContext ctx, IgniteProductFeatures testNodeVerFeatures) {
            super(ctx, () -> testNodeVerFeatures);
        }

        /** {@inheritDoc} */
        @Override @Nullable public IgniteNodeValidationResult validateNode(ClusterNode joiningNode) {
            IgniteNodeValidationResult res = super.validateNode(joiningNode);

            if (res != null)
                return res;

            if (nodeJoinValidationCompletedLatch != null) {
                nodeJoinValidationCompletedLatch.countDown();

                try {
                    assertTrue(nodeJoinUnblockedLatch.await(5_000, MILLISECONDS));
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    log.error(e.getMessage(), e);

                    return new IgniteNodeValidationResult(joiningNode.id(), e.getMessage());
                }
            }

            if (isNodeValidationFailureImitationEnabled.get())
                return new IgniteNodeValidationResult(joiningNode.id(), "expected validation error");

            return null;
        }
    }

    /** */
    protected boolean isFeatureActive(Ignite ignite, IgniteFeature feature) {
        return ((IgniteEx)ignite).context().rollingUpgrade().features().isActive(feature);
    }

    /** */
    protected void listenFeatureActivation(Ignite ignite, IgniteFeature feature, IgniteRunnable lsnr) {
        ((IgniteEx)ignite).context().rollingUpgrade().features().listenActivation(feature, lsnr);
    }
}
