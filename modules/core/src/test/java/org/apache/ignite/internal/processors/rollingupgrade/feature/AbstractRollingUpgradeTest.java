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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
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
import static org.apache.ignite.events.EventType.EVT_NODE_VALIDATION_FAILED;

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
        public TestRollingUpgradeProcessor(GridKernalContext ctx, IgniteProductFeatures testNodeVerFeatures) {
            super(ctx, () -> testNodeVerFeatures);

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
    protected boolean isFeatureActive(Ignite ignite, IgniteFeature feature) {
        return ((IgniteEx)ignite).context().rollingUpgrade().features().isActive(feature);
    }

    /** */
    protected void listenFeatureActivation(Ignite ignite, IgniteFeature feature, IgniteRunnable lsnr) {
        ((IgniteEx)ignite).context().rollingUpgrade().features().listenActivation(feature, lsnr);
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
