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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests rebalancing of IgniteSet for a case when a custom class, that is used as a key, is absent
 * in the classpath on the joined node.
 */
public class GridCacheSetRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final String CUSTOM_KEY_CLS = "org.apache.ignite.tests.p2p.IgniteSetCustomKey";

    /** Internal name for atomic partitioned data structures {@link DataStructuresProcessor}. */
    private static final String DATA_STRUCTURES_CACHE_NAME = "datastructures_ATOMIC_PARTITIONED_1@default-ds-group";

    /** Flag that is raised in case the failure handler was called. */
    private final AtomicBoolean failure = new AtomicBoolean();

    /** Flag indicates additional classes should be included into the class-path. */
    private boolean useExtendedClasses;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(200 * 1024 * 1024))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setAutoActivationEnabled(false);

        if (useExtendedClasses)
            cfg.setClassLoader(getExternalClassLoader());

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setFailureHandler(new TestFailureHandler());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testCollocatedSet() throws Exception {
        useExtendedClasses = true;

        Ignite ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteSet set = ignite0.set("test-set", new CollectionConfiguration().setBackups(1).setCollocated(true));

        Class customKeyCls = ignite0.configuration().getClassLoader().loadClass(CUSTOM_KEY_CLS);

        for (int i = 0; i < 100; ++i)
            set.add(customKeyCls.newInstance());

        TestRecordingCommunicationSpi.spi(ignite0)
            .blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return (msg instanceof GridDhtPartitionSupplyMessage)
                        && ((GridCacheGroupIdMessage)msg).groupId() == groupIdForCache(ignite0, DATA_STRUCTURES_CACHE_NAME);
                }
            });

        useExtendedClasses = false;

        startGrid(1);

        ignite0.cluster().setBaselineTopology(ignite0.cluster().forServers().nodes());

        assertTrue(
            "Data rebalancing is not started.",
            TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked(1, 10_000));

        // Resend delayed rebalance messages.
        TestRecordingCommunicationSpi.spi(ignite0).stopBlock(true);

        GridDhtPartitionDemander.RebalanceFuture fut = (GridDhtPartitionDemander.RebalanceFuture)grid(1).context().
            cache().internalCache(DATA_STRUCTURES_CACHE_NAME).preloader().rebalanceFuture();

        // Rebalance future should not be cancelled or failed.
        assertTrue(fut.get(10, TimeUnit.SECONDS));

        // Check that the failure handler was not called.
        assertFalse(failure.get());
    }

    /** Test failure handler. */
    private class TestFailureHandler extends NoOpFailureHandler {
        /** {@inheritDoc} */
        @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
            failure.set(true);

            return super.onFailure(ignite, failureCtx);
        }
    }
}
