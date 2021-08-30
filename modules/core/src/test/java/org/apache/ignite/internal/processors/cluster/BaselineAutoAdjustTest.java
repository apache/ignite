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

package org.apache.ignite.internal.processors.cluster;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class BaselineAutoAdjustTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_NAME = "TEST_NAME";

    /** */
    private static int autoAdjustTimeout = 5000;

    /** Lifecycle bean. */
    private LifecycleBean lifecycleBean;

    /**
     * @throws Exception if failed.
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        autoAdjustTimeout = 5000;
    }

    /**
     * @throws Exception if failed.
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(isPersistent())
            .setMaxSize(500L * 1024 * 1024);

        storageCfg
            .setWalSegments(3)
            .setWalSegmentSize(512 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setLifecycleBeans(lifecycleBean);

        return cfg;
    }

    /** */
    protected boolean isPersistent() {
        return true;
    }

    /**
     * Tests that merging exchanges properly triggers baseline changing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExchangeMerge() throws Exception {
        // Latch that waits for PME (intTopVer == 3.0)
        CountDownLatch exchangeWorkerLatch = new CountDownLatch(1);

        // Lyficycle bean is needed in order to register EVT_NODE_JOIN lister that is called
        // right after GridCachePartitionExchangeManager and before GridClusterStateProcessor.
        lifecycleBean = new LifecycleBean() {
            /** Ignite instance. */
            @IgniteInstanceResource
            IgniteEx ignite;

            /** {@inheritDoc} */
            @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                if (evt == LifecycleEventType.BEFORE_NODE_START) {
                    ignite.context().internalSubscriptionProcessor()
                        .registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
                            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                                ignite.context().event().addDiscoveryEventListener((evt, disco) -> {
                                    if (evt.type() == EVT_NODE_JOINED && evt.topologyVersion() == 3) {
                                        try {
                                            // Let's wait for exchange worker starts PME
                                            // that related to the first node joined the cluster.
                                            exchangeWorkerLatch.await(getTestTimeout(), MILLISECONDS);
                                        }
                                        catch (InterruptedException e) {
                                            throw new IgniteException("exchangeWorkerLatch has been interrupted.", e);
                                        }
                                    }
                                }, EVT_NODE_JOINED);
                            }
                        });
                }
            }
        };

        // Start the coordinator node.
        IgniteEx crd = startGrid(0);

        // This bean is only required on the coordinator node.
        lifecycleBean = null;

        // Latch indicates that EVT_NODE_JOINED (topVer == 4.0) was processed by all listeners.
        CountDownLatch nodeJoinLatch = new CountDownLatch(1);

        // This listener is the last one in the queue of handlers.
        crd.context().event().addDiscoveryEventListener((evt, disco) -> {
            if (evt.type() == EVT_NODE_JOINED && evt.topologyVersion() == 4)
                nodeJoinLatch.countDown();
        }, EVT_NODE_JOINED);

        IgniteEx nonCrd = startGrid(1);

        crd.cluster().state(ACTIVE);

        crd.cluster().baselineAutoAdjustEnabled(true);
        crd.cluster().baselineAutoAdjustTimeout(autoAdjustTimeout);

        awaitPartitionMapExchange(false, true, null);

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(nonCrd);
        spi1.blockMessages((node, msg) -> msg instanceof GridDhtPartitionsSingleMessage);

        // Target major topology version (4 nodes)
        long targetTopVer = 4;

        // Let's block exchange process in order to merge two following exchanges (3.0 && 4.0).
        crd.context()
            .cache()
            .context()
            .exchange()
            .mergeExchangesTestWaitVersion(new AffinityTopologyVersion(targetTopVer, 0), null);

        AtomicInteger cnt = new AtomicInteger(G.allGrids().size());
        runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(cnt.getAndIncrement());

                return null;
            }
        }, 2, "async-grid-starter");

        // Make sure that PME is in progress.
        assertTrue("Failed to wait for PME [topVer=3]", spi1.waitForBlocked(1, getTestTimeout()));

        assertTrue(
            "Failed to wait for the first started exchange.",
            waitForCondition(() -> {
                GridDhtPartitionsExchangeFuture fut = crd.context().cache().context().exchange().lastTopologyFuture();
                return fut.initialVersion().topologyVersion() == 3;
            }, getTestTimeout()));

        // This guarantees that BaselineAutoAdjustData listens to real GridDhtPartitionsExchangeFuture
        // instead of readyAffinityFuture.
        exchangeWorkerLatch.countDown();

        assertTrue(
            "Failed to wait for processing node join event [topVer=3]",
            nodeJoinLatch.await(getTestTimeout(), MILLISECONDS));

        // Unblock PME
        spi1.stopBlock();

        assertTrue(
            "Failed to wait for changing baseline in " + autoAdjustTimeout * 2 + " ms.",
            waitForCondition(() -> crd.cluster().currentBaselineTopology().size() == targetTopVer, autoAdjustTimeout * 2));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustAfterNodeLeft() throws Exception {
        Ignite ignite0 = startGrids(2);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustTimeout(autoAdjustTimeout);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopGrid(1);

        Set<Object> nodeLeftBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        assertEquals(initBaseline, nodeLeftBaseline);

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite0),
            autoAdjustTimeout * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustSinceSecondNodeLeft() throws Exception {
        Ignite ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustTimeout(autoAdjustTimeout);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopGrid(1);

        doSleep(autoAdjustTimeout / 2);

        stopGrid(2);

        doSleep(autoAdjustTimeout / 2);

        Set<Object> twoNodeLeftBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        assertEquals(initBaseline, twoNodeLeftBaseline);

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite0),
            autoAdjustTimeout * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustSinceCoordinatorLeft() throws Exception {
        Ignite ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustTimeout(autoAdjustTimeout);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopGrid(1);

        doSleep(autoAdjustTimeout / 2);

        stopGrid(0);

        doSleep(autoAdjustTimeout / 2);

        Ignite ignite2 = ignite(2);

        Set<Object> twoNodeLeftBaseline = ignite2.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        assertEquals(initBaseline, twoNodeLeftBaseline);

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(ignite2),
            autoAdjustTimeout
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustAfterNodeJoin() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustTimeout(autoAdjustTimeout);

        assertTrue(isCurrentBaselineFromOneNode(ignite0));

        startGrid(1);

        assertTrue(isCurrentBaselineFromOneNode(ignite0));

        assertTrue(waitForCondition(
            () -> ignite0.cluster().currentBaselineTopology().size() == 2,
            autoAdjustTimeout * 10
        ));
    }

    /**
     * @param ignite0 Node to check.
     * @return {@code true} if current baseline consist from one node.
     */
    private boolean isCurrentBaselineFromOneNode(Ignite ignite0) {
        return ignite0.cluster().currentBaselineTopology() != null &&
            ignite0.cluster().currentBaselineTopology().stream()
                .map(BaselineNode::consistentId)
                .allMatch(((IgniteEx)ignite0).localNode().consistentId()::equals);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustDisabledAfterGridHasLostPart() throws Exception {
        autoAdjustTimeout = 0;

        Ignite ignite0 = startGrids(2);

        ignite0.cluster().state(ACTIVE);

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        ignite0.cluster().baselineAutoAdjustTimeout(autoAdjustTimeout);

        IgniteCache<Object, Object> cache = ignite0.getOrCreateCache(new CacheConfiguration<>(TEST_NAME)
            .setBackups(0)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
        );

        for (int j = 0; j < 500; j++)
            cache.put(j, "Value" + j);

        stopGrid(1);

        doSleep(3000);

        Set<Object> baselineAfterNodeLeft = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        if (isPersistent())
            assertEquals(initBaseline, baselineAfterNodeLeft);
        else {
            assertNotEquals(initBaseline, baselineAfterNodeLeft);

            assertEquals(1, baselineAfterNodeLeft.size());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test(expected = BaselineAdjustForbiddenException.class)
    public void testBaselineAutoAdjustThrowExceptionWhenBaselineChangedManually() throws Exception {
        IgniteEx ignite0 = startGrids(2);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustTimeout(autoAdjustTimeout);

        Collection<BaselineNode> baselineNodes = ignite0.cluster().currentBaselineTopology();

        assertEquals(2, baselineNodes.size());

        stopGrid(1);

        ignite0.cluster().setBaselineTopology(Collections.singletonList(ignite0.localNode()));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustTriggeredAfterFirstEventRegardlessInitBaseline() throws Exception {
        assumeTrue(isPersistent());

        autoAdjustTimeout = 3000;

        Ignite ignite0 = startGrids(3);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustTimeout(autoAdjustTimeout);

        assertTrue(ignite0.cluster().isBaselineAutoAdjustEnabled());

        Set<Object> baselineNodes = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopAllGrids();

        ignite0 = startGrids(2);

        ignite0.cluster().state(ACTIVE);

        Set<Object> baselineNodesAfterRestart = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        assertEquals(baselineNodes, baselineNodesAfterRestart);

        stopGrid(1);

        Ignite finalIgnite = ignite0;

        assertTrue(waitForCondition(
            () -> isCurrentBaselineFromOneNode(finalIgnite),
            autoAdjustTimeout * 2
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustIgnoreClientNodes() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        startGrid(1);

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustTimeout(autoAdjustTimeout);

        assertTrue(ignite0.cluster().isBaselineAutoAdjustEnabled());

        stopGrid(1);

        doSleep(autoAdjustTimeout / 2);

        IgniteEx igniteClient = startClientGrid(getConfiguration(getTestIgniteInstanceName(2)));

        doSleep(autoAdjustTimeout / 2);

        igniteClient.close();

        assertTrue(isCurrentBaselineFromOneNode(ignite0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBaselineAutoAdjustDisableByDefaultBecauseNotNewCluster() throws Exception {
        assumeTrue(isPersistent());

        {
            //It emulate working cluster before auto-adjust feature was available.
            Ignite ignite0 = startGrids(2);
            ignite0.cluster().baselineAutoAdjustEnabled(false);

            //This activation guarantee that baseline would be set.
            ignite0.cluster().state(ACTIVE);

            ignite0.cluster().baselineAutoAdjustTimeout(0);

            stopAllGrids();
        }

        //Auto-activation is expected. Not first activation should be detected.
        Ignite ignite0 = startGrids(2);

        awaitPartitionMapExchange();

        Set<Object> initBaseline = ignite0.cluster().currentBaselineTopology().stream()
            .map(BaselineNode::consistentId)
            .collect(Collectors.toSet());

        stopGrid(1);

        //Should nothing happened because auto-adjust should be disable due to activation is not first.
        assertFalse(waitForCondition(
            () -> !initBaseline.equals(
                ignite0.cluster().currentBaselineTopology().stream()
                    .map(BaselineNode::consistentId)
                    .collect(Collectors.toSet())
            ),
            5_000
        ));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void shouldNodeWithPersistenceSuccessfullyJoinedToClusterWhenAutoAdjustDisabled() throws Exception {
        assumeFalse(isPersistent());

        cleanPersistenceDir();

        IgniteEx ignite0 = startGrid(inMemoryConfiguration(0));

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustEnabled(false);

        startGrid(persistentRegionConfiguration(1));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void shouldNodeWithPersistenceSuccessfullyJoinedToClusterWhenTimeoutGreaterThanZero() throws Exception {
        IgniteEx ignite0 = startGrid(inMemoryConfiguration(0));

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustTimeout(1);

        startGrid(persistentRegionConfiguration(1));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void shouldJoinSuccessBecauseCoordinatorIsPersistent() throws Exception {
        IgniteEx ignite0 = startGrid(persistentRegionConfiguration(0));

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        ignite0.cluster().state(ACTIVE);

        startGrid(inMemoryConfiguration(1));

        startGrid(persistentRegionConfiguration(2));

        assertEquals(3, ignite0.cluster().nodes().size());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void shouldJoinSuccessBecauseClusterHasPersistentNode() throws Exception {
        IgniteEx ignite0 = startGrid(inMemoryConfiguration(0));

        ignite0.cluster().state(ACTIVE);

        ignite0.cluster().baselineAutoAdjustEnabled(false);

        startGrid(persistentRegionConfiguration(1));

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        startGrid(persistentRegionConfiguration(2));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void shouldJoinFailedBecauseCoordinatorIsInMemoryNodeAndEnabledAutoAdjust() throws Exception {
        IgniteEx ignite0 = startGrid(inMemoryConfiguration(0));
        startGrid(inMemoryConfiguration(1));

        ignite0.cluster().state(ACTIVE);

        try {
            startGrid(persistentRegionConfiguration(2));
        }
        catch (IgniteCheckedException ex) {
            if (!X.hasCause(ex, "Joining persistence node to in-memory cluster couldn't be allowed", IgniteSpiException.class))
                fail("Join should be fail due to cluster has in-memory node and enabled auto-adjust.");
        }
    }

    /**
     * Test that node joins baseline topology after enabling auto adjust.
     *
     * Description:
     * Start first node and set baseline auto adjust timeout to 100ms. Disable auto adjust and activate cluster.
     * Start another node and wait until it joins cluster. Enable auto adjust and check that node is in
     * baseline topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJoinBltExistingNode() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().baselineAutoAdjustTimeout(100);

        ignite0.cluster().baselineAutoAdjustEnabled(false);

        ignite0.cluster().state(ACTIVE);

        startGrid(1);

        startClientGrid(2);

        awaitPartitionMapExchange();

        assertEquals(3, ignite0.cluster().nodes().size());

        assertEquals(1, ignite0.cluster().currentBaselineTopology().size());

        ignite0.cluster().baselineAutoAdjustEnabled(true);

        assertTrue(GridTestUtils.waitForCondition(() -> {
            return 2 == ignite0.cluster().currentBaselineTopology().size();
        }, 10_000));
    }

    /**
     * @throws Exception if failed.
     */
    private IgniteConfiguration inMemoryConfiguration(int id) throws Exception {
        IgniteConfiguration conf = getConfiguration(getTestIgniteInstanceName(id));

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(false)
            .setMaxSize(500L * 1024 * 1024);

        storageCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setName("InMemoryRegion")
            .setPersistenceEnabled(false)
            .setMaxSize(500L * 1024 * 1024));

        conf.setDataStorageConfiguration(storageCfg);

        return conf;
    }

    /**
     * @throws Exception if failed.
     */
    private IgniteConfiguration persistentRegionConfiguration(int id) throws Exception {
        IgniteConfiguration conf = getConfiguration(getTestIgniteInstanceName(id));

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(false)
            .setMaxSize(500L * 1024 * 1024);

        storageCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setName("PersistentRegion")
            .setPersistenceEnabled(true)
            .setMaxSize(500L * 1024 * 1024));

        conf.setDataStorageConfiguration(storageCfg);

        return conf;
    }
}
