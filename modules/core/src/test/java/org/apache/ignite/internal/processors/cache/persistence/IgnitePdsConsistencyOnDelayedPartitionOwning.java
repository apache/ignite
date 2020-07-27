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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.WalStateManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;

/**
 * Tests a scenario when delayed partition owning on exchange overlaps with new rebalancing.
 * Previous future should be cancelled if it's not compatible with new to avoid sending stale partition map
 * after checkpoint has been completed asynchronously.
 */
public class IgnitePdsConsistencyOnDelayedPartitionOwning extends GridCommonAbstractTest {
    /** Parts. */
    private static final int PARTS = 128;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(50L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalSegmentSize(4 * 1024 * 1024)
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(100000); // Disable timeout checkpoints.

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void checkConsistencyNodeLeft() throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(4);
        crd.cluster().active(true);

        for (int i = 0; i < PARTS; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint();

        stopGrid(1);

        for (int i = 0; i < PARTS; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i + 1);

        // Block supply messages from all owners.
        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(grid(0));
        TestRecordingCommunicationSpi spi2 = TestRecordingCommunicationSpi.spi(grid(2));
        TestRecordingCommunicationSpi spi3 = TestRecordingCommunicationSpi.spi(grid(3));

        IgniteBiPredicate<ClusterNode, Message> pred = new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                return msg instanceof GridDhtPartitionSupplyMessage;
            }
        };

        spi0.blockMessages(pred);
        spi2.blockMessages(pred);
        spi3.blockMessages(pred);

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        });

        spi0.waitForBlocked();
        spi2.waitForBlocked();
        spi3.waitForBlocked();

        spi0.stopBlock();
        spi2.stopBlock();

        CountDownLatch topInitLatch = new CountDownLatch(1);
        CountDownLatch enableDurabilityCPStartLatch = new CountDownLatch(1);
        CountDownLatch delayedOnwningLatch = new CountDownLatch(1);

        GridCacheDatabaseSharedManager dbMgr =
            (GridCacheDatabaseSharedManager) grid(1).context().cache().context().database();

        dbMgr.addCheckpointListener(new DbCheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // No-op.
            }

            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // No-op.
            }

            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
                String reason = ctx.progress().reason();

                String reason0 = WalStateManager.reason(cacheId(DEFAULT_CACHE_NAME), new AffinityTopologyVersion(6, 0));

                if (reason != null && reason.equals(reason0)) {
                    enableDurabilityCPStartLatch.countDown();

                    try {
                        assertTrue(U.await(delayedOnwningLatch, 10_000, TimeUnit.MILLISECONDS));
                    } catch (IgniteInterruptedCheckedException e) {
                        fail(X.getFullStackTrace(e));
                    }
                }
            }
        });

        TestRecordingCommunicationSpi.spi(grid(1)).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionDemandMessage) {
                    GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage) msg;

                    return msg0.topologyVersion().equals(new AffinityTopologyVersion(7, 0));
                }

                return false;
            }
        });

        grid(1).context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                if (fut.initialVersion().equals(new AffinityTopologyVersion(7, 0))) {
                    topInitLatch.countDown();

                    try {
                        assertTrue(U.await(enableDurabilityCPStartLatch, 20_000, TimeUnit.MILLISECONDS));
                    } catch (IgniteInterruptedCheckedException e) {
                        fail(X.getFullStackTrace(e));
                    }

                    System.out.println();
                }
            }
        });

        // Trigger rebalancing remap because owner has left.
        IgniteInternalFuture stopFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                stopGrid(2); // TODO start cache.
            }
        });

        // Wait for topology (7,0) init on grid1 before finishing rebalancing on (6,0).
        assertTrue(U.await(topInitLatch, 20_000, TimeUnit.MILLISECONDS));

        // Release last supply message, causing triggering a cp for enabling durability.
        spi3.stopBlock();

        // Wait for new rebalancing assignments ready on grid1.
        TestRecordingCommunicationSpi.spi(grid(1)).waitForBlocked();

        // Triggers spurious ideal switching before rebalancing has finished for (7,0).
        delayedOnwningLatch.countDown();

        stopFut.get();

        TestRecordingCommunicationSpi.spi(grid(1)).stopBlock();

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        CacheGroupContext grpCtx = grid(1).context().cache().cacheGroup(cacheId(DEFAULT_CACHE_NAME));

        if (grpCtx != null)
            assertTrue(grpCtx.localWalEnabled());
    }
}
