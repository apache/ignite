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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.management.cache.IdleVerifyTaskResultV2;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.SNAPSHOT_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** Tests the consistency of taken snapshots across multiple nodes. */
@RunWith(Parameterized.class)
public class IgniteSnapshotConsistencyTest extends GridCommonAbstractTest {
    /** */
    private final AtomicInteger keyCntr = new AtomicInteger(0);

    /** */
    private final Map<Integer, BlockingCheckpointListener> blockedCheckpointNodes = new ConcurrentHashMap<>();

    /** */
    @Parameterized.Parameter
    public boolean isOpInitiatorClient;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicity;

    /** */
    @Parameterized.Parameter(2)
    public int backups;

    /** */
    @Parameterized.Parameter(3)
    public TransactionConcurrency txConcurrency;

    /** */
    @Parameterized.Parameter(4)
    public boolean onlyPrimary;

    /** */
    @Parameterized.Parameters(name = "isClient={0}, atomicity={1}, backups={2}, txConcurrency={3}, onlyPrimayr={4}")
    public static Iterable<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        for (boolean isClient : Arrays.asList(true, false)) {
            for (int backups = 1; backups <= 2; ++backups) {
                for (boolean onlyPrimary : Arrays.asList(true, false)) {
                    res.add(new Object[]{isClient, ATOMIC, backups, null, onlyPrimary});

                    for (TransactionConcurrency txConcurrency : TransactionConcurrency.values())
                        res.add(new Object[]{isClient, TRANSACTIONAL, backups, txConcurrency, onlyPrimary});
                }
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        cfg.setConsistentId(igniteInstanceName);
        cfg.setUserAttributes(singletonMap(IDX_ATTR, getTestIgniteInstanceIndex(igniteInstanceName)));
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

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

    /**
     * Tests the following scenario:
     * 1. Cache put is performed while snapshot creation operation is in progress.
     * 2. Cache put is mapped to a topology version that is earlier than the version associated with the start of the
     *    snapshot operation.
     * 3. Check that snapshots taken from all nodes are consistent.
     */
    @Test
    public void testConcurrentPutWithStaleTopologyVersion() throws Exception {
        int srvCnt = 3;
        int primaryIdx = 1;
        int hangingBackupIdx = 2;

        IgniteEx crd = startGrids(srvCnt);
        IgniteEx opInitiator = isOpInitiatorClient ? startClientGrid(srvCnt) : startGrid(srvCnt);

        crd.cluster().state(ACTIVE);

        crd.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setBackups(backups)
            .setAffinity(new GridCacheModuloAffinityFunction(srvCnt, backups))
            .setAtomicityMode(atomicity));

        awaitPartitionMapExchange();

        for (int i = 0; i < srvCnt; i++)
            grid(i).cache(DEFAULT_CACHE_NAME).put(keyForNode(i), 0);

        forceCheckpoint();

        spi(opInitiator).blockMessages(
            atomicity == TRANSACTIONAL ? GridNearTxPrepareRequest.class : GridNearAtomicSingleUpdateRequest.class,
            grid(primaryIdx).name()
        );

        IgniteInternalFuture<Void> putFut = doPut(opInitiator, keyForNode(primaryIdx));

        spi(opInitiator).waitForBlocked(1, getTestTimeout());

        blockCheckpoint(hangingBackupIdx);

        AffinityTopologyVersion snpTopVer = grid(1).context().cache().context()
            .exchange()
            .lastTopologyFuture()
            .topologyVersion()
            .nextMinorVersion();

        IgniteFuture<Void> snpFut = snp(crd).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

        waitForReadyTopology(grid(primaryIdx).cachex(DEFAULT_CACHE_NAME).context().topology(), snpTopVer);

        AtomicBoolean isPutRemappedOnNewTop = new AtomicBoolean(false);

        addNewTopologyVersionRemapListener(primaryIdx, /*initiator node idx*/ srvCnt, snpTopVer, isPutRemappedOnNewTop);

        spi(opInitiator).stopBlock();

        GridTestUtils.waitForCondition(() -> isPutRemappedOnNewTop.get() || putFut.isDone(), getTestTimeout());

        unblockCheckpoint(hangingBackupIdx);

        snpFut.get(getTestTimeout());
        putFut.get(getTestTimeout());

        IdleVerifyTaskResultV2 snpVerifyRes = crd.context().cache().context().snapshotMgr()
            .checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult();

        assertFalse(snpVerifyRes.hasConflicts());
    }

    /** */
    private void addNewTopologyVersionRemapListener(
        int locNodeIdx,
        int rmtNodeIdx,
        AffinityTopologyVersion newTopVer,
        AtomicBoolean remapFlag
    ) {
        grid(locNodeIdx).context().io().addMessageListener(TOPIC_CACHE, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (
                    (msg instanceof GridNearAtomicSingleUpdateRequest || msg instanceof GridNearTxPrepareRequest)
                        && nodeId.equals(grid(rmtNodeIdx).localNode().id())
                        && ((GridCacheMessage)msg).topologyVersion().compareTo(newTopVer) == 0
                )
                    remapFlag.set(true);
            }
        });
    }

    /** */
    private IgniteInternalFuture<Void> doPut(Ignite ignite, int key) {
        IgniteCache<Integer, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<Void> fut;

        if (atomicity == TRANSACTIONAL) {
            fut = GridTestUtils.runAsync(() -> {
                try (Transaction tx = ignite.transactions().txStart(txConcurrency, REPEATABLE_READ)) {
                    cache.put(key, "val");

                    tx.commit();
                }
            });
        }
        else
            fut = ((IgniteFutureImpl<Void>)cache.putAsync(key, "val")).internalFuture();

        return fut;
    }

    /** */
    private void blockCheckpoint(int nodeIdx) {
        BlockingCheckpointListener blockLsnr = new BlockingCheckpointListener();

        ((GridCacheDatabaseSharedManager)grid(nodeIdx).context().cache().context().database())
            .addCheckpointListener(blockLsnr);

        blockedCheckpointNodes.put(nodeIdx, blockLsnr);
    }

    /** */
    private void unblockCheckpoint(int nodeIdx) {
        BlockingCheckpointListener lsnr = blockedCheckpointNodes.remove(nodeIdx);

        if (lsnr == null)
            return;

        lsnr.unblock();

        ((GridCacheDatabaseSharedManager)grid(nodeIdx).context().cache().context().database())
            .removeCheckpointListener(lsnr);
    }

    /** */
    private int keyForNode(int nodeIdx) {
        return keyForNode(grid(0).affinity(DEFAULT_CACHE_NAME), keyCntr, grid(nodeIdx).cluster().localNode());
    }

    /** */
    private static class BlockingCheckpointListener implements CheckpointListener {
        /** */
        private final CountDownLatch checkpointUnblockedLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public void onMarkCheckpointBegin(CheckpointListener.Context ctx) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void onCheckpointBegin(CheckpointListener.Context ctx) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void beforeCheckpointBegin(CheckpointListener.Context ctx) throws IgniteCheckedException {
            try {
                checkpointUnblockedLatch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteCheckedException(e);
            }
        }

        /** */
        public void unblock() {
            checkpointUnblockedLatch.countDown();
        }
    }
}
