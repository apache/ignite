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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import com.google.common.base.Functions;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test partitions consistency after a historical rebalance with lost transaction requests and responses.
 */
public class HistoricalRebalanceCheckpointTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(50L * 1024 * 1024).setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.getDataStorageConfiguration().setFileIOFactory(
            new BlockableFileIOFactory(cfg.getDataStorageConfiguration().getFileIOFactory()));

        cfg.getDataStorageConfiguration().setWalMode(WALMode.FSYNC); // Allows to use special IO at WAL as well.

        cfg.setFailureHandler(new StopNodeFailureHandler()); // Helps to kill nodes on stop with disabled IO.

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_STOPPED);

        return cfg;
    }

    /**
     * Tests delayed prepare/finish transaction requests to the backups with 2 backups and 2-phase commit.
     */
    @Test
    public void testDelayedToBackupsRequests2Backups() throws Exception {
        doTestDelayedToBackupsRequests(3, false);
    }

    /**
     * Tests delayed prepare/finish transaction requests to the backups with 2 backups and 2-phase commit. Does more
     * puts after the gaps.
     */
    @Test
    public void testDelayedToBackupsRequests2BackupsMorePuts() throws Exception {
        doTestDelayedToBackupsRequests(3, true);
    }

    /**
     * Tests delayed prepare/finish transaction requests to the backups with 1 backup and one-phase commit.
     */
    @Test
    public void testDelayedToBackupsRequests1Backup() throws Exception {
        doTestDelayedToBackupsRequests(2, false);
    }

    /**
     * Tests delayed prepare/finish transaction requests to the backups with 1 backup and one-phase commit. Does more
     * puts after the gaps.
     */
    @Test
    public void testDelayedToBackupsRequests1BackupMorePuts() throws Exception {
        doTestDelayedToBackupsRequests(2, true);
    }

    /**
     * Test one-phase commit with lost responses from the backup.
     */
    @Test
    public void testDelayed1PhaseCommitResponses() throws Exception {
        final int preloadCnt = 2_000;

        prepareCluster(2, preloadCnt);

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        Ignite backup = backupNodes(0L, DEFAULT_CACHE_NAME).get(0);

        AtomicBoolean prepareBlock = new AtomicBoolean();

        AtomicReference<CountDownLatch> blockLatch = new AtomicReference<>();

        TestRecordingCommunicationSpi.spi(backup).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtTxPrepareResponse && prepareBlock.get()) {
                    CountDownLatch latch = blockLatch.get();

                    latch.countDown();

                    return true;
                }
                else
                    return false;
            }
        });

        IgniteCache<Integer, Integer> primCache = prim.cache(DEFAULT_CACHE_NAME);

        Consumer<Integer> cachePutAsync = (key) -> GridTestUtils.runAsync(() -> primCache.put(key, key));

        prepareBlock.set(true);

        blockLatch.set(new CountDownLatch(20));

        int updateCnt = preloadCnt;

        for (int i = 0; i < 20; i++)
            cachePutAsync.accept(++updateCnt);

        blockLatch.get().await();

        // Storing the highest counters on backup.
        forceCheckpoint();

        IdleVerifyResultV2 checkRes = idleVerify(prim, DEFAULT_CACHE_NAME);

        Map<Boolean, PartitionHashRecordV2> conflicts = F.flatCollections(checkRes.counterConflicts().values())
            .stream().collect(Collectors.toMap(PartitionHashRecordV2::isPrimary, Functions.identity()));

        // The cache is of only 1 partition with 2 nodes: primary and backup.
        assertEquals(2, conflicts.size());

        // Ensure the backup node got a higher counter.
        assertCounters(conflicts.get(true).updateCounter(), preloadCnt, null, preloadCnt);
        assertCounters(conflicts.get(false).updateCounter(), updateCnt, null, updateCnt);

        String backName = backup.name();

        backup.close();

        TestRecordingCommunicationSpi.spi(prim).blockMessages((n, m) -> m instanceof GridDhtPartitionDemandMessage ||
            m instanceof GridDhtPartitionSupplyMessage
        );

        startGrid(backName);

        awaitPartitionMapExchange();

        // Primary commits transactions when the backup node leaves. Ensure no rebalance occurs.
        assertFalse(TestRecordingCommunicationSpi.spi(prim).waitForBlocked(1, 5_000));

        checkRes = idleVerify(prim, DEFAULT_CACHE_NAME);
        assertFalse(checkRes.hasConflicts());
    }

    /** */
    private int prepareCluster(int nodes, int loadCnt) throws Exception {
        assert nodes > 1;

        int backupNodes = nodes - 1;

        IgniteEx ignite = startGrids(nodes);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(backupNodes)
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC) // Allows to be sure that all messages are sent when put succeed.
            .setReadFromBackup(true)); // Allows checking values on backups.

        // Initial preloading enough to have historical rebalance.
        for (int i = 0; i < loadCnt; i++)  //
            cache.put(i, i);

        // To have historical rebalance on cluster recovery. Decreases percent of updates in comparison to cache size.
        stopAllGrids();
        startGrids(nodes);

        return backupNodes;
    }

    /**
     * Tests delayed prepare/finish transaction requests to the backups.
     *
     * @param nodes Nodes number. The backups number is {@code nodes} - 1.
     * @param putAfterGaps If {@code true}, does more puts to the cache after the simulated gaps.
     */
    private void doTestDelayedToBackupsRequests(int nodes, boolean putAfterGaps) throws Exception {
        final int preloadCnt = 2_000;
        final int prepareBlockCnt = 20;
        final int finishBlockCnt = 30;
        final int putsAfterGapsCnt = 50;

        int backupNodes = prepareCluster(nodes, preloadCnt);

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        List<Ignite> backups = backupNodes(0L, DEFAULT_CACHE_NAME);

        AtomicBoolean prepareBlock = new AtomicBoolean();
        AtomicBoolean finishBlock = new AtomicBoolean();

        AtomicReference<CountDownLatch> blockLatch = new AtomicReference<>();

        TestRecordingCommunicationSpi.spi(prim).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if ((msg instanceof GridDhtTxPrepareRequest && prepareBlock.get()) ||
                    (msg instanceof GridDhtTxFinishRequest && finishBlock.get())) {
                    CountDownLatch latch = blockLatch.get();

                    assertTrue(latch.getCount() > 0);

                    latch.countDown();

                    return true;
                }
                else
                    return false;
            }
        });

        IgniteCache<Integer, Integer> primCache = prim.cache(DEFAULT_CACHE_NAME);

        Consumer<Integer> cachePutAsync = (key) -> GridTestUtils.runAsync(() -> primCache.put(key, key));

        int updateCnt = preloadCnt;

        try {
            // Blocked at primary and backups.
            prepareBlock.set(true);

            blockLatch.set(new CountDownLatch(backupNodes * prepareBlockCnt));

            for (int i = 0; i < prepareBlockCnt; i++)
                cachePutAsync.accept(++updateCnt);

            blockLatch.get().await();
        }
        finally {
            prepareBlock.set(false);
        }

        if (backupNodes > 1) {
            try {
                // Blocked at backups only.
                finishBlock.set(true);

                blockLatch.set(new CountDownLatch(backupNodes * finishBlockCnt));

                for (int i = 0; i < finishBlockCnt; i++)
                    cachePutAsync.accept(++updateCnt);

                blockLatch.get().await();
            }
            finally {
                finishBlock.set(false);
            }
        }

        if (putAfterGaps) {
            for (int i = 0; i < putsAfterGapsCnt; i++)
                prim.cache(DEFAULT_CACHE_NAME).put(++updateCnt, updateCnt);
        }

        // Storing counters on primary.
        forceCheckpoint();

        Collection<PartitionHashRecordV2> conflicts =
            F.flatCollections(idleVerify(prim, DEFAULT_CACHE_NAME).counterConflicts().values());

        // With one-phase commit backup writes entries on the transaction request.
        assertTrue(!conflicts.isEmpty() || backupNodes == 1);

        // Ensure the primary node got a higher counter.
        for (PartitionHashRecordV2 c : conflicts) {
            if (c.isPrimary()) {
                assertCounters(c.updateCounter(), preloadCnt, "" + (preloadCnt + 1) + " - " +
                    (preloadCnt + prepareBlockCnt), updateCnt);
            }
            else {
                assertCounters(c.updateCounter(), preloadCnt,
                    putAfterGaps ? "" + (preloadCnt + 1) + " - " + (preloadCnt + putsAfterGapsCnt) : null,
                    putAfterGaps ? updateCnt : preloadCnt);
            }
        }

        // Emulating power off, OOM or disk overflow. Keeping data as is, with missed counters updates.
        backups.forEach(node -> ((BlockableFileIOFactory)node.configuration().getDataStorageConfiguration()
            .getFileIOFactory()).blocked = true);

        List<String> backNames = backups.stream().map(Ignite::name).collect(Collectors.toList());

        CountDownLatch rebalanceFinished = new CountDownLatch(1);

        backups.forEach(Ignite::close);

        TestRecordingCommunicationSpi.spi(prim).blockMessages(GridDhtPartitionSupplyMessage.class, backNames.get(0));

        ListeningTestLogger testLog = new ListeningTestLogger(prim.log());

        // Ensures the rebalance was historical.
        LogListener rebalanceLsnr = LogListener.matches("fullPartitions=[], " +
            "histPartitions=[0]").times(backupNodes).build();

        testLog.registerListener(rebalanceLsnr);

        // Restore just any backup.
        IgniteEx backup = startGrid(backNames.get(0));

        TestRecordingCommunicationSpi.spi(prim).waitForBlocked();

        backup.events().localListen(evt -> {
            rebalanceFinished.countDown();

            return true;
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        TestRecordingCommunicationSpi.spi(prim).stopBlock();

        rebalanceFinished.await();

        rebalanceLsnr.check();

        IdleVerifyResultV2 checkRes = idleVerify(prim, DEFAULT_CACHE_NAME);
        assertFalse(checkRes.hasConflicts());
    }

    /** */
    private static void assertCounters(Object cntr, int lwm, String missed, int hwm) {
        assertEquals(cntr, "[lwm=" + lwm + ", missed=[" + (F.isEmpty(missed) ? "" : missed) + "], hwm=" + hwm +
            "]");
    }

    /**
     * Simulates IO falure on data writing.
     */
    private static class BlockableFileIOFactory implements FileIOFactory {
        /** IO Factory. */
        private final FileIOFactory factory;

        /** Blocked. */
        public volatile boolean blocked;

        /**
         * @param factory Factory.
         */
        public BlockableFileIOFactory(FileIOFactory factory) {
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new FileIODecorator(factory.create(file, modes)) {
                /** {@inheritDoc} */
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (blocked)
                        throw new IOException("Simulated IO failure.");

                    return super.write(srcBuf);
                }

                /** {@inheritDoc} */
                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    if (blocked)
                        throw new IOException();

                    return super.write(srcBuf, position);
                }

                /** {@inheritDoc} */
                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    if (blocked)
                        throw new IOException();

                    return super.write(buf, off, len);
                }
            };
        }
    }
}
