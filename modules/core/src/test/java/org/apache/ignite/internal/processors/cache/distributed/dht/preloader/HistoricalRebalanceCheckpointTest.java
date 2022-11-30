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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
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
     * Test two-phase commit.
     */
    @Test
    public void testCountersOnCrashRecovery2Backups() throws Exception {
        doTestTwoOnOnePhaseCommit(2, false);
    }

    /**
     * Test two-phase commit with puts after gaps.
     */
    @Test
    public void testCountersOnCrashRecovery2BackupsMorePuts() throws Exception {
        doTestTwoOnOnePhaseCommit(2, false);
    }

    /**
     * Test one-phase commit.
     */
    @Test
    public void testCountersOnCrashRecovery1Backup() throws Exception {
        doTestTwoOnOnePhaseCommit(1, false);
    }

    /**
     * Test one-phase commit with puts after gaps.
     */
    @Test
    public void testCountersOnCrashRecovery1BackupMorePuts() throws Exception {
        doTestTwoOnOnePhaseCommit(1, true);
    }

    /**
     * Test one-phase commit with lost backup responses.
     */
    @Test
    public void test() throws Exception {
        int backupNodes = 1;
        int nodes = backupNodes + 1;

        IgniteEx ignite = startGrids(nodes);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(backupNodes)
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC) // Allows to be sure that all messages are sent when put succeed.
            .setReadFromBackup(true)); // Allows checking values on backups.

        int updateCnt = 0;

        // Initial preloading enough to have historical rebalance.
        for (int i = 0; i < 2_000; i++)  //
            cache.put(++updateCnt, updateCnt);

        // To have historical rebalance on cluster recovery. Decreases percent of updates in comparison to cache size.
        stopAllGrids();
        startGrids(nodes);

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        Ignite backup = backupNodes(0L, DEFAULT_CACHE_NAME).get(0);

        AtomicBoolean prepareBlock = new AtomicBoolean();

        AtomicReference<CountDownLatch> blockLatch = new AtomicReference<>();

        TestRecordingCommunicationSpi.spi(backup).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtTxPrepareResponse && prepareBlock.get()) {
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

        // Blocked at primary and backups.
        prepareBlock.set(true);

        blockLatch.set(new CountDownLatch(backupNodes * 20));

        for (int i = 0; i < 20; i++)
            cachePutAsync.accept(++updateCnt);

        blockLatch.get().await();

        // Storing counters on primary.
        forceCheckpoint();

        // Emulating power off, OOM or disk overflow. Keeping data as is, with missed counters updates.
//        ((BlockableFileIOFactory)backup.configuration().getDataStorageConfiguration()
//            .getFileIOFactory()).blocked = true;

        String backName = backup.name();

        backup.close();

        prepareBlock.set(false);

//        TestRecordingCommunicationSpi.spi(prim).blockMessages(GridDhtPartitionDemandMessage.class, backName);
//
//        CountDownLatch rebalanceFinished = new CountDownLatch(1);
//
//        prim.events().localListen(evt -> {
//            rebalanceFinished.countDown();
//
//            return true;
//        }, EventType.EVT_CACHE_REBALANCE_STOPPED);
//
        startGrid(backName);
//
//        TestRecordingCommunicationSpi.spi(prim).waitForBlocked();
//
//        TestRecordingCommunicationSpi.spi(prim).stopBlock();
//
//        rebalanceFinished.await();

        IdleVerifyResultV2 checkRes = idleVerify(prim, DEFAULT_CACHE_NAME);

        assertFalse(checkRes.hasConflicts());
    }

    /** */
    private void doTestTwoOnOnePhaseCommit(int backupNodes, boolean putAfterGaps) throws Exception {
        assert backupNodes > 0;

        int nodes = backupNodes + 1;

        IgniteEx ignite = startGrids(nodes);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(backupNodes)
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC) // Allows to be sure that all messages are sent when put succeed.
            .setReadFromBackup(true)); // Allows checking values on backups.

        int updateCnt = 0;

        // Initial preloading enough to have historical rebalance.
        for (int i = 0; i < 2_000; i++)  //
            cache.put(++updateCnt, updateCnt);

        // To have historical rebalance on cluster recovery. Decreases percent of updates in comparison to cache size.
        stopAllGrids();
        startGrids(nodes);

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

        try {
            // Blocked at primary and backups.
            prepareBlock.set(true);

            blockLatch.set(new CountDownLatch(backupNodes * 20));

            for (int i = 0; i < 20; i++)
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

                blockLatch.set(new CountDownLatch(backupNodes * 30));

                for (int i = 0; i < 30; i++)
                    cachePutAsync.accept(++updateCnt);

                blockLatch.get().await();
            }
            finally {
                finishBlock.set(false);
            }
        }

        if (putAfterGaps) {
            for (int i = 0; i < 50; i++)
                prim.cache(DEFAULT_CACHE_NAME).put(++updateCnt, updateCnt);
        }

        // Storing counters on primary.
        forceCheckpoint();

        // Emulating power off, OOM or disk overflow. Keeping data as is, with missed counters updates.
        backups.forEach(node -> ((BlockableFileIOFactory)node.configuration().getDataStorageConfiguration()
            .getFileIOFactory()).blocked = true);

        List<String> backNames = backups.stream().map(Ignite::name).collect(Collectors.toList());

        CountDownLatch rebalanceFinished = new CountDownLatch(1);

        backups.forEach(Ignite::close);

        TestRecordingCommunicationSpi.spi(prim).blockMessages(GridDhtPartitionSupplyMessage.class, backNames.get(0));

        // Restore just any backup.
        IgniteEx backup = startGrid(backNames.get(0));

        TestRecordingCommunicationSpi.spi(prim).waitForBlocked();

        backup.events().localListen(evt -> {
            rebalanceFinished.countDown();

            return true;
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        TestRecordingCommunicationSpi.spi(prim).stopBlock();

        rebalanceFinished.await();

        IdleVerifyResultV2 checkRes = idleVerify(prim, DEFAULT_CACHE_NAME);

        assertFalse(checkRes.hasConflicts());
    }

    /** */
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
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (blocked)
                        throw new IOException("Simulated IO failure.");

                    return super.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    if (blocked)
                        throw new IOException();

                    return super.write(srcBuf, position);
                }

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    if (blocked)
                        throw new IOException();

                    return super.write(buf, off, len);
                }
            };
        }
    }
}
