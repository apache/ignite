/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.ObjectGauge;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingSyncSelfTest.checkPartitionMapExchangeFinished;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_IN_PROGRESS_ERR_MSG;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_NODE_STOPPING_ERR_MSG;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.isSnapshotOperation;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.resolveSnapshotWorkDirectory;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Cluster-wide snapshot test.
 */
public class IgniteClusterSnapshotSelfTest extends AbstractSnapshotSelfTest {
    /** Time to wait while rebalance may happen. */
    private static final long REBALANCE_AWAIT_TIME = GridTestUtils.SF.applyLB(10_000, 3_000);

    /** Cache configuration for test. */
    private static final CacheConfiguration<Integer, Integer> atomicCcfg = new CacheConfiguration<Integer, Integer>("atomicCacheName")
        .setAtomicityMode(CacheAtomicityMode.ATOMIC)
        .setBackups(2)
        .setAffinity(new RendezvousAffinityFunction(false, CACHE_PARTS_COUNT));

    /** {@code true} if node should be started in separate jvm. */
    protected volatile boolean jvm;

    /** @throws Exception If fails. */
    @Before
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        jvm = false;
    }

    /**
     * Take snapshot from the whole cluster and check snapshot consistency when the
     * cluster tx load starts on a new topology version.
     * Note: Client nodes and server nodes not in baseline topology must not be affected.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testConsistentClusterSnapshotLoadNewTopology() throws Exception {
        int grids = 3;
        String snpName = "backup23012020";
        AtomicInteger atKey = new AtomicInteger(CACHE_KEYS_RANGE);
        AtomicInteger txKey = new AtomicInteger(CACHE_KEYS_RANGE);

        IgniteEx ignite = startGrids(grids);
        startClientGrid();

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ACTIVE);

        // Start node not in baseline.
        IgniteEx notBltIgnite = startGrid(grids);
        File locSnpDir = snp(notBltIgnite).snapshotLocalDir(SNAPSHOT_NAME);
        String notBltDirName = folderName(notBltIgnite);

        IgniteCache<Integer, Integer> atCache = ignite.createCache(atomicCcfg);

        for (int idx = 0; idx < CACHE_KEYS_RANGE; idx++) {
            atCache.put(atKey.incrementAndGet(), -1);
            ignite.cache(DEFAULT_CACHE_NAME).put(txKey.incrementAndGet(), -1);
        }

        forceCheckpoint();

        CountDownLatch loadLatch = new CountDownLatch(1);

        ignite.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            /** {@inheritDoc} */
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                if (fut.firstEvent().type() != EVT_DISCOVERY_CUSTOM_EVT)
                    return;

                // First discovery custom event will be a snapshot operation.
                assertTrue(isSnapshotOperation(fut.firstEvent()));
                assertTrue("Snapshot must use pme-free exchange", fut.context().exchangeFreeSwitch());
            }

            /** {@inheritDoc} */
            @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                if (fut.firstEvent().type() != EVT_DISCOVERY_CUSTOM_EVT)
                    return;

                DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)fut.firstEvent()).customMessage();

                assertNotNull(msg);

                if (msg instanceof SnapshotDiscoveryMessage)
                    loadLatch.countDown();
            }
        });

        // Start cache load.
        IgniteInternalFuture<Long> loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                U.await(loadLatch);

                while (!Thread.currentThread().isInterrupted()) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int atIdx = rnd.nextInt(grids);

                    // Zero out the sign bit.
                    grid(atIdx).cache(atomicCcfg.getName()).put(txKey.incrementAndGet(), rnd.nextInt() & Integer.MAX_VALUE);

                    int txIdx = rnd.nextInt(grids);

                    grid(txIdx).cache(DEFAULT_CACHE_NAME).put(atKey.incrementAndGet(), rnd.nextInt() & Integer.MAX_VALUE);
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }
        }, 3, "cache-put-");

        try {
            IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(snpName);

            U.await(loadLatch, 10, TimeUnit.SECONDS);

            fut.get();
        }
        finally {
            loadFut.cancel();
        }

        // Cluster can be deactivated but we must test snapshot restore when binary recovery also occurred.
        stopAllGrids();

        assertTrue("Snapshot directory must be empty for node not in baseline topology: " + notBltDirName,
            !searchDirectoryRecursively(locSnpDir.toPath(), notBltDirName).isPresent());

        IgniteEx snpIg0 = startGridsFromSnapshot(grids, snpName);

        assertEquals("The number of all (primary + backup) cache keys mismatch for cache: " + DEFAULT_CACHE_NAME,
            CACHE_KEYS_RANGE, snpIg0.cache(DEFAULT_CACHE_NAME).size());

        assertEquals("The number of all (primary + backup) cache keys mismatch for cache: " + atomicCcfg.getName(),
            CACHE_KEYS_RANGE, snpIg0.cache(atomicCcfg.getName()).size());

        snpIg0.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>(null))
            .forEach(e -> assertTrue("Snapshot must contains only negative values " +
                "[cache=" + DEFAULT_CACHE_NAME + ", entry=" + e + ']', (Integer)e.getValue() < 0));

        snpIg0.cache(atomicCcfg.getName()).query(new ScanQuery<>(null))
            .forEach(e -> assertTrue("Snapshot must contains only negative values " +
                "[cache=" + atomicCcfg.getName() + ", entry=" + e + ']', (Integer)e.getValue() < 0));
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotPrimaryBackupsTheSame() throws Exception {
        int grids = 3;
        AtomicInteger cacheKey = new AtomicInteger();

        IgniteEx ignite = startGridsWithCache(grids, dfltCacheCfg, CACHE_KEYS_RANGE);

        IgniteInternalFuture<Long> atLoadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int gId = rnd.nextInt(grids);

                IgniteCache<Integer, Integer> txCache = grid(gId).getOrCreateCache(dfltCacheCfg.getName());

                try (Transaction tx = grid(gId).transactions().txStart()) {
                    txCache.put(cacheKey.incrementAndGet(), 0);

                    txCache.put(cacheKey.incrementAndGet(), 1);

                    tx.commit();
                }
            }
        }, 5, "tx-cache-put-");

        IgniteInternalFuture<Long> txLoadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                IgniteCache<Integer, Integer> atomicCache = grid(rnd.nextInt(grids))
                    .getOrCreateCache(atomicCcfg);

                atomicCache.put(cacheKey.incrementAndGet(), 0);
            }
        }, 5, "atomic-cache-put-");

        try {
            IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

            fut.get();
        }
        finally {
            txLoadFut.cancel();
            atLoadFut.cancel();
        }

        stopAllGrids();

        IgniteEx snpIg0 = startGridsFromSnapshot(grids, cfg -> resolveSnapshotWorkDirectory(cfg).getAbsolutePath(), SNAPSHOT_NAME, false);

        // Block whole rebalancing.
        for (Ignite g : G.allGrids())
            TestRecordingCommunicationSpi.spi(g).blockMessages((node, msg) -> msg instanceof GridDhtPartitionDemandMessage);

        snpIg0.cluster().state(ACTIVE);

        assertFalse("Primary and backup in snapshot must have the same counters. Rebalance must not happen.",
            GridTestUtils.waitForCondition(() -> {
                boolean hasMsgs = false;

                for (Ignite g : G.allGrids())
                    hasMsgs |= TestRecordingCommunicationSpi.spi(g).hasBlockedMessages();

                return hasMsgs;
            }, REBALANCE_AWAIT_TIME));

        TestRecordingCommunicationSpi.stopBlockAll();

        assertPartitionsSame(idleVerify(snpIg0, dfltCacheCfg.getName(), atomicCcfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotConsistencyUnderLoad() throws Exception {
        int clientsCnt = 50;
        int balance = 10_000;
        int transferLimit = 1000;
        int total = clientsCnt * balance * 2;
        int grids = 3;
        int transferThreadCnt = 4;
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch txStarted = new CountDownLatch(1);

        CacheConfiguration<Integer, Account> eastCcfg = txCacheConfig(new CacheConfiguration<>("east"));
        CacheConfiguration<Integer, Account> westCcfg = txCacheConfig(new CacheConfiguration<>("west"));

        startGridsWithCache(grids, clientsCnt, key -> new Account(key, balance), eastCcfg, westCcfg);

        Ignite client = startClientGrid(grids);

        assertEquals("The initial summary value in all caches is not correct.",
            total, sumAllCacheValues(client, clientsCnt, eastCcfg.getName(), westCcfg.getName()));

        forceCheckpoint();

        IgniteInternalFuture<?> txLoadFut = GridTestUtils.runMultiThreadedAsync(
            () -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int amount;

                try {
                    while (!stop.get()) {
                        IgniteEx ignite = grid(rnd.nextInt(grids));
                        IgniteCache<Integer, Account> east = ignite.cache("east");
                        IgniteCache<Integer, Account> west = ignite.cache("west");

                        amount = rnd.nextInt(transferLimit);

                        txStarted.countDown();

                        try (Transaction tx = ignite.transactions().txStart()) {
                            Integer id = rnd.nextInt(clientsCnt);

                            Account acc0 = east.get(id);
                            Account acc1 = west.get(id);

                            acc0.balance -= amount;
                            acc1.balance += amount;

                            east.put(id, acc0);
                            west.put(id, acc1);

                            tx.commit();
                        }
                    }
                }
                catch (Throwable e) {
                    U.error(log, e);

                    fail("Tx must not be failed.");
                }
            }, transferThreadCnt, "transfer-account-thread-");

        try {
            U.await(txStarted);

            grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get();
        }
        finally {
            stop.set(true);
        }

        txLoadFut.get();

        assertEquals("The summary value should not changed during tx transfers.",
            total, sumAllCacheValues(client, clientsCnt, eastCcfg.getName(), westCcfg.getName()));

        stopAllGrids();

        IgniteEx snpIg0 = startGridsFromSnapshot(grids, SNAPSHOT_NAME);

        assertEquals("The total amount of all cache values must not changed in snapshot.",
            total, sumAllCacheValues(snpIg0, clientsCnt, eastCcfg.getName(), westCcfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithCacheNodeFilter() throws Exception {
        int grids = 4;

        CacheConfiguration<Integer, Integer> ccfg = txCacheConfig(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME))
            .setNodeFilter(node -> node.consistentId().toString().endsWith("1"));

        IgniteEx ig0 = startGridsWithoutCache(grids);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.getOrCreateCache(ccfg).put(i, i);

        ig0.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(grids,
            cfg -> resolveSnapshotWorkDirectory(cfg.setCacheConfiguration()).getAbsolutePath(),
            SNAPSHOT_NAME,
            true);

        awaitPartitionMapExchange();
        checkCacheDiscoveryDataConsistent();

        CacheGroupDescriptor descr = snp.context().cache().cacheGroupDescriptors()
            .get(CU.cacheId(ccfg.getName()));

        assertNotNull(descr);
        assertNotNull(descr.config().getNodeFilter());
        assertEquals(ccfg.getNodeFilter().apply(grid(1).localNode()),
            descr.config().getNodeFilter().apply(grid(1).localNode()));
        assertSnapshotCacheKeys(snp.cache(ccfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testRejectCacheStopDuringClusterSnapshot() throws Exception {
        // Block the full message, so cluster-wide snapshot operation would not be fully completed.
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        BlockingCustomMessageDiscoverySpi spi = discoSpi(ignite);
        spi.block((msg) -> {
            if (msg instanceof FullMessage) {
                FullMessage<?> msg0 = (FullMessage<?>)msg;

                assertEquals("Snapshot distributed process must be used",
                    DistributedProcess.DistributedProcessType.START_SNAPSHOT.ordinal(), msg0.type());

                assertTrue("Snapshot has to be finished successfully on all nodes", msg0.error().isEmpty());

                return true;
            }

            return false;
        });

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        spi.waitBlocked(10_000L);

        // Creating of new caches should not be blocked.
        ignite.getOrCreateCache(dfltCacheCfg.setName("default2"))
            .put(1, 1);

        forceCheckpoint();

        assertThrowsAnyCause(log,
            () -> {
                ignite.destroyCache(DEFAULT_CACHE_NAME);

                return 0;
            },
            IgniteCheckedException.class,
            SNP_IN_PROGRESS_ERR_MSG);

        spi.unblock();

        fut.get();
    }

    /** @throws Exception If fails. */
    @Test
    public void testBltChangeDuringClusterSnapshot() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        startGrid(3);

        long topVer = ignite.cluster().topologyVersion();

        BlockingCustomMessageDiscoverySpi spi = discoSpi(ignite);
        spi.block((msg) -> msg instanceof FullMessage);

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        spi.waitBlocked(10_000L);

        // Not baseline node joins successfully.
        String grid4Dir = folderName(startGrid(4));

        // Not blt node left the cluster and snapshot not affected.
        stopGrid(4);

        // Client node must connect successfully.
        startClientGrid(4);

        // Changing baseline complete successfully.
        ignite.cluster().setBaselineTopology(topVer);

        spi.unblock();

        fut.get();

        assertTrue("Snapshot directory must be empty for node 0 due to snapshot future fail: " + grid4Dir,
            !searchDirectoryRecursively(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath(), grid4Dir).isPresent());
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotExOnInitiatorLeft() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        BlockingCustomMessageDiscoverySpi spi = discoSpi(ignite);
        spi.block((msg) -> msg instanceof FullMessage);

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        spi.waitBlocked(10_000L);

        ignite.close();

        assertThrowsAnyCause(log,
            fut::get,
            NodeStoppingException.class,
            SNP_NODE_STOPPING_ERR_MSG);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotExistsException() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        assertThrowsAnyCause(log,
            () -> ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(),
            IgniteException.class,
            "Snapshot with given name already exists on local node.");

        stopAllGrids();

        // Check that snapshot has not been accidentally deleted.
        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCleanedOnLeft() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch partProcessed = new CountDownLatch(1);

        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        File locSnpDir = snp(ignite).snapshotLocalDir(SNAPSHOT_NAME);
        String dirNameIgnite0 = folderName(ignite);

        String dirNameIgnite1 = folderName(grid(1));

        snp(grid(1)).localSnapshotSenderFactory(
            blockingLocalSnapshotSender(grid(1), partProcessed, block));

        TestRecordingCommunicationSpi commSpi1 = TestRecordingCommunicationSpi.spi(grid(1));
        commSpi1.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        IgniteFuture<?> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        U.await(partProcessed);

        stopGrid(1);

        block.countDown();

        assertThrowsAnyCause(log,
            fut::get,
            IgniteCheckedException.class,
            "Execution of local snapshot tasks fails");

        assertTrue("Snapshot directory must be empty for node 0 due to snapshot future fail: " + dirNameIgnite0,
            !searchDirectoryRecursively(locSnpDir.toPath(), dirNameIgnite0).isPresent());

        startGrid(1);

        awaitPartitionMapExchange();

        // Snapshot directory must be cleaned.
        assertTrue("Snapshot directory must be empty for node 1 due to snapshot future fail: " + dirNameIgnite1,
            !searchDirectoryRecursively(locSnpDir.toPath(), dirNameIgnite1).isPresent());

        List<String> allSnapshots = snp(ignite).localSnapshotNames();

        assertTrue("Snapshot directory must be empty due to snapshot fail: " + allSnapshots,
            allSnapshots.isEmpty());
    }

    /** @throws Exception If fails. */
    @Test
    public void testRecoveryClusterSnapshotJvmHalted() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        String grid0Dir = folderName(ignite);
        String grid1Dir = folderName(grid(1));
        File locSnpDir = snp(ignite).snapshotLocalDir(SNAPSHOT_NAME);

        jvm = true;

        IgniteConfiguration cfg2 = optimize(getConfiguration(getTestIgniteInstanceName(2)));

        cfg2.getDataStorageConfiguration()
            .setFileIOFactory(new HaltJvmFileIOFactory(new RandomAccessFileIOFactory(),
                (Predicate<File> & Serializable) file -> {
                    // Trying to create FileIO over partition file.
                    return file.getAbsolutePath().contains(SNAPSHOT_NAME);
                }));

        startGrid(cfg2);

        String grid2Dir = U.maskForFileName(cfg2.getConsistentId().toString());

        jvm = false;

        ignite.cluster().setBaselineTopology(ignite.cluster().topologyVersion());

        awaitPartitionMapExchange();

        assertThrowsAnyCause(log,
            () -> ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(),
            IgniteCheckedException.class,
            "Execution of local snapshot tasks fails");

        assertTrue("Snapshot directory must be empty: " + grid0Dir,
            !searchDirectoryRecursively(locSnpDir.toPath(), grid0Dir).isPresent());

        assertTrue("Snapshot directory must be empty: " + grid1Dir,
            !searchDirectoryRecursively(locSnpDir.toPath(), grid1Dir).isPresent());

        assertTrue("Snapshot directory must exist due to grid2 has been halted and cleanup not fully performed: " + grid2Dir,
            searchDirectoryRecursively(locSnpDir.toPath(), grid2Dir).isPresent());

        IgniteEx grid2 = startGrid(2);

        assertTrue("Snapshot directory must be empty after recovery: " + grid2Dir,
            !searchDirectoryRecursively(locSnpDir.toPath(), grid2Dir).isPresent());

        awaitPartitionMapExchange();

        assertTrue("Snapshot directory must be empty", grid2.context().cache().context().snapshotMgr().localSnapshotNames().isEmpty());

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithRebalancing() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(ignite);
        commSpi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage);

        startGrid(2);

        ignite.cluster().setBaselineTopology(ignite.cluster().topologyVersion());

        commSpi.waitForBlocked();

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        commSpi.stopBlock(true);

        fut.get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(3, SNAPSHOT_NAME);

        awaitPartitionMapExchange();
        checkPartitionMapExchangeFinished();

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithExplicitPath() throws Exception {
        File exSnpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);

        try {
            IgniteEx ignite = null;

            for (int i = 0; i < 2; i++) {
                IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(i)));

                cfg.setSnapshotPath(exSnpDir.getAbsolutePath());

                ignite = startGrid(cfg);
            }

            ignite.cluster().baselineAutoAdjustEnabled(false);
            ignite.cluster().state(ACTIVE);

            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ignite.cache(DEFAULT_CACHE_NAME).put(i, i);

            ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
                .get();

            stopAllGrids();

            IgniteEx snp = startGridsFromSnapshot(2, cfg -> exSnpDir.getAbsolutePath(), SNAPSHOT_NAME, true);

            assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
        }
        finally {
            stopAllGrids();

            U.delete(exSnpDir);
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotMetrics() throws Exception {
        String newSnapshotName = SNAPSHOT_NAME + "_new";
        CountDownLatch deltaApply = new CountDownLatch(1);
        CountDownLatch deltaBlock = new CountDownLatch(1);
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        MetricRegistry mreg0 = ignite.context().metric().registry(SNAPSHOT_METRICS);

        LongMetric startTime = mreg0.findMetric("LastSnapshotStartTime");
        LongMetric endTime = mreg0.findMetric("LastSnapshotEndTime");
        ObjectGauge<String> snpName = mreg0.findMetric("LastSnapshotName");
        ObjectGauge<String> errMsg = mreg0.findMetric("LastSnapshotErrorMessage");
        ObjectGauge<List<String>> snpList = mreg0.findMetric("LocalSnapshotNames");

        // Snapshot process will be blocked when delta partition files processing starts.
        snp(ignite).localSnapshotSenderFactory(
            blockingLocalSnapshotSender(ignite, deltaApply, deltaBlock));

        assertEquals("Snapshot start time must be undefined prior to snapshot operation started.",
            0, startTime.value());
        assertEquals("Snapshot end time must be undefined to snapshot operation started.",
            0, endTime.value());
        assertTrue("Snapshot name must not exist prior to snapshot operation started.", snpName.value().isEmpty());
        assertTrue("Snapshot error message must null prior to snapshot operation started.", errMsg.value().isEmpty());
        assertTrue("Snapshots on local node must not exist", snpList.value().isEmpty());

        long cutoffStartTime = U.currentTimeMillis();

        IgniteFuture<Void> fut0 = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        U.await(deltaApply);

        assertTrue("Snapshot start time must be set prior to snapshot operation started " +
            "[startTime=" + startTime.value() + ", cutoffTime=" + cutoffStartTime + ']',
            startTime.value() >= cutoffStartTime);
        assertEquals("Snapshot end time must be zero prior to snapshot operation started.",
            0, endTime.value());
        assertEquals("Snapshot name must be set prior to snapshot operation started.",
            SNAPSHOT_NAME, snpName.value());
        assertTrue("Snapshot error message must null prior to snapshot operation started.",
            errMsg.value().isEmpty());

        IgniteFuture<Void> fut1 = grid(1).snapshot().createSnapshot(newSnapshotName);

        assertThrowsWithCause((Callable<Object>)fut1::get, IgniteException.class);

        MetricRegistry mreg1 = grid(1).context().metric().registry(SNAPSHOT_METRICS);

        LongMetric startTime1 = mreg1.findMetric("LastSnapshotStartTime");
        LongMetric endTime1 = mreg1.findMetric("LastSnapshotEndTime");
        ObjectGauge<String> snpName1 = mreg1.findMetric("LastSnapshotName");
        ObjectGauge<String> errMsg1 = mreg1.findMetric("LastSnapshotErrorMessage");

        assertTrue("Snapshot start time must be greater than zero for finished snapshot.",
            startTime1.value() > 0);
        assertEquals("Snapshot end time must zero for failed on start snapshots.",
            0, endTime1.value());
        assertEquals("Snapshot name must be set when snapshot operation already finished.",
            newSnapshotName, snpName1.value());
        assertNotNull("Concurrent snapshot operation must failed.",
            errMsg1.value());

        deltaBlock.countDown();

        fut0.get();

        assertTrue("Snapshot start time must be greater than zero for finished snapshot.",
            startTime.value() > 0);
        assertTrue("Snapshot end time must be greater than zero for finished snapshot.",
            endTime.value() > 0);
        assertEquals("Snapshot name must be set when snapshot operation already finished.",
            SNAPSHOT_NAME, snpName.value());
        assertTrue("Concurrent snapshot operation must finished successfully.",
            errMsg.value().isEmpty());
        assertEquals("Only the first snapshot must be created and stored on disk.",
            Collections.singletonList(SNAPSHOT_NAME), snpList.value());
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotIncorrectNameFails() throws Exception {
        IgniteEx ignite = startGridsWithCache(1, dfltCacheCfg, CACHE_KEYS_RANGE);

        assertThrowsAnyCause(log,
            () -> ignite.snapshot().createSnapshot("--№=+.:(snapshot)").get(),
            IllegalArgumentException.class,
            "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithOfflineBlt() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        stopGrid(2);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        awaitPartitionMapExchange();

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
        assertPartitionsSame(idleVerify(snp, dfltCacheCfg.getName()));
    }


    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotWithSharedCacheGroup() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg1 = txCacheConfig(new CacheConfiguration<>("tx1"));
        CacheConfiguration<Integer, Integer> ccfg2 = txCacheConfig(new CacheConfiguration<>("tx2"));

        ccfg1.setGroupName("group");
        ccfg2.setGroupName("group");

        IgniteEx ignite = startGridsWithCache(3, CACHE_KEYS_RANGE, Integer::new, ccfg1, ccfg2);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(3, SNAPSHOT_NAME);

        awaitPartitionMapExchange();

        assertSnapshotCacheKeys(snp.cache(ccfg1.getName()));
        assertSnapshotCacheKeys(snp.cache(ccfg2.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCoordinatorStopped() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);
        startClientGrid(3);

        awaitPartitionMapExchange();

        for (Ignite grid : Arrays.asList(grid(1), grid(2))) {
            ((IgniteEx)grid).context().cache().context().exchange()
                .registerExchangeAwareComponent(new PartitionsExchangeAware() {
                    /** {@inheritDoc} */
                    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                        try {
                            block.await();
                        }
                        catch (InterruptedException e) {
                            fail("Must not catch exception here: " + e.getMessage());
                        }
                    }
                });
        }

        for (Ignite grid : G.allGrids()) {
            TestRecordingCommunicationSpi.spi(grid)
                .blockMessages((node, msg) -> {
                    if (msg instanceof GridDhtPartitionsSingleMessage)
                        return ((GridDhtPartitionsSingleMessage)msg).exchangeId() != null;

                    return false;
                });
        }

        IgniteFuture<Void> fut = grid(1).snapshot().createSnapshot(SNAPSHOT_NAME);

        stopGrid(0);

        block.countDown();

        // There are two exchanges happen: snapshot, node left (with pme-free).
        // Both of them are not require for sending messages.
        assertFalse("Pme-free switch doesn't expect messaging exchanging between nodes",
            GridTestUtils.waitForCondition(() -> {
                boolean hasMsgs = false;

                for (Ignite g : G.allGrids())
                    hasMsgs |= TestRecordingCommunicationSpi.spi(g).hasBlockedMessages();

                return hasMsgs;
            }, 5_000));

        assertThrowsWithCause((Callable<Object>)fut::get, IgniteException.class);

        List<GridDhtPartitionsExchangeFuture> exchFuts =
            grid(1).context().cache().context().exchange().exchangeFutures();

        assertFalse("Exchanges cannot be empty due to snapshot and node left happened",
            exchFuts.isEmpty());

        for (GridDhtPartitionsExchangeFuture exch : exchFuts) {
            assertTrue("Snapshot and node left events must keep `rebalanced` state" + exch,
                exch.rebalanced());
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotOnMovingPartitionsCoordinatorLeft() throws Exception {
        startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        for (Ignite grid : G.allGrids()) {
            TestRecordingCommunicationSpi.spi(grid)
                .blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage);
        }

        Ignite ignite = startGrid(2);

        ignite.cluster().setBaselineTopology(ignite.cluster().topologyVersion());

        TestRecordingCommunicationSpi.spi(grid(0))
            .waitForBlocked();

        CountDownLatch latch = new CountDownLatch(G.allGrids().size());
        IgniteInternalFuture<?> stopFut = GridTestUtils.runAsync(() -> {
            try {
                U.await(latch);

                stopGrid(0);
            }
            catch (IgniteInterruptedCheckedException e) {
                fail("Must not fail here: " + e.getMessage());
            }
        });

        Queue<T2<GridDhtPartitionExchangeId, Boolean>> exchFuts = new ConcurrentLinkedQueue<>();

        for (Ignite ig : G.allGrids()) {
            ((IgniteEx)ig).context().cache().context().exchange()
                .registerExchangeAwareComponent(new PartitionsExchangeAware() {
                    /** {@inheritDoc} */
                    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                        try {
                            exchFuts.add(new T2<>(fut.exchangeId(), fut.rebalanced()));
                            latch.countDown();

                            stopFut.get();
                        }
                        catch (IgniteCheckedException e) {
                            U.log(log, "Interrupted on coordinator: " + e.getMessage());
                        }
                    }
                });
        }

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        stopFut.get();

        assertThrowsAnyCause(log,
            fut::get,
            IgniteException.class,
            "Snapshot creation has been finished with an error");

        assertEquals("Snapshot futures expected: " + exchFuts, 3, exchFuts.size());

        for (T2<GridDhtPartitionExchangeId, Boolean> exch : exchFuts)
            assertFalse("Snapshot `rebalanced` must be false with moving partitions: " + exch.get1(), exch.get2());
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotPartitionExchangeAwareOrder() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        Map<UUID, PartitionsExchangeAware> comps = new HashMap<>();

        for (Ignite ig : G.allGrids()) {
            PartitionsExchangeAware comp;

            ((IgniteEx)ig).context().cache().context().exchange()
                .registerExchangeAwareComponent(comp = new PartitionsExchangeAware() {
                    private final AtomicInteger order = new AtomicInteger();

                    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                        assertEquals("Exchange order violated: " + fut.firstEvent(), 0, order.getAndIncrement());
                    }

                    @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                        assertEquals("Exchange order violated: " + fut.firstEvent(), 1, order.getAndIncrement());
                    }

                    @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                        assertEquals("Exchange order violated: " + fut.firstEvent(), 2, order.getAndIncrement());
                    }

                    @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                        assertEquals("Exchange order violated: " + fut.firstEvent(), 3, order.getAndSet(0));
                    }
                });

            comps.put(((IgniteEx)ig).localNode().id(), comp);
        }

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        for (Ignite ig : G.allGrids()) {
            ((IgniteEx)ig).context().cache().context().exchange()
                .unregisterExchangeAwareComponent(comps.get(((IgniteEx)ig).localNode().id()));
        }

        awaitPartitionMapExchange();

        assertEquals("Some of ignite instances failed during snapshot", 3, G.allGrids().size());

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(3, SNAPSHOT_NAME);

        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotFromClient() throws Exception {
        startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);
        IgniteEx clnt = startClientGrid(2);

        clnt.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        stopAllGrids();

        IgniteEx snp = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        awaitPartitionMapExchange();
        assertSnapshotCacheKeys(snp.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testConcurrentClusterSnapshotFromClient() throws Exception {
        IgniteEx grid = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        IgniteEx clnt = startClientGrid(2);

        IgniteSnapshotManager mgr = snp(grid);
        Function<String, SnapshotSender> old = mgr.localSnapshotSenderFactory();

        BlockingExecutor block = new BlockingExecutor(mgr.snapshotExecutorService());

        mgr.localSnapshotSenderFactory((snpName) ->
            new DelegateSnapshotSender(log, block, old.apply(snpName)));

        IgniteFuture<Void> fut = grid.snapshot().createSnapshot(SNAPSHOT_NAME);

        assertThrowsAnyCause(log,
            () -> clnt.snapshot().createSnapshot(SNAPSHOT_NAME).get(),
            IgniteException.class,
            "Snapshot has not been created");

        block.unblock();
        fut.get();
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotFromClientDisconnected() throws Exception {
        startGridsWithCache(1, dfltCacheCfg, CACHE_KEYS_RANGE);
        IgniteEx clnt = startClientGrid(1);

        stopGrid(0);

        assertThrowsAnyCause(log,
            () -> clnt.snapshot().createSnapshot(SNAPSHOT_NAME).get(),
            IgniteException.class,
            "Client disconnected. Snapshot result is unknown");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotInProgressCancelled() throws Exception {
        IgniteEx srv = startGridsWithCache(1, dfltCacheCfg, CACHE_KEYS_RANGE);
        IgniteEx startCli = startClientGrid(1);
        IgniteEx killCli = startClientGrid(2);

        doSnapshotCancellationTest(startCli, Collections.singletonList(srv), srv.cache(dfltCacheCfg.getName()),
            snpName -> killCli.snapshot().cancelSnapshot(snpName).get());
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotFinishedTryCancel() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();
        ignite.snapshot().cancelSnapshot(SNAPSHOT_NAME).get();

        stopAllGrids();

        IgniteEx snpIg = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertSnapshotCacheKeys(snpIg.cache(dfltCacheCfg.getName()));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotInMemoryFail() throws Exception {
        persistence = false;

        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        IgniteEx clnt = startClientGrid(1);

        IgniteFuture<?> fut = clnt.snapshot().createSnapshot(SNAPSHOT_NAME);

        assertThrowsAnyCause(log,
            fut::get,
            IgniteException.class,
            "Snapshots on an in-memory clusters are not allowed.");
    }

    /**
     * @param ignite Ignite instance.
     * @param started Latch will be released when delta partition processing starts.
     * @param blocked Latch to await delta partition processing.
     * @return Factory which produces local snapshot senders.
     */
    private Function<String, SnapshotSender> blockingLocalSnapshotSender(IgniteEx ignite,
        CountDownLatch started,
        CountDownLatch blocked
    ) {
        Function<String, SnapshotSender> old = snp(ignite).localSnapshotSenderFactory();

        return (snpName) -> new DelegateSnapshotSender(log, snp(ignite).snapshotExecutorService(), old.apply(snpName)) {
            @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
                if (log.isInfoEnabled())
                    log.info("Processing delta file has been blocked: " + delta.getName());

                started.countDown();

                try {
                    U.await(blocked);

                    if (log.isInfoEnabled())
                        log.info("Latch released. Processing delta file continued: " + delta.getName());

                    super.sendDelta0(delta, cacheDirName, pair);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteException("Interrupted by node stop", e);
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return jvm;
    }

    /**
     * @param ignite Ignite instance.
     * @param caches Cache names to read values.
     * @return Summary value.
     */
    private static int sumAllCacheValues(Ignite ignite, int keys, String... caches) {
        AtomicInteger total = new AtomicInteger();

        for (String name : caches) {
            IgniteCache<Integer, Account> cache = ignite.cache(name);

            for (int key = 0; key < keys; key++)
                total.addAndGet(cache.get(key).balance);
        }

        return total.get();
    }

    /**
     * I/O Factory which will halt JVM on conditions occurred.
     */
    private static class HaltJvmFileIOFactory implements FileIOFactory {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Delegate factory. */
        private final FileIOFactory delegate;

        /** Condition to halt. */
        private final Predicate<File> pred;

        /**
         * @param delegate Delegate factory.
         */
        public HaltJvmFileIOFactory(FileIOFactory delegate, Predicate<File> pred) {
            this.delegate = delegate;
            this.pred = pred;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = this.delegate.create(file, modes);

            if (pred.test(file))
                Runtime.getRuntime().halt(Ignition.KILL_EXIT_CODE);

            return delegate;
        }
    }
}
