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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toCollection;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/**
 * Test framework for ordering transaction's prepares and commits by intercepting messages and releasing then
 * in user defined order.
 */
public abstract class TxPartitionCounterStateAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    protected static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    protected int backups;

    /** */
    public static final int TEST_TIMEOUT = 30_000;

    /** */
    private AtomicReference<Throwable> testFailed = new AtomicReference<>();

    /** Number of keys to preload before txs to enable historical rebalance. */
    protected static final int PRELOAD_KEYS_CNT = 1;

    /** */
    protected static final String CLIENT_GRID_NAME = "client";

    /** */
    protected static final int PARTS_CNT = 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(false);
        cfg.setAutoActivationEnabled(false);

        cfg.setConsistentId("node" + igniteInstanceName);
        cfg.setFailureHandler(new StopNodeFailureHandler());
        cfg.setRebalanceThreadPoolSize(4); // Necessary to reproduce some issues.

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        // TODO set this only for historical rebalance tests.
        cfg.setCommunicationSpi(new IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setWalHistorySize(1000).
            setWalSegmentSize(8 * MB).setWalMode(LOG_ONLY).setPageSize(1024).
            setCheckpointFrequency(MILLISECONDS.convert(365, DAYS)).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled()).
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        if (!igniteInstanceName.startsWith(CLIENT_GRID_NAME))
            cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /**
     * @return Partitions count.
     */
    protected int partitions() {
        return PARTS_CNT;
    }

    /**
     * @return Default tx concurrency.
     */
    protected TransactionConcurrency concurrency() {
        return PESSIMISTIC;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
    }

    /** */
    protected boolean persistenceEnabled() {
        return true;
    }

    /**
     * @param name Name.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setOnheapCacheEnabled(false);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, partitions()));

        return ccfg;
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
     */
    protected void configureBaselineAutoAdjust() {
        ignite(0).cluster().baselineAutoAdjustEnabled(false);
    }

    /**
     * Runs a scenario.
     *
     * @param partId Partition id.
     * @param part2Sup Optional second partition supplier.
     * @param nodesCnt Nodes count.
     * @param clo Callback closure which produces {@link TxCallback}.
     * @param sizes Sizes.
     */
    protected Map<Integer, T2<Ignite, List<Ignite>>> runOnPartition(
        int partId,
        @Nullable Supplier<Integer> part2Sup,
        int backups,
        int nodesCnt,
        IgniteClosure<Map<Integer, T2<Ignite, List<Ignite>>>, TxCallback> clo,
        int[] sizes) throws Exception {
        this.backups = backups;

        IgniteEx crd = startGrids(nodesCnt);

        crd.cluster().active(true);

        configureBaselineAutoAdjust();

        assertEquals(0, crd.cache(DEFAULT_CACHE_NAME).size());

        int[][] ranges = new int[sizes.length][2];

        int totalKeys = 0;

        for (int i = 0; i < sizes.length; i++) {
            int size = sizes[i];

            ranges[i] = new int[] {totalKeys, size};

            totalKeys += size;
        }

        IgniteEx client = startClientGrid(CLIENT_GRID_NAME);

        // Preload one key to partition to enable historical rebalance.
        List<Integer> preloadKeys = loadDataToPartition(partId, "client", DEFAULT_CACHE_NAME, PRELOAD_KEYS_CNT, 0);

        forceCheckpoint();

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        List<Integer> keys = partitionKeys(crd.cache(DEFAULT_CACHE_NAME), partId, totalKeys, PRELOAD_KEYS_CNT);

        assertFalse(preloadKeys.get(0).equals(keys.get(0)));

        Ignite prim = primaryNode(keys.get(0), DEFAULT_CACHE_NAME);

        List<Ignite> backupz = backups == 0 ? null : backups == 1 ?
                Collections.singletonList(backupNode(keys.get(0), DEFAULT_CACHE_NAME)) :
                backupNodes(keys.get(0), DEFAULT_CACHE_NAME);

        final TestRecordingCommunicationSpi clientWrappedSpi = spi(client);

        Map<IgniteUuid, GridCacheVersion> futMap = new ConcurrentHashMap<>();
        Map<GridCacheVersion, GridCacheVersion> nearToLocVerMap = new ConcurrentHashMap<>();

        Map<Integer, T2<Ignite, List<Ignite>>> txTop = new HashMap<>();

        txTop.put(partId, new T2<>(prim, backupz));

        List<Integer> keysPart2 = part2Sup == null ? null :
            partitionKeys(crd.cache(DEFAULT_CACHE_NAME), part2Sup.get(), sizes.length, 0);

        log.info("TX: topology [part1=" + partId + ", primary=" + prim.name() +
            ", backups=" + F.transform(backupz, Ignite::name));

        if (part2Sup != null) {
            int partId2 = part2Sup.get();

            Ignite prim2 = primaryNode(keysPart2.get(0), DEFAULT_CACHE_NAME);

            assertNotSame(prim, prim2);

            List<Ignite> backupz2 = backupNodes(keysPart2.get(0), DEFAULT_CACHE_NAME);

            txTop.put(partId2, new T2<>(prim2, backupz2));

            log.info("TX: topology [part2=" + partId2 + ", primary=" + prim2.name() +
                ", backups=" + F.transform(backupz2, Ignite::name));
        }

        TxCallback cb = clo.apply(txTop);

        clientWrappedSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridNearTxPrepareRequest) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)msg;

                if (!req.last())
                    return false;

                futMap.put(req.futureId(), req.version());

                return cb.beforePrimaryPrepare(to, req.version().asIgniteUuid(), createSendFuture(clientWrappedSpi, msg));
            }
            else if (msg instanceof GridNearTxFinishRequest) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                GridNearTxFinishRequest req = (GridNearTxFinishRequest)msg;

                futMap.put(req.futureId(), req.version());

                IgniteInternalTx tx = findTx(to, req.version(), true);

                assertNotNull(tx);

                return cb.beforePrimaryFinish(to, tx, createSendFuture(clientWrappedSpi, msg));
            }

            return false;
        });

        spi(prim).blockMessages(createPrimaryMessagePredicate(spi(prim), futMap, nearToLocVerMap, cb));

        if (part2Sup != null) {
            Ignite prim2 = txTop.get(part2Sup.get()).get1();

            spi(prim2).blockMessages(createPrimaryMessagePredicate(spi(prim2), futMap, nearToLocVerMap, cb));
        }

        if (backupz != null) {
            for (Ignite backup : backupz)
                spi(backup).blockMessages(
                    createBackupMessagePredicate(spi(backup), futMap, cb));

            if (part2Sup != null) {
                for (Ignite backup : txTop.get(part2Sup.get()).get2())
                    spi(backup).blockMessages(
                        createBackupMessagePredicate(spi(backup), futMap, cb));
            }
        }

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        AtomicInteger idx = new AtomicInteger();

        CyclicBarrier b = new CyclicBarrier(sizes.length);

        IgniteInternalFuture<Long> fut = runMultiThreadedAsync(() -> {
            int txIdx = idx.getAndIncrement();

            int[] range = ranges[txIdx];

            String lb = "t" + txIdx;

            try (Transaction tx = client.transactions().withLabel(lb).txStart()) {
                cb.onTxStart(tx, txIdx);

                U.awaitQuiet(b); // Wait should always success.

                for (Integer key : keys.subList(range[0], range[0] + range[1]))
                    client.cache(DEFAULT_CACHE_NAME).put(key, 0);

                if (keysPart2 != null) { // Force 2PC.
                    client.cache(DEFAULT_CACHE_NAME).put(keysPart2.get(txIdx), 0);
                }

                tx.commit();
            }
            catch (Throwable ignored) {
                // No-op.
            }
        }, sizes.length, "tx-thread");

        try {
            fut.get(TEST_TIMEOUT); // TODO verify all created futures.
        }
        catch (IgniteCheckedException e) {
            Throwable err = testFailed.get();

            if (err != null)
                log.error("Test execution failed", err);

            fail("Test is timed out");
        }

        return txTop;
    }

    /**
     * @param wrappedPrimSpi Wrapped prim spi.
     * @param futMap Future map.
     * @param nearToLocVerMap Near to local version map.
     * @param cb Callback.
     */
    private IgniteBiPredicate<ClusterNode, Message> createPrimaryMessagePredicate(TestRecordingCommunicationSpi wrappedPrimSpi,
        Map<IgniteUuid, GridCacheVersion> futMap,
        Map<GridCacheVersion, GridCacheVersion> nearToLocVerMap,
        TxCallback cb) {
        return (node, msg) -> {
            if (msg instanceof GridDhtTxPrepareRequest) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                GridDhtTxPrepareRequest req = (GridDhtTxPrepareRequest)msg;

                if (!req.last())
                    return false;

                futMap.put(req.futureId(), req.nearXidVersion());
                nearToLocVerMap.put(req.version(), req.nearXidVersion());

                IgniteEx from = fromNode(wrappedPrimSpi);

                IgniteInternalTx primTx = findTx(from, req.nearXidVersion(), true);

                return cb.beforeBackupPrepare(from, to, primTx, createSendFuture(wrappedPrimSpi, msg));
            }
            else if (msg instanceof GridDhtTxFinishRequest) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                GridDhtTxFinishRequest req = (GridDhtTxFinishRequest)msg;

                GridCacheVersion nearVer = nearToLocVerMap.get(req.version());
                futMap.put(req.futureId(), nearVer);

                IgniteEx from = fromNode(wrappedPrimSpi);

                IgniteInternalTx primTx = findTx(from, nearVer, true);
                IgniteInternalTx backupTx = findTx(to, nearVer, false);

                return cb.beforeBackupFinish(from, to, primTx, backupTx, nearVer.asIgniteUuid(), createSendFuture(wrappedPrimSpi, msg));
            }
            else if (msg instanceof GridNearTxPrepareResponse) {
                GridNearTxPrepareResponse resp = (GridNearTxPrepareResponse)msg;

                IgniteEx from = fromNode(wrappedPrimSpi);

                GridCacheVersion ver = futMap.get(resp.futureId());

                IgniteInternalTx primTx = findTx(from, ver, true);

                return cb.afterPrimaryPrepare(from, primTx, ver.asIgniteUuid(), createSendFuture(wrappedPrimSpi, msg));
            }
            else if (msg instanceof GridNearTxFinishResponse) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                GridNearTxFinishResponse req = (GridNearTxFinishResponse)msg;

                IgniteEx from = fromNode(wrappedPrimSpi);

                IgniteUuid nearVer = futMap.get(req.futureId()).asIgniteUuid();

                return cb.afterPrimaryFinish(from, nearVer, createSendFuture(wrappedPrimSpi, msg));
            }

            return false;
        };
    }

    /**
     * @param wrappedBackupSpi Wrapped backup spi.
     * @param futMap Future map.
     * @param cb Callback.
     */
    private IgniteBiPredicate<ClusterNode, Message> createBackupMessagePredicate(TestRecordingCommunicationSpi wrappedBackupSpi,
        Map<IgniteUuid, GridCacheVersion> futMap, TxCallback cb) {
        return (node, msg) -> {
            if (msg instanceof GridDhtTxPrepareResponse) {
                IgniteEx from = fromNode(wrappedBackupSpi);
                IgniteEx to = IgnitionEx.gridxx(node.id());

                GridDhtTxPrepareResponse resp = (GridDhtTxPrepareResponse)msg;

                GridCacheVersion ver = futMap.get(resp.futureId());

                if (ver == null)
                    return false; // Message from parallel partition.

                IgniteInternalTx backupTx = findTx(from, ver, false);

                return cb.afterBackupPrepare(to, from, backupTx, ver.asIgniteUuid(), createSendFuture(wrappedBackupSpi, msg));
            }
            else if (msg instanceof GridDhtTxFinishResponse) {
                IgniteEx from = fromNode(wrappedBackupSpi);
                IgniteEx to = IgnitionEx.gridxx(node.id());

                GridDhtTxFinishResponse resp = (GridDhtTxFinishResponse)msg;

                GridCacheVersion ver = futMap.get(resp.futureId());

                if (ver == null)
                    return false; // Message from parallel partition.

                // Version is null if message is a response to checkCommittedRequest.
                return cb.afterBackupFinish(to, from, ver.asIgniteUuid(), createSendFuture(wrappedBackupSpi, msg));
            }

            return false;
        };
    }

    /**
     * @param wrapperSpi Wrapper spi.
     * @param msg Message.
     */
    private GridFutureAdapter<?> createSendFuture(TestRecordingCommunicationSpi wrapperSpi, Message msg) {
        GridFutureAdapter<?> fut = new GridFutureAdapter<>();

        fut.listen(fut1 -> wrapperSpi.stopBlock(true, objects -> objects.get2().message() == msg, false, true));

        return fut;
    }

    /**
     * Defines logic of tx messages delivery ordering.
     */
    protected interface TxCallback {
        /**
         * @param primary Primary.
         * @param nearXidVer Near xid version.
         * @param proceedFut Proceed future.
         */
        public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut);

        /**
         * @param primary Primary node.
         * @param backup Backup prim.
         * @param primaryTx Primary tx.
         * @param proceedFut Proceed future.
         */
        public boolean beforeBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx primaryTx,
            GridFutureAdapter<?> proceedFut);

        /**
         * @param primary Primary.
         * @param tx Tx.
         * @param proceedFut Proceed future.
         */
        public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx, GridFutureAdapter<?> proceedFut);

        /**
         * @param primary Primary.
         * @param nearXidVer Near xid version.
         * @param proceedFut Proceed future.
         */
        public boolean afterPrimaryFinish(IgniteEx primary, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut);

        /**
         * @param primary Primary.
         * @param backup Backup.
         * @param backupTx Backup tx.
         * @param nearXidVer Near xid version.
         * @param proceedFut Future.
         */
        public boolean afterBackupPrepare(IgniteEx primary, IgniteEx backup, @Nullable IgniteInternalTx backupTx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut);

        /**
         * @param primary Primary.
         * @param backup Backup.
         * @param nearXidVer Near xid version.
         * @param proceedFut Future.
         */
        public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut);

        /**
         * @param primary Prim.
         * @param backup Backup.
         * @param primaryTx Prim tx. Null for 2pc.
         * @param backupTx Backup tx.
         * @param nearXidVer Near xid version.
         * @param proceedFut Future.
         */
        public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup, @Nullable IgniteInternalTx primaryTx,
            IgniteInternalTx backupTx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut);

        /**
         * @param primary Primary.
         * @param tx Tx or null for one-phase commit.
         * @param nearXidVer Near xid version.
         * @param proceedFut Future.
         */
        public boolean afterPrimaryPrepare(IgniteEx primary, @Nullable IgniteInternalTx tx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut);

        /**
         * Called when transaction got an order assignment.
         *
         * @param tx Tx.
         * @param idx Index.
         */
        void onTxStart(Transaction tx, int idx);
    }

    /** */
    protected class TxCallbackAdapter implements TxCallback {
        /** */
        private Map<Integer, IgniteUuid> txMap = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteUuid, Integer> revTxMap = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean beforeBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx primaryTx,
            GridFutureAdapter<?> proceedFut) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean afterBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx backupTx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean afterPrimaryPrepare(IgniteEx primary, @Nullable IgniteInternalTx tx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx, GridFutureAdapter<?> proceedFut) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean afterPrimaryFinish(IgniteEx primary, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup, IgniteInternalTx primaryTx,
            IgniteInternalTx backupTx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            return false;
        }

        /**
         * @param order Order.
         */
        protected IgniteUuid version(int order) {
            return txMap.get(order);
        }

        /**
         * @param ver Version.
         */
        protected int order(IgniteUuid ver) {
            return revTxMap.get(ver);
        }

        /** {@inheritDoc} */
        @Override public void onTxStart(Transaction tx, int idx) {
            txMap.put(idx, tx.xid());
            revTxMap.put(tx.xid(), idx);
        }
    }

    /**
     * Find a tx by near xid version.
     *
     * @param n Node.
     * @param nearVer Near version.
     * @param primary {@code True} to search primary tx.
     */
    private IgniteInternalTx findTx(IgniteEx n, GridCacheVersion nearVer, boolean primary) {
        return n.context().cache().context().tm().activeTransactions().stream().
            filter(tx -> nearVer.equals(tx.nearXidVersion()) && tx.local() == primary).findAny().orElse(null);
    }

    /**
     * @param r Runnable.
     */
    public void runAsync(Runnable r) {
        IgniteInternalFuture fut = GridTestUtils.runAsync(r);

        // Fail test if future failed to finish normally.
        fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
            @Override public void apply(IgniteInternalFuture fut0) {
                try {
                    fut0.get();
                }
                catch (Throwable t) {
                    testFailed.set(t);
                }
            }
        });
    }

    /**
     * @param partId Partition id.
     *
     * @return Counter or null if node is not partition owner.
     */
    protected @Nullable PartitionUpdateCounter counter(int partId) {
        @Nullable GridDhtLocalPartition part = internalCache(0).context().topology().localPartition(partId);

        return part == null ? null : part.dataStore().partUpdateCounter();
    }

    /**
     * @param skipCheckpointOnStop Skip checkpoint on stop.
     * @param name Grid instance.
     */
    protected void stopGrid(boolean skipCheckpointOnStop, String name) {
        IgniteEx grid = grid(name);

        if (skipCheckpointOnStop && persistenceEnabled()) {
            GridCacheDatabaseSharedManager db =
                (GridCacheDatabaseSharedManager)grid.context().cache().context().database();

            db.enableCheckpoints(false);
        }

        stopGrid(grid.name(), skipCheckpointOnStop);
    }

    /**
     * @param spi SPI.
     *
     * @return Corresponding Ignite instance.
     */
    private IgniteEx fromNode(TestRecordingCommunicationSpi spi) {
        return IgnitionEx.gridxx(spi.getSpiContext().localNode().id());
    }

    /**
     * The callback order prepares and commits on primary and backups nodes.
     */
    protected class TwoPhaseCommitTxCallbackAdapter extends TxCallbackAdapter {
        /** */
        private Map<T3<IgniteEx /** Node */, TxState /** State */, IgniteUuid /** Near xid */>, GridFutureAdapter<?>>
            futures = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteEx, Queue<Integer>> assigns = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteEx, Queue<Integer>> prepares = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteEx, Queue<Integer>> commits = new ConcurrentHashMap<>();

        /** */
        private final int txCnt;

        /** */
        private Map<IgniteUuid, Boolean> allPrimaryCommitted = new ConcurrentHashMap<>();

        /** */
        private AtomicBoolean allPrimaryCommittedFlag = new AtomicBoolean();

        /** */
        private Map<T2<IgniteEx, IgniteUuid>, Integer> assignCntr = new ConcurrentHashMap<>();

        /**
         * @param prepares Prepares.
         * @param commits Commits.
         * @param txCnt Tx count.
         */
        public TwoPhaseCommitTxCallbackAdapter(Map<IgniteEx, int[]> prepares, Map<IgniteEx, int[]> commits, int txCnt) {
            this(prepares, prepares, commits, txCnt);
        }

        /**
         * @param prepares Map of node to it's prepare order.
         * @param commits Map of node to it's commit order.
         */
        public TwoPhaseCommitTxCallbackAdapter(
            Map<IgniteEx, int[]> assigns,
            Map<IgniteEx, int[]> prepares,
            Map<IgniteEx, int[]> commits,
            int txCnt) {
            this.txCnt = txCnt;

            // TODO validate orders (all unique)

            prepares.forEach((ex, ints) -> assertEquals("Wrong order of prepares", txCnt, ints.length));

            commits.forEach((ex, ints) -> assertEquals("Wrong order of commits", txCnt, ints.length));

            for (Map.Entry<IgniteEx, int[]> entry : prepares.entrySet()) {
                this.prepares.put(entry.getKey(),
                    IntStream.of(entry.getValue()).boxed().collect(toCollection(ConcurrentLinkedQueue::new)));
            }

            for (Map.Entry<IgniteEx, int[]> entry : commits.entrySet()) {
                this.commits.put(entry.getKey(),
                    IntStream.of(entry.getValue()).boxed().collect(toCollection(ConcurrentLinkedQueue::new)));
            }

            for (Map.Entry<IgniteEx, int[]> entry : assigns.entrySet()) {
                this.assigns.put(entry.getKey(),
                    IntStream.of(entry.getValue()).boxed().collect(toCollection(ConcurrentLinkedQueue::new)));
            }
        }

        /**
         * @param primary Primary.
         * @param tx Tx.
         * @param idx Index.
         */
        protected boolean onPrimaryPrepared(IgniteEx primary, IgniteInternalTx tx, int idx) {
            log.info("TX: prepared on primary [name=" + primary.name() + ", txId=" + idx + ']');

            return false;
        }

        /**
         * Note: callbacks are called only if node presents in defined order.
         *
         * @param primary Primary.
         */
        protected void onAllPrimaryPrepared(IgniteEx primary) {
            log.info("TX: all primary prepared [name=" + primary.name() + ']');
        }

        /**
         * Note: callbacks are called only if node presents in defined order.
         *
         * @param backup Backup.
         * @param tx Tx.
         * @param idx Index.
         */
        protected boolean onBackupPrepared(IgniteEx backup, IgniteInternalTx tx, int idx) {
            log.info("TX: backup prepared [name=" + backup.name() + ", txId=" + idx + ']');

            return false;
        }

        /**
         * Note: callbacks are called only if node presents in defined order.
         *
         * @param primary Primary.
         * @param idx Index.
         */
        protected boolean onPrimaryCommitted(IgniteEx primary, int idx) {
            log.info("TX: primary committed [name=" + primary.name() + ", txId=" + idx + ']');

            return false;
        }

        /**
         * Note: callbacks are called only if node presents in defined order.
         *
         * @param backup Backup node.
         * @param idx Index.
         */
        protected boolean onBackupCommitted(IgniteEx backup, int idx) {
            log.info("TX: backup committed [name=" + backup.name() + ", id=" + backup.localNode().id() +
                ", txId=" + idx + ']');

            return false;
        }

        /**
         * Note: callbacks are called only if node presents in defined order.
         *
         * @param primary Primary node.
         */
        protected void onAllPrimaryCommitted(IgniteEx primary) {
            log.info("TX: all primary committed [name=" + primary.name() + ']');
        }

        /**
         * Note: callbacks are called only if node presents in defined order.
         *
         * @param backup Backup node.
         */
        protected void onAllBackupCommitted(IgniteEx backup) {
            log.info("TX: all backup committed: [name=" + backup.name() + ']');
        }

        /**
         * @param primary Primary node.
         * @param tx Primary tx.
         */
        protected void onCounterAssigned(IgniteEx primary, IgniteInternalTx tx, int idx) {
            log.info("TX: primary counter assigned: [name=" + primary.name() + ", txId=" + idx + ']');
        }

        /**
         * @param node Node.
         * @param state State.
         * @return Count of futures for node.
         */
        private long countForNode(IgniteEx node, TxState state) {
            return futures.keySet().stream().filter(objects -> objects.get1() == node && objects.get2() == state).count();
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
            if (assigns.get(primary) == null)
                return false;

            runAsync(() -> {
                futures.put(new T3<>(primary, TxState.ASSIGN, nearXidVer), proceedFut);

                futures.put(new T3<>(primary, TxState.PREPARE, nearXidVer), new GridCompoundFuture() {
                    @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
                        Collection<IgniteInternalFuture<?>> futures = futures();

                        for (IgniteInternalFuture<?> future : futures)
                            ((GridFutureAdapter)future).onDone();

                        return super.onDone(res, err);
                    }
                });

                // Order counter assigns.
                if (countForNode(primary, TxState.ASSIGN) == txCnt) { // Wait until all prep requests queued and force prepare order.
                    futures.remove(new T3<>(primary, TxState.ASSIGN, version(assigns.get(primary).poll()))).onDone();
                }
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterPrimaryPrepare(IgniteEx primary, IgniteInternalTx tx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            if (prepares.get(primary) == null)
                return false;

            runAsync(() -> {
                if (onPrimaryPrepared(primary, tx, order(nearXidVer)))
                    return;

                if (prepares.get(primary).isEmpty()) {
                    onAllPrimaryPrepared(primary);

                    return;
                }

                futures.remove(new T3<>(primary, TxState.PREPARE, version(prepares.get(primary).poll()))).onDone();
            });

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx, GridFutureAdapter<?>
            proceedFut) {
            if (commits.get(primary) == null)
                return false;

            runAsync(() -> {
                futures.put(new T3<>(primary, TxState.COMMIT, tx.nearXidVersion().asIgniteUuid()), proceedFut);

                if (countForNode(primary, TxState.COMMIT) == txCnt)
                    futures.remove(new T3<>(primary, TxState.COMMIT, version(commits.get(primary).poll()))).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean beforeBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx primaryTx,
            GridFutureAdapter<?> proceedFut) {

            assert primary != backup;

            if (prepares.get(backup) == null && assigns.get(primary) == null)
                return false;

            runAsync(() -> {
                if (assigns.get(primary) != null) {
                    int v0 = assignCntr.compute(new T2<>(primary, primaryTx.nearXidVersion().asIgniteUuid()),
                        (key, val) -> (val == null ? 0 : val) + 1);

                    if (v0 == 2) {
                        onCounterAssigned(primary, primaryTx, order(primaryTx.nearXidVersion().asIgniteUuid()));

                        if (!assigns.get(primary).isEmpty())
                            futures.remove(new T3<>(primary, TxState.ASSIGN,
                                version(assigns.get(primary).poll()))).onDone();
                    }
                }

                if (prepares.get(backup) != null) {
                    futures.put(new T3<>(backup, TxState.PREPARE, primaryTx.nearXidVersion().asIgniteUuid()), proceedFut);

                    // Wait until all prep requests queued and force prepare order.
                    if (countForNode(backup, TxState.PREPARE) == txCnt) {
                        futures.remove(new T3<>(backup, TxState.PREPARE, version(prepares.get(backup).poll()))).onDone();
                    }
                }
            });

            return prepares.get(backup) != null;
        }

        /** {@inheritDoc} */
        @Override public boolean afterBackupPrepare(
            IgniteEx primary,
            IgniteEx backup,
            IgniteInternalTx backupTx,
            IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            if (prepares.get(backup) == null && prepares.get(primary) == null)
                return false;

            runAsync(() -> {
                if (prepares.get(primary) != null) {
                    GridCompoundFuture<Object, Object> fut0 =
                        (GridCompoundFuture<Object, Object>)futures.get(new T3<>(primary, TxState.PREPARE, nearXidVer));

                    fut0.add((IgniteInternalFuture<Object>)proceedFut);

                    int sum = futures.entrySet().stream().filter(objects -> objects.getKey().get1() == primary &&
                        objects.getKey().get2() == TxState.PREPARE).
                        mapToInt(value -> ((GridCompoundFuture)value.getValue()).futures().size()).sum();

                    if (sum == txCnt * 2)
                        futures.remove(new T3<>(primary, TxState.PREPARE, version(prepares.get(primary).poll()))).onDone();
                }

                if (prepares.get(backup) != null) {
                    if (onBackupPrepared(backup, backupTx, order(nearXidVer)))
                        return;

                    if (prepares.get(backup).isEmpty())
                        return;

                    futures.remove(new T3<>(backup, TxState.PREPARE, version(prepares.get(backup).poll()))).onDone();
                }

            });

            return prepares.get(primary) != null;
        }

        /** {@inheritDoc} */
        @Override public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup, @Nullable IgniteInternalTx primaryTx,
            IgniteInternalTx backupTx, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
            if (commits.get(backup) == null) // Ignore backup because the order is not specified.
                return false;

            runAsync(() -> {
                futures.put(new T3<>(backup, TxState.COMMIT, nearXidVer), proceedFut);

                // First finish message to backup means what tx was committed on primary.
                Boolean prev = allPrimaryCommitted.putIfAbsent(nearXidVer, Boolean.TRUE);

                if (prev == null) {
                    if (onPrimaryCommitted(primary, order(nearXidVer)))
                        return;
                }

                if (countForNode(primary, TxState.COMMIT) == 0 && countForNode(backup, TxState.COMMIT) == txCnt) {
                    if (allPrimaryCommittedFlag.compareAndSet(false, true))
                        onAllPrimaryCommitted(primary); // Report all primary committed once.

                    // Proceed with commit to backups.
                    futures.remove(new T3<>(backup, TxState.COMMIT, version(commits.get(backup).poll()))).onDone();

                    return;
                }

                if (prev == null)
                    futures.remove(new T3<>(primary, TxState.COMMIT, version(commits.get(primary).poll()))).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            if (commits.get(backup) == null) // Ignore backup because the order is not specified.
                return false;

            runAsync(() -> {
                if (onBackupCommitted(backup, order(nearXidVer)))
                    return;

                if (commits.get(backup).isEmpty()) {
                    onAllBackupCommitted(backup);

                    return;
                }

                futures.remove(new T3<>(backup, TxState.COMMIT, version(commits.get(backup).poll()))).onDone();
            });

            return false;
        }
    }

    /**
     * Blocks tx recovery between all nodes.
     */
    protected void blockRecovery() {
        for (Ignite grid : G.allGrids()) {
            if (grid.configuration().isClientMode())
                continue;

            TestRecordingCommunicationSpi.spi(grid).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                    return msg instanceof GridCacheTxRecoveryRequest;
                }
            });
        }
    }

    /** */
    private enum TxState {
        /** Prepare. */ PREPARE,
        /** Assign. */ ASSIGN,
        /** Commit. */COMMIT
    }
}
