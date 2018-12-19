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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import junit.framework.AssertionFailedError;
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
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Mini test framework for ordering transaction's prepares and commits by intercepting messages and releasing then in user defined order.
 *
 * crd.affinity(DEFAULT_CACHE_NAME).primaryPartitions(crd.localNode()) = {int[8]@5854}
 0 = 16
 1 = 1
 2 = 10
 3 = 11
 4 = 12
 5 = 13
 6 = 29
 7 = 15
 crd.affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(1).localNode()) = {int[11]@5862}
 0 = 18
 1 = 19
 2 = 5
 3 = 21
 4 = 6
 5 = 7
 6 = 8
 7 = 9
 8 = 25
 9 = 26
 10 = 31
 crd.affinity(DEFAULT_CACHE_NAME).primaryPartitions(grid(2).localNode()) = {int[13]@5888}
 0 = 0
 1 = 2
 2 = 3
 3 = 4
 4 = 14
 5 = 17
 6 = 20
 7 = 22
 8 = 23
 9 = 24
 10 = 27
 11 = 28
 12 = 30
 */
public abstract class TxPartitionCounterStateAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private int backups;

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

        cfg.setConsistentId("node" + igniteInstanceName);
        cfg.setFailureHandler(new StopNodeFailureHandler());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith(CLIENT_GRID_NAME);

        cfg.setClientMode(client);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setWalSegmentSize(8 * MB).setWalMode(LOG_ONLY).setPageSize(1024).setCheckpointFrequency(10000000000L).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        cfg.setFailureDetectionTimeout(600000);

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(backups);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);
            ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

            cfg.setCacheConfiguration(ccfg);
        }

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
     * @param partId Partition id.
     * @param part2Sup Optional second partition supplier.
     * @param nodesCnt Nodes count.
     * @param clo Callback build closure.
     * @param sizes Sizes.
     */
    protected Map<Integer, T2<Ignite, List<Ignite>>> runOnPartition(int partId, @Nullable Supplier<Integer> part2Sup, int backups, int nodesCnt,
        IgniteClosure<Map<Integer, T2<Ignite, List<Ignite>>>, TxCallback> clo, int[] sizes) throws Exception {
        this.backups = backups;

        IgniteEx crd = (IgniteEx)startGrids(nodesCnt);

        crd.cluster().active(true);

        assertEquals(0, crd.cache(DEFAULT_CACHE_NAME).size());

        int[][] ranges = new int[sizes.length][2];

        int totalKeys = 0;

        for (int i = 0; i < sizes.length; i++) {
            int size = sizes[i];

            ranges[i] = new int[] {totalKeys, size};

            totalKeys += size;
        }

        IgniteEx client = startGrid("client");

        // Preload one key to partition to enable historical rebalance.
        List<Integer> preloadKeys = loadDataToPartition(partId, "client", DEFAULT_CACHE_NAME, PRELOAD_KEYS_CNT, 0);

        forceCheckpoint();

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        List<Integer> keys = partitionKeys(crd.cache(DEFAULT_CACHE_NAME), partId, totalKeys, PRELOAD_KEYS_CNT);

        assertFalse(preloadKeys.get(0).equals(keys.get(0)));

        Ignite prim = primaryNode(keys.get(0), DEFAULT_CACHE_NAME);

        List<Ignite> backupz = backups == 0 ? null : backups == 1 ? Collections.singletonList(backupNode(keys.get(0), DEFAULT_CACHE_NAME)) : backupNodes(keys.get(0), DEFAULT_CACHE_NAME);

        final TestRecordingCommunicationSpi clientWrappedSpi = TestRecordingCommunicationSpi.spi(client);

        Map<IgniteUuid, GridCacheVersion> futMap = new ConcurrentHashMap<>();
        Map<GridCacheVersion, GridCacheVersion> nearToLocVerMap = new ConcurrentHashMap<>();

        Map<Integer, T2<Ignite, List<Ignite>>> txTop = new HashMap<>();

        txTop.put(partId, new T2<>(prim, backupz));

        List<Integer> keysPart2 = part2Sup == null ? null :
            partitionKeys(crd.cache(DEFAULT_CACHE_NAME), part2Sup.get(), sizes.length, 0) ;

        log.info("TX: topology [part1=" + partId + ", primary=" + prim.name() + ", backups=" + F.transform(backupz, new IgniteClosure<Ignite, String>() {
            @Override public String apply(Ignite ignite) {
                return ignite.name();
            }
        }));

        if (part2Sup != null) {
            int partId2 = part2Sup.get();

            Ignite prim2 = primaryNode(keysPart2.get(0), DEFAULT_CACHE_NAME);

            assertNotSame(prim, prim2);

            List<Ignite> backupz2 = backupNodes(keysPart2.get(0), DEFAULT_CACHE_NAME);

            txTop.put(partId2, new T2<>(prim2, backupz2));

            log.info("TX: topology [part2=" + partId2 + ", primary=" + prim2.name() + ", backups=" + F.transform(backupz2, new IgniteClosure<Ignite, String>() {
                @Override public String apply(Ignite ignite) {
                    return ignite.name();
                }
            }));
        }

        TxCallback cb = clo.apply(txTop);

        clientWrappedSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                if (msg instanceof GridNearTxPrepareRequest) {
                    GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)msg;

                    if (!req.last())
                        return false;

                    futMap.put(req.futureId(), req.version());

                    return cb.beforePrimaryPrepare(to, req.version().asGridUuid(), createSendFuture(clientWrappedSpi, msg));
                }
                else if (msg instanceof GridNearTxFinishRequest) {
                    GridNearTxFinishRequest req = (GridNearTxFinishRequest)msg;

                    futMap.put(req.futureId(), req.version());

                    IgniteInternalTx tx = findTx(to, req.version(), true);

                    assertNotNull(tx);

                    return cb.beforePrimaryFinish(to, tx, createSendFuture(clientWrappedSpi, msg));
                }

                return false;
            }
        });

        TestRecordingCommunicationSpi primWrapperSpi = TestRecordingCommunicationSpi.spi(prim);

        primWrapperSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                if (msg instanceof GridDhtTxPrepareRequest) {
                    GridDhtTxPrepareRequest req = (GridDhtTxPrepareRequest)msg;

                    if (!req.last())
                        return false;

                    futMap.put(req.futureId(), req.nearXidVersion());
                    nearToLocVerMap.put(req.version(), req.nearXidVersion());

                    IgniteEx from = fromNode(primWrapperSpi);

                    IgniteInternalTx primTx = findTx(from, req.nearXidVersion(), true);

                    return cb.beforeBackupPrepare(from, to, primTx, createSendFuture(primWrapperSpi, msg));
                }
                else if (msg instanceof GridDhtTxFinishRequest) {
                    GridDhtTxFinishRequest req = (GridDhtTxFinishRequest)msg;

                    GridCacheVersion nearVer = nearToLocVerMap.get(req.version());
                    futMap.put(req.futureId(), nearVer);

                    IgniteEx from = fromNode(primWrapperSpi);

                    IgniteInternalTx primTx = findTx(from, nearVer, true);
                    IgniteInternalTx backupTx = findTx(to, nearVer, false);

                    return cb.beforeBackupFinish(from, to, primTx, backupTx, nearVer.asGridUuid(), createSendFuture(primWrapperSpi, msg));
                }
                else if (msg instanceof GridNearTxPrepareResponse) {
                    GridNearTxPrepareResponse resp = (GridNearTxPrepareResponse)msg;

                    IgniteEx from = fromNode(primWrapperSpi);

                    GridCacheVersion ver = futMap.get(resp.futureId());

                    IgniteInternalTx primTx = findTx(from, ver, true);

                    return cb.afterPrimaryPrepare(from, primTx, ver.asGridUuid(), createSendFuture(primWrapperSpi, msg));
                }
                else if (msg instanceof GridNearTxFinishResponse) {
                    GridNearTxFinishResponse req = (GridNearTxFinishResponse)msg;

                    IgniteEx from = fromNode(primWrapperSpi);

                    IgniteUuid nearVer = futMap.get(req.futureId()).asGridUuid();

                    return cb.afterPrimaryFinish(from, nearVer, createSendFuture(primWrapperSpi, msg));
                }

                return false;
            }
        });

        if (backupz != null) {
            for (Ignite backup : backupz) {
                TestRecordingCommunicationSpi backupWrapperSpi = TestRecordingCommunicationSpi.spi(backup);

                backupWrapperSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                    @Override public boolean apply(ClusterNode node, Message msg) {
                        IgniteEx from = IgnitionEx.gridxx(backupWrapperSpi.getSpiContext().localNode().id());
                        IgniteEx to = IgnitionEx.gridxx(node.id());

                        if (msg instanceof GridDhtTxPrepareResponse) {
                            GridDhtTxPrepareResponse resp = (GridDhtTxPrepareResponse)msg;

                            GridCacheVersion ver = futMap.get(resp.futureId());

                            if (ver == null)
                                return false; // Message from parallel partition.

                            IgniteInternalTx tx = findTx(from, ver, false);

                            return cb.afterBackupPrepare(to, from, tx, ver.asGridUuid(), createSendFuture(backupWrapperSpi, msg));
                        }
                        else if (msg instanceof GridDhtTxFinishResponse) {
                            GridDhtTxFinishResponse resp = (GridDhtTxFinishResponse)msg;

                            GridCacheVersion ver = futMap.get(resp.futureId());

                            if (ver == null)
                                return false; // Message from parallel partition.

                            // Version is null if message is a response to checkCommittedRequest.
                            return cb.afterBackupFinish(to, from, ver.asGridUuid(), createSendFuture(backupWrapperSpi, msg));
                        }

                        return false;
                    }
                });
            }
        }

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        AtomicInteger idx = new AtomicInteger();

        CyclicBarrier b = new CyclicBarrier(sizes.length);

        IgniteInternalFuture<Long> fut = runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int txIdx = idx.getAndIncrement();

                int[] range = ranges[txIdx];

                String lb = "t" + txIdx;

                try (Transaction tx = client.transactions().withLabel(lb).txStart()) {
                    cb.onTxStart(tx, txIdx);

                    U.awaitQuiet(b); // Wait should always success.

                    for (Integer key : keys.subList(range[0], range[0] + range[1]))
                        client.cache(DEFAULT_CACHE_NAME).put(key, 0);

                    if (keysPart2 != null) {// Force 2PC.
                        client.cache(DEFAULT_CACHE_NAME).put(keysPart2.get(txIdx), 0);
                    }

                    tx.commit();
                }
                catch (Exception ignored) {
                    // No-op.
                }

                // TODO FIXME expect rollback for some scenarios.
            }
        }, sizes.length, "tx-thread");

        try {
            fut.get(TEST_TIMEOUT);
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
     * @param wrapperSpi Wrapper spi.
     * @param msg Message.
     */
    private GridFutureAdapter<?> createSendFuture(TestRecordingCommunicationSpi wrapperSpi, Message msg) {
        GridFutureAdapter<?> fut = new GridFutureAdapter<Object>();

        fut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> fut) {
                wrapperSpi.stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
                    @Override public boolean apply(T2<ClusterNode, GridIoMessage> objects) {
                        return objects.get2().message() == msg;
                    }
                }, false);
            }
        });

        return fut;
    }

    protected interface TxCallback {
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

        public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx, GridFutureAdapter<?> proceedFut);

        public boolean afterPrimaryFinish(IgniteEx primary, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut);

        public boolean afterBackupPrepare(IgniteEx primary, IgniteEx backup, @Nullable IgniteInternalTx tx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> fut);

        public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer, GridFutureAdapter<?> fut);

        /**
         * @param primary Prim.
         * @param backup Backup.
         * @param primaryTx Prim tx. Null for 2pc.
         * @param backupTx Backup tx.
         * @param nearXidVer
         * @param future Future.
         */
        public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup, @Nullable IgniteInternalTx primaryTx,
            IgniteInternalTx backupTx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> future);

        /**
         * @param primary Primary.
         * @param tx Tx or null for one-phase commit.
         * @param nearXidVer Near xid version.
         * @param fut Future.
         */
        public boolean afterPrimaryPrepare(IgniteEx primary, @Nullable IgniteInternalTx tx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> fut);

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
        private Map<Integer, IgniteUuid> txMap = new ConcurrentHashMap<>();
        private Map<IgniteUuid, Integer> revTxMap = new ConcurrentHashMap<>();

        @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
            return false;
        }

        @Override public boolean beforeBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx primaryTx,
            GridFutureAdapter<?> proceedFut) {
            return false;
        }

        @Override public boolean afterBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx tx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
            return false;
        }

        @Override public boolean afterPrimaryPrepare(IgniteEx primary, @Nullable IgniteInternalTx tx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> fut) {
            return false;
        }

        @Override public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx, GridFutureAdapter<?> proceedFut) {
            return false;
        }

        @Override public boolean afterPrimaryFinish(IgniteEx primary, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            return false;
        }

        @Override public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup, IgniteInternalTx primaryTx,
            IgniteInternalTx backupTx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> future) {
            return false;
        }

        @Override public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer,
            GridFutureAdapter<?> fut) {
            return false;
        }

        protected IgniteUuid version(int order) {
            return txMap.get(order);
        }

        protected int order(IgniteUuid ver) {
            return revTxMap.get(ver);
        }

        @Override public void onTxStart(Transaction tx, int idx) {
            txMap.put(idx, tx.xid());
            revTxMap.put(tx.xid(), idx);
        }
    }

    /**
     * The callback order prepares and commits on primary node.
     */
    protected class TwoPhasePessimisticTxCallbackAdapter extends TxCallbackAdapter {
        /** */
        private Queue<Integer> prepOrder;

        /** */
        private Queue<Integer> primCommitOrder;

        /** */
        private Queue<Integer> backupCommitOrder;

        /** */
        private Map<IgniteUuid, GridFutureAdapter<?>> prepFuts = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteUuid, GridFutureAdapter<?>> primFinishFuts = new ConcurrentHashMap<>();

        /** */
        private Map<IgniteUuid, GridFutureAdapter<?>> backupFinishFuts = new ConcurrentHashMap<>();

        /** */
        private final Ignite primaryNode;

        /** */
        private final Ignite backupNode;

        /** */
        private final int txCnt;

        /**
         * @param prepOrd Prepare order.
         * @param primCommitOrder Commit order.
         */
        public TwoPhasePessimisticTxCallbackAdapter(int[] prepOrd, Ignite primaryNode, int[] primCommitOrder,
            Ignite backupNode, int[] backupCommitOrder) {
            this.primaryNode = primaryNode;
            this.backupNode = backupNode;
            this.txCnt = prepOrd.length;

            prepOrder = new ConcurrentLinkedQueue<>();

            for (int aPrepOrd : prepOrd)
                prepOrder.add(aPrepOrd);

            this.primCommitOrder = new ConcurrentLinkedQueue<>();

            for (int aCommitOrd : primCommitOrder)
                this.primCommitOrder.add(aCommitOrd);

            this.backupCommitOrder = new ConcurrentLinkedQueue<>();

            for (int aCommitOrd : backupCommitOrder)
                this.backupCommitOrder.add(aCommitOrd);
        }

        /** */
        protected boolean onPrepared(IgniteEx primary, IgniteInternalTx tx, int idx) {
            log.info("TX: prepared on primary [name=" + primary.name() + ", txId=" + idx + ", tx=" + CU.txString(tx) + ']');

            return false;
        }

        /**
         * @param primary Primary primary.
         */
        protected void onAllPrimaryPrepared(IgniteEx primary) {
            log.info("TX: all primary prepared [name=" + primary.name() + ']');
        }

        /**
         * @param primary Primary node.
         * @param idx Index.
         */
        protected boolean onPrimaryCommitted(IgniteEx primary, int idx) {
            log.info("TX: primary committed [name=" + primary.name() + ", txId=" + idx + ']');

            return false;
        }

        /**
         * @param backup Backup node.
         * @param idx Index.
         */
        protected boolean onBackupCommitted(IgniteEx backup, int idx) {
            log.info("TX: backup committed " + idx);

            return false;
        }

        /**
         * @param primary Primary node.
         */
        protected void onAllPrimaryCommitted(IgniteEx primary) {
            log.info("TX: all primary committed");
        }

        /**
         * @param backup Backup node.
         */
        protected void onAllBackupCommitted(IgniteEx backup) {
            log.info("TX: all backup committed");
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            if (primary != primaryNode) // Ignore events from other tx participants.
                return false;

            runAsync(() -> {
                prepFuts.put(nearXidVer, proceedFut);

                // Order prepares.
                if (prepFuts.size() == prepOrder.size()) {// Wait until all prep requests queued and force prepare order.
                    prepFuts.remove(version(prepOrder.poll())).onDone();
                }
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterPrimaryPrepare(IgniteEx primary, IgniteInternalTx tx, IgniteUuid nearXidVer,
            GridFutureAdapter<?> fut) {
            if (primary != primaryNode)
                return false;

            runAsync(() -> {
                if (onPrepared(primary, tx, order(tx.nearXidVersion().asGridUuid())))
                    return;

                if (prepOrder.isEmpty()) {
                    onAllPrimaryPrepared(primary);

                    return;
                }

                prepFuts.remove(version(prepOrder.poll())).onDone();
            });

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx tx, GridFutureAdapter<?>
            proceedFut) {
            if (this.primaryNode != primary) // Ignore events from other tx participants.
                return false;

            runAsync(() -> {
                primFinishFuts.put(tx.nearXidVersion().asGridUuid(), proceedFut);

                // Order prepares.
                if (primFinishFuts.size() == 3)
                    primFinishFuts.remove(version(primCommitOrder.poll())).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup, @Nullable IgniteInternalTx primaryTx,
            IgniteInternalTx backupTx, IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
            if (primary != primaryNode || backup != backupNode)
                return false;

            runAsync(() -> {
                backupFinishFuts.put(nearXidVer, fut);

                if (onPrimaryCommitted(primary, order(nearXidVer)))
                    return;

                if (primCommitOrder.isEmpty() && backupFinishFuts.size() == txCnt) {
                    onAllPrimaryCommitted(primary);

                    assertEquals(txCnt, backupFinishFuts.size());

                    backupFinishFuts.remove(version(backupCommitOrder.poll())).onDone();

                    return;
                }

                primFinishFuts.remove(version(primCommitOrder.poll())).onDone();
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer,
            GridFutureAdapter<?> fut) {
            if (primary != primaryNode || backup != backupNode)
                return false;

            runAsync(() -> {
                if (onBackupCommitted(backup, order(nearXidVer)))
                    return;

                if (backupCommitOrder.isEmpty()) {
                    onAllBackupCommitted(backup);

                    return;
                }

                backupFinishFuts.remove(version(backupCommitOrder.poll())).onDone();
            });

            return false;
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
        return n.context().cache().context().tm().activeTransactions().stream().filter(new Predicate<IgniteInternalTx>() {
            @Override public boolean test(IgniteInternalTx tx) {
                return nearVer.equals(tx.nearXidVersion()) && tx.local() == primary;
            }
        }).findAny().orElse(null);
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
     */
    protected PartitionUpdateCounter counter(int partId) {
        return internalCache(0).context().topology().localPartition(partId).dataStore().partUpdateCounter();
    }

    /**
     * @param partId Partition id.
     */
    protected PartitionUpdateCounter counter(int partId, String gridName) {
        return internalCache(grid(gridName).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(partId).dataStore().partUpdateCounter();
    }


    /**
     * @param skipCheckpointOnStop Skip checkpoint on stop.
     * @param name Grid instance.
     */
    protected void stopGrid(boolean skipCheckpointOnStop, String name) {
        IgniteEx grid = grid(name);

        if (skipCheckpointOnStop) {
            GridCacheDatabaseSharedManager db =
                (GridCacheDatabaseSharedManager)grid.context().cache().context().database();

            db.enableCheckpoints(false);
        }

        stopGrid(grid.name(), skipCheckpointOnStop);
    }

    private IgniteEx fromNode(TestRecordingCommunicationSpi primWrapperSpi) {
        return IgnitionEx.gridxx(primWrapperSpi.getSpiContext().localNode().id());
    }

    /**
     * @param res Response.
     */
    protected void assertPartitionsSame(IdleVerifyResultV2 res) throws AssertionFailedError {
        if (res.hasConflicts()) {
            StringBuilder b = new StringBuilder();

            res.print(b::append);

            fail(b.toString());
        }
    }
}
