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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 */
public class TxSinglePartitionAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    public static final int COUNT = 5000;

    /** */
    private int backups;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId("node" + igniteInstanceName);
        cfg.setFailureHandler(new StopNodeFailureHandler());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

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
            ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

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
     * @param nodesCnt Nodes count.
     * @param sizes Sizes.
     * @param cb Callback.
     */
    protected void runOnPartition(int partId, int backups, int nodesCnt, TxCallback cb, int... sizes) throws Exception {
        this.backups = backups;

        IgniteEx crd = (IgniteEx)startGridsMultiThreaded(nodesCnt);

        int[][] ranges = new int[sizes.length][2];

        int totalKeys = 0;

        for (int i = 0; i < sizes.length; i++) {
            int size = sizes[i];

            ranges[i] = new int[] {totalKeys, size};

            totalKeys += size;
        }

        List<Integer> keys = partitionKeys(crd.cache(DEFAULT_CACHE_NAME), partId, totalKeys);

        IgniteEx client = startGrid("client");

        Ignite prim = primaryNode(keys.get(0), DEFAULT_CACHE_NAME);

        List<Ignite> backupz = backupNodes(keys.get(0), DEFAULT_CACHE_NAME);

        final TestRecordingCommunicationSpi clientWrappedSpi = TestRecordingCommunicationSpi.spi(client);

        Map<IgniteUuid, IgniteUuid> futMap = new ConcurrentHashMap<>();

        clientWrappedSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                if (msg instanceof GridNearTxPrepareRequest) {
                    GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)msg;

                    futMap.put(req.futureId(), req.version().asGridUuid());

                    return cb.onBeforePrimaryPrepare(to, req.version().asGridUuid(), createSendFuture(clientWrappedSpi, msg));
                }
                else if (msg instanceof GridNearTxFinishRequest) {
                    GridNearTxFinishRequest req = (GridNearTxFinishRequest)msg;

                    IgniteInternalTx tx = findTx(to, req.version(), true);

                    assertNotNull(tx);

                    return cb.onBeforePrimaryFinish(to, tx, createSendFuture(clientWrappedSpi, msg));
                }

                return false;
            }
        });

            TestRecordingCommunicationSpi primWrapperSpi = TestRecordingCommunicationSpi.spi(prim);

            primWrapperSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    IgniteEx from = IgnitionEx.gridxx(primWrapperSpi.getSpiContext().localNode().id());
                    IgniteEx to = IgnitionEx.gridxx(node.id());

                    if (msg instanceof GridDhtTxPrepareRequest) {
                        GridDhtTxPrepareRequest req = (GridDhtTxPrepareRequest)msg;

                        return cb.onAfterPrimaryPrepare(to, findTx(from, req.nearXidVersion(), true), createSendFuture(primWrapperSpi, msg));
                    }
                    else if (msg instanceof GridDhtTxFinishRequest)
                        return false; // cb.onBeforeCommit(n, null, null, createSendFuture(primWrapperSpi, msg));
                    else if (msg instanceof GridNearTxFinishResponse) {
                        GridNearTxFinishResponse req = (GridNearTxFinishResponse)msg;

                        IgniteUuid nearVer = futMap.get(req.futureId());

                        return cb.onAfterPrimaryFinish(to, nearVer, createSendFuture(primWrapperSpi, msg));
                    }

                    return false;
                }
            });
//
//            for (Ignite backup : backups) {
//                TestRecordingCommunicationSpi backupWrapperSpi = TestRecordingCommunicationSpi.spi(backup);
//
//                backupWrapperSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
//                    @Override public boolean apply(ClusterNode node, Message msg) {
//                        IgniteEx n = IgnitionEx.gridxx(node.id());
//
//                        if (msg instanceof GridDhtTxPrepareResponse)
//                            return cb.onBeforePrimaryPrepare(n, null, null, createSendFuture(backupWrapperSpi, msg));
//                        else if (msg instanceof GridDhtTxFinishResponse)
//                            return cb.onBeforePrimaryPrepare(n, null, null, createSendFuture(backupWrapperSpi, msg));
//
//                        return false;
//                    }
//                });
//            }

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        AtomicInteger idx = new AtomicInteger();

        CyclicBarrier b = new CyclicBarrier(sizes.length);

        IgniteInternalFuture<Long> fut = runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int txIdx = idx.getAndIncrement();

                int[] range = ranges[txIdx];

                String lb = "t" + idx;

                try (Transaction tx = client.transactions().withLabel(lb).txStart()) {
                    cb.onTxStart(tx, txIdx);

                    U.awaitQuiet(b); // Wait should always success.

                    for (Integer key : keys.subList(range[0], range[0] + range[1]))
                        client.cache(DEFAULT_CACHE_NAME).put(key, 0);

                    tx.commit();
                }

                // TODO FIXME expect rollback for some scenarios.
            }
        }, sizes.length, "tx-thread");

        fut.get();
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
                        boolean res = objects.get2().message() == msg;

                        if (res) {
                            Message message = objects.get2().message();
                            System.out.println("EBAT: " + message.hashCode());
                        }

                        return res;
                    }
                }, false);
            }
        });

        return fut;
    }

    public static interface TxCallback {
        public boolean onBeforePrimaryPrepare(IgniteEx node, IgniteUuid version,
            GridFutureAdapter<?> proceedFut);

        public boolean onAfterPrimaryPrepare(IgniteEx node, IgniteInternalTx tx,
            GridFutureAdapter<?> proceedFut);

        boolean onBeforePrimaryFinish(IgniteEx n, IgniteInternalTx tx, GridFutureAdapter<?> proceedFut);

        boolean onAfterPrimaryFinish(IgniteEx n, IgniteUuid version, GridFutureAdapter<?> proceedFut);

        void onTxStart(Transaction tx, int idx);
    }



    private IgniteInternalTx findTx(IgniteEx n, GridCacheVersion nearVersion, boolean primary) {
        return n.context().cache().context().tm().activeTransactions().stream().filter(new Predicate<IgniteInternalTx>() {
            @Override public boolean test(IgniteInternalTx tx) {
                return nearVersion.equals(tx.nearXidVersion()) && tx.local() == primary;
            }
        }).findAny().orElse(null);
    }
}
