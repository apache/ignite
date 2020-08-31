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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 */
public class TxCrossCacheMapOnInvalidTopologyTest extends GridCommonAbstractTest {
    /** Partitions count. */
    private static final int PARTS_CNT = 32;

    /** Cache 1. */
    private static final String CACHE1 = DEFAULT_CACHE_NAME;

    /** Cache 2. */
    private static final String CACHE2 = DEFAULT_CACHE_NAME + "2";

    /** */
    private static final int MB = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setCacheConfiguration(cacheConfiguration(CACHE1), cacheConfiguration(CACHE2).setRebalanceOrder(10));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setPageSize(1024).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        return cfg;
    }

    /**
     * @param name Name.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(String name) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(name);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(2);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        return ccfg;
    }

    /**
     *
     */
    @Test
    public void testCrossCacheTxMapOnInvalidTopologyPessimistic() throws Exception {
        doTestCrossCacheTxMapOnInvalidTopology(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     *
     */
    @Test
    public void testCrossCacheTxMapOnInvalidTopologyOptimistic() throws Exception {
        doTestCrossCacheTxMapOnInvalidTopology(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     *
     */
    @Test
    public void testCrossCacheTxMapOnInvalidTopologyOptimisticSerializable() throws Exception {
        doTestCrossCacheTxMapOnInvalidTopology(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * Test scenario: cross-cache tx is started when node is left in the middle of rebalance, first cache is rebalanced
     * and second is partially rebalanced.
     *
     * First cache map request will trigger client compatible remap for pessimistic txs,
     * second cache map request should use new topology version.
     *
     * For optimistic tx remap is enforced if more than one mapping in transaction or all enlisted caches have compatible
     * assignments.
     *
     * Success: tx is finished on ideal topology version over all mapped nodes.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     */
    private void doTestCrossCacheTxMapOnInvalidTopology(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        try {
            IgniteEx crd = startGrid(0);
            IgniteEx g1 = startGrid(1);

            awaitPartitionMapExchange();

            IgniteEx client = startClientGrid("client");
            assertNotNull(client.cache(CACHE1));
            assertNotNull(client.cache(CACHE2));

            try (IgniteDataStreamer<Object, Object> streamer = crd.dataStreamer(CACHE1)) {
                // Put 500 keys per partition.
                for (int k = 0; k < PARTS_CNT * 500; k++)
                    streamer.addData(k, new byte[10]);
            }

            try (IgniteDataStreamer<Object, Object> streamer = crd.dataStreamer(CACHE2)) {
                // Put 500 keys per partition.
                for (int k = 0; k < PARTS_CNT * 500; k++)
                    streamer.addData(k, new byte[10]);
            }

            TestRecordingCommunicationSpi crdSpi = TestRecordingCommunicationSpi.spi(crd);

            final AffinityTopologyVersion joinVer = new AffinityTopologyVersion(4, 0);
            AffinityTopologyVersion leftVer = new AffinityTopologyVersion(5, 0);
            AffinityTopologyVersion idealVer = new AffinityTopologyVersion(5, 1);

            AtomicReference<Set<Integer>> full = new AtomicReference<>();

            GridConcurrentSkipListSet<Integer> leftVerParts = new GridConcurrentSkipListSet<>();

            crdSpi.blockMessages((node, m) -> {
                if (m instanceof GridDhtPartitionSupplyMessage) {
                    GridDhtPartitionSupplyMessage msg = (GridDhtPartitionSupplyMessage)m;

                    // Allow full rebalance for cache 1 and system cache.
                    if (msg.groupId() != CU.cacheId(CACHE2))
                        return false;

                    // Allow only first batch for cache 2.
                    if (msg.topologyVersion().equals(joinVer)) {
                        if (full.get() == null) {
                            Map<Integer, Long> last = U.field(msg, "last");

                            full.set(last.keySet());

                            return false;
                        }

                        return true;
                    }

                    if (msg.topologyVersion().equals(leftVer)) {
                        Map<Integer, Long> last = U.field(msg, "last");

                        leftVerParts.addAll(last.keySet());

                        return true;
                    }
                } else if (m instanceof GridDhtPartitionsFullMessage) {
                    GridDhtPartitionsFullMessage msg = (GridDhtPartitionsFullMessage)m;

                    // Delay full message for ideal topology switch.
                    GridDhtPartitionExchangeId exchId = msg.exchangeId();

                    if (exchId != null && exchId.topologyVersion().equals(idealVer))
                        return true;
                }

                return false;
            });

            TestRecordingCommunicationSpi g1Spi = TestRecordingCommunicationSpi.spi(g1);
            g1Spi.blockMessages((node, msg) -> {
                if (msg instanceof GridDhtPartitionSupplyMessage) {
                    GridDhtPartitionSupplyMessage m = (GridDhtPartitionSupplyMessage)msg;

                    return m.groupId() == CU.cacheId(CACHE2);
                }

                return false;
            });

            startGrid(2);

            crdSpi.waitForBlocked();
            g1Spi.waitForBlocked();

            // Wait partial owning.
            assertTrue("Timed out while waiting for rebalance", GridTestUtils.waitForCondition(() -> {
                // Await full rebalance for cache 2.
                GridDhtPartitionTopology top0 = grid(2).cachex(CACHE1).context().topology();

                for (int p = 0; p < PARTS_CNT; p++) {
                    if (top0.localPartition(p).state() != OWNING)
                        return false;
                }

                // Await partial rebalance for cache 1.
                GridDhtPartitionTopology top1 = grid(2).cachex(CACHE2).context().topology();

                for (Integer part : full.get()) {
                    if (top1.localPartition(part).state() != OWNING)
                        return false;
                }

                return true;
            }, 10_000));

            // At this point cache 1 is fully rebalanced and cache 2 is partially rebalanced.
            // Stop supplier in the middle of rebalance.
            g1.close();

            // Wait for topologies and calculate required partitions.
            grid(0).cachex(CACHE1).context().affinity().affinityReadyFuture(leftVer).get();
            grid(2).cachex(CACHE1).context().affinity().affinityReadyFuture(leftVer).get();
            grid(0).cachex(CACHE2).context().affinity().affinityReadyFuture(leftVer).get();
            grid(2).cachex(CACHE2).context().affinity().affinityReadyFuture(leftVer).get();

            AffinityAssignment assignment0 = grid(0).cachex(CACHE1).context().affinity().assignment(leftVer);
            AffinityAssignment assignment = grid(0).cachex(CACHE2).context().affinity().assignment(leftVer);

            // Search for a partition with incompatible assignment.
            int stablePart = -1; // Partition for cache1 which is mapped for both late and ideal topologies to the same primary.
            int movingPart = -1; // Partition for cache2 which is mapped for both late and ideal topologies on different primaries.

            for (int p = 0; p < assignment0.assignment().size(); p++) {
                List<ClusterNode> curr = assignment.assignment().get(p);
                List<ClusterNode> ideal = assignment.idealAssignment().get(p);

                if (curr.equals(ideal) && curr.get(0).order() == 1) {
                    stablePart = p;

                    break;
                }
            }

            assertFalse(stablePart == -1);

            for (int p = 0; p < assignment.assignment().size(); p++) {
                List<ClusterNode> curr = assignment.assignment().get(p);
                List<ClusterNode> ideal = assignment.idealAssignment().get(p);

                if (!curr.equals(ideal) && curr.get(0).order() == 1) {
                    movingPart = p;

                    break;
                }
            }

            assertFalse(movingPart == -1);

            TestRecordingCommunicationSpi.spi(client).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (concurrency == PESSIMISTIC)
                        return msg instanceof GridNearLockRequest;
                    else
                        return msg instanceof GridNearTxPrepareRequest;
                }
            });

            final int finalStablePart = stablePart;
            final int finalMovingPart = movingPart;

            IgniteInternalFuture<?> txFut = multithreadedAsync(() -> {
                try (Transaction tx = client.transactions().txStart(concurrency, isolation)) {
                    client.cache(CACHE1).put(finalStablePart, 0); // Will map on crd(order=1).

                    // Next request will remap to ideal topology, but it's not ready on other node except crd.
                    client.cache(CACHE2).put(finalMovingPart, 0);

                    tx.commit();
                }
            }, 1, "tx-thread");

            // Wait until all missing supply messages are blocked.
            assertTrue(GridTestUtils.waitForCondition(() -> leftVerParts.size() == PARTS_CNT - full.get().size(), 5_000));

            // Delay first lock request on late topology.
            TestRecordingCommunicationSpi.spi(client).waitForBlocked();

            // At this point only supply messages should be blocked.
            // Unblock to continue rebalance and trigger ideal topology switch.
            crdSpi.stopBlock(true, null, false, true);

            // Wait until ideal topology is ready on crd.
            crd.context().cache().context().exchange().affinityReadyFuture(idealVer).get(10_000);

            // Other node must wait for full message.
            assertFalse(GridTestUtils.waitForCondition(() ->
                grid(2).context().cache().context().exchange().affinityReadyFuture(idealVer).isDone(), 1_000));

            // Map on unstable topology (PME is in progress on other node).
            TestRecordingCommunicationSpi.spi(client).stopBlock();

            // Capture local transaction.
            IgniteInternalTx tx0 = client.context().cache().context().tm().activeTransactions().iterator().next();

            // Expected behavior: tx must hang (both pessimistic and optimistic) because topology is not ready.
            try {
                txFut.get(3_000);

                fail("TX must not complete");
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                // Expected.
            }

            crdSpi.stopBlock();

            txFut.get();

            // Check transaction map version. Should be mapped on ideal topology.
            assertEquals(tx0.topologyVersionSnapshot(), idealVer);

            awaitPartitionMapExchange();

            checkFutures();
        }
        finally {
            stopAllGrids();
        }
    }
}
