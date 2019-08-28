/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
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
        cfg.setClientMode("client".equals(igniteInstanceName));
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
        ccfg.setOnheapCacheEnabled(false);
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
     *
     */
    private void doTestCrossCacheTxMapOnInvalidTopology(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        try {
            IgniteEx crd = startGrid(0);
            IgniteEx g1 = startGrid(1);

            awaitPartitionMapExchange();

            Ignite client = startGrid("client");
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

            AtomicReference<Set<Integer>> full = new AtomicReference<>();

            crdSpi.blockMessages((node, m) -> {
                if (m instanceof GridDhtPartitionSupplyMessage) {
                    GridDhtPartitionSupplyMessage msg = (GridDhtPartitionSupplyMessage)m;

                    // Allow full rebalance for cache 1 and system cache.
                    if (msg.groupId() == CU.cacheId(CACHE1) || msg.groupId() == CU.cacheId(GridCacheUtils.UTILITY_CACHE_NAME))
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

                    if (msg.topologyVersion().equals(leftVer))
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

            AffinityAssignment assignment = grid(0).cachex(CACHE2).context().affinity().assignment(leftVer);

            // Search for a partition with incompatible assignment.
            int movingPart = -1;

            for (int p = 0; p < assignment.assignment().size(); p++) {
                List<ClusterNode> nodes = assignment.assignment().get(p);
                List<ClusterNode> nodes2 = assignment.idealAssignment().get(p);

                if (!nodes.equals(nodes2) && nodes.get(0).order() == 1)
                    movingPart = p;
            }

            assertFalse(movingPart == -1);

            // Delay rebalance for next top ver.
            TestRecordingCommunicationSpi.spi(grid(2)).blockMessages((node, message) -> {
                if (message instanceof GridDhtPartitionsSingleMessage) {
                    GridDhtPartitionsSingleMessage sm = (GridDhtPartitionsSingleMessage)message;

                    return sm.exchangeId() != null;
                }

                return false;
            });

            TestRecordingCommunicationSpi.spi(client).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message message) {
                    if (concurrency == PESSIMISTIC)
                        return message instanceof GridNearLockRequest;
                    else
                        return message instanceof GridNearTxPrepareRequest;
                }
            });

            final int finalMovingPart = movingPart;
            IgniteInternalFuture<?> txFut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = client.transactions().txStart(concurrency, isolation)) {
                        Map<Integer, Integer> map = new LinkedHashMap<>();

                        for (int p = 0; p < PARTS_CNT; p++)
                            map.put(p, p);

                        client.cache(CACHE1).putAll(map); // Will successfully lock topology.
                        client.cache(CACHE2).put(finalMovingPart, 0); // Should remap but will go through without fix.

                        tx.commit();
                    }
                }
            }, 1, "tx-thread");

            TestRecordingCommunicationSpi.spi(client).waitForBlocked();

            crdSpi.stopBlock(); // Continue rebalance and trigger next topology switch.

            TestRecordingCommunicationSpi.spi(grid(2)).waitForBlocked();

            TestRecordingCommunicationSpi.spi(client).stopBlock();

            doSleep(5000); // Make sure request will listen for current topology future completion.

            TestRecordingCommunicationSpi.spi(grid(2)).stopBlock();

            crdSpi.stopBlock();

            awaitPartitionMapExchange();

            txFut.get();

            checkFutures();
        }
        finally {
            stopAllGrids();
        }
    }
}
