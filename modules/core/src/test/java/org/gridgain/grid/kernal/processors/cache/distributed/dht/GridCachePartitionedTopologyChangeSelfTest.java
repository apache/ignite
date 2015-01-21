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

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Tests that new transactions do not start until partition exchange is completed.
 */
public class GridCachePartitionedTopologyChangeSelfTest extends GridCommonAbstractTest {
    /** Partition does not belong to node. */
    private static final int PARTITION_READER = 0;

    /** Node is primary for partition. */
    private static final int PARTITION_PRIMARY = 1;

    /** Node is backup for partition. */
    private static final int PARTITION_BACKUP = 2;

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        // Discovery.
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setAffinity(new GridCacheConsistentHashAffinityFunction(false, 18));
        cc.setBackups(1);
        cc.setPreloadMode(SYNC);
        cc.setDistributionMode(PARTITIONED_ONLY);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearTxNodeJoined() throws Exception {
        checkTxNodeJoined(PARTITION_READER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryTxNodeJoined() throws Exception {
        checkTxNodeJoined(PARTITION_PRIMARY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupTxNodeJoined() throws Exception {
        checkTxNodeJoined(PARTITION_BACKUP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearTxNodeLeft() throws Exception {
        checkTxNodeLeft(PARTITION_READER);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryTxNodeLeft() throws Exception {
        // This test does not make sense because if node is primary for some partition,
        // it will reside on node until node leaves grid.
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupTxNodeLeft() throws Exception {
        checkTxNodeLeft(PARTITION_BACKUP);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitLocks() throws Exception {
        try {
            startGridsMultiThreaded(2);

            GridKernal[] nodes = new GridKernal[] {(GridKernal)grid(0), (GridKernal)grid(1)};

            Collection<IgniteFuture> futs = new ArrayList<>();

            final CountDownLatch startLatch = new CountDownLatch(1);

            for (final GridKernal node : nodes) {
                List<Integer> parts = partitions(node, PARTITION_PRIMARY);

                Map<Integer, Integer> keyMap = keysFor(node, parts);

                for (final Integer key : keyMap.values()) {
                    futs.add(multithreadedAsync(new Runnable() {
                        @Override public void run() {
                            try {
                                try {
                                    node.jcache(null).lock(key).lock();

                                    info(">>> Acquired explicit lock for key: " + key);

                                    startLatch.await();

                                    info(">>> Acquiring explicit lock for key: " + key * 10);

                                    node.jcache(null).lock(key * 10).lock();

                                    info(">>> Releasing locks [key1=" + key + ", key2=" + key * 10 + ']');
                                }
                                finally {
                                    node.cache(null).unlock(key * 10);
                                    node.cache(null).unlock(key);
                                }
                            }
                            catch (IgniteCheckedException e) {
                                info(">>> Failed to perform lock [key=" + key + ", e=" + e + ']');
                            }
                            catch (InterruptedException ignored) {
                                info(">>> Interrupted while waiting for start latch.");

                                Thread.currentThread().interrupt();
                            }
                        }
                    }, 1));
                }
            }

            IgniteFuture<?> startFut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    try {
                        startGrid(2);

                        info(">>> Started grid2.");
                    }
                    catch (Exception e) {
                        info(">>> Failed to start grid: " + e);
                    }
                }
            }, 1);

            U.sleep(5000);

            assertFalse(startFut.isDone());

            info(">>> Waiting for all locks to be released.");

            startLatch.countDown();

            for (IgniteFuture fut : futs)
                fut.get(1000);

            startFut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkTxNodeJoined(int nodeType) throws Exception {
        startGrids(3);

        final GridKernal g0 = (GridKernal)grid(0);
        final GridKernal g1 = (GridKernal)grid(1);
        final GridKernal g2 = (GridKernal)grid(2);

        GridKernal[] nodes = new GridKernal[] {g0, g1, g2};

        try {
            info(">>> Started nodes [g0=" + g0.localNode().id() + ", g1=" + g1.localNode().id() + ", g2=" +
                g2.localNode().id() + ']');

            final CountDownLatch commitLatch = new CountDownLatch(1);

            Collection<IgniteFuture> futs = new ArrayList<>();

            for (final GridKernal node : nodes) {
                printDistribution(node);

                // Get partitions that does not reside on g0.
                List<Integer> parts = partitions(node, nodeType);

                info(">>> Partitions for node [nodeId=" + node.localNode().id() + ", parts=" + parts +
                   ", type=" + nodeType + ']');

                final Map<Integer, Integer> keysMap = keysFor(node, parts);

                info(">>> Generated keys for node [nodeId=" + node.localNode().id() + ", keysMap=" + keysMap + ']');

                // Start tx for every key in map.
                for (final Integer key : keysMap.values()) {
                    futs.add(multithreadedAsync(new Runnable() {
                        @Override public void run() {
                            GridCache<Integer, Integer> cache = node.cache(null);

                            try {
                                try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                    cache.put(key, key);

                                    info(">>> Locked key, waiting for latch: " + key);

                                    commitLatch.await();

                                    tx.commit();
                                }
                            }
                            catch (IgniteCheckedException e) {
                                info("Failed to run tx for key [key=" + key + ", e=" + e + ']');
                            }
                            catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();

                                info("Got interrupted while waiting for commit latch: " + key);
                            }
                        }
                    }, 1));
                }
            }

            final CountDownLatch joinLatch = new CountDownLatch(1);

            g0.events().localListen(new IgnitePredicate<IgniteEvent>() {
                @Override public boolean apply(IgniteEvent evt) {
                    assert evt.type() == EVT_NODE_JOINED;

                    info(">>> Node has joined: " + evt.node().id());

                    joinLatch.countDown();

                    g0.events().stopLocalListen(this, EVT_NODE_JOINED);

                    return true;
                }
            }, EVT_NODE_JOINED);

            // Now start new node. We do it in a separate thread since startGrid
            // should block until partition exchange completes.
            IgniteFuture startFut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    try {
                        Ignite g3 = startGrid(3);

                        info(">>> Started grid g3: " + g3.cluster().localNode().id());
                    }
                    catch (Exception e) {
                        info(">>> Failed to start 4th node: " + e);
                    }
                }
            }, 1);

            joinLatch.await();

            Thread.sleep(100);

            assertFalse("Node was able to join the grid while there exist pending transactions.", startFut.isDone());

            // Now check that new transactions will wait for new topology version to become available.
            Collection<IgniteFuture> txFuts = new ArrayList<>(nodes.length);

            for (final Ignite g : nodes) {
                txFuts.add(multithreadedAsync(new Runnable() {
                    @Override public void run() {
                        GridCache<Integer, Integer> cache = g.cache(null);

                        int key = (int)Thread.currentThread().getId();

                        try {
                            try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                // This method should block until all previous transactions are completed.
                                cache.put(key, key);

                                info(">>> Acquired second lock for key: " + key);

                                tx.commit();
                            }
                        }
                        catch (IgniteCheckedException e) {
                            info(">>> Failed to execute tx on new topology [key=" + key + ", e=" + e + ']');
                        }
                    }
                }, 1));
            }

            Thread.sleep(500);

            for (IgniteFuture txFut : txFuts)
                assertFalse("New transaction was completed before new node joined topology", txFut.isDone());

            info(">>> Committing pending transactions.");

            commitLatch.countDown();

            for (IgniteFuture fut : futs)
                fut.get(1000);

            startFut.get(1000);

            for (IgniteFuture txFut : txFuts)
                txFut.get(1000);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkTxNodeLeft(int nodeType) throws Exception {
        startGridsMultiThreaded(4);

        final GridKernal g0 = (GridKernal)grid(0);
        final GridKernal g1 = (GridKernal)grid(1);
        final GridKernal g2 = (GridKernal)grid(2);
        final GridKernal g3 = (GridKernal)grid(3);

        GridKernal[] nodes = new GridKernal[] {g0, g1, g2};

        final CountDownLatch commitLatch = new CountDownLatch(1);

        UUID leftNodeId = g3.localNode().id();

        try {
            info(">>> Started nodes [g0=" + g0.localNode().id() + ", g1=" + g1.localNode().id() + ", g2=" +
                g2.localNode().id() + ", g3=" + g3.localNode().id() + ']');

            Collection<IgniteFuture> futs = new ArrayList<>();

            printDistribution(g3);

            for (final GridKernal node : nodes) {
                printDistribution(node);

                // Get partitions that does not reside on g0.
                List<Integer> parts = partitions(node, nodeType);

                info(">>> Partitions for node [nodeId=" + node.localNode().id() + ", parts=" + parts +
                    ", type=" + nodeType + ']');

                final Map<Integer, Integer> keysMap = keysFor(node, parts);

                info(">>> Generated keys for node [nodeId=" + node.localNode().id() + ", keysMap=" + keysMap + ']');

                // Start tx for every key in map.
                for (final Integer key : keysMap.values()) {
                    futs.add(multithreadedAsync(new Runnable() {
                        @Override public void run() {
                            GridCache<Integer, Integer> cache = node.cache(null);

                            try {
                                try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                    cache.put(key, key);

                                    commitLatch.await();

                                    tx.commit();
                                }
                            }
                            catch (IgniteCheckedException e) {
                                info("Failed to run tx for key [key=" + key + ", e=" + e + ']');
                            }
                            catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();

                                info("Got interrupted while waiting for commit latch: " + key);
                            }
                        }
                    }, 1));
                }
            }

            final CountDownLatch leaveLatch = new CountDownLatch(1);

            g0.events().localListen(new IgnitePredicate<IgniteEvent>() {
                @Override public boolean apply(IgniteEvent evt) {
                    assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

                    info(">>> Node has left: " + evt.node().id());

                    leaveLatch.countDown();

                    g0.events().stopLocalListen(this, EVT_NODE_LEFT, EVT_NODE_FAILED);

                    return true;
                }
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);

            // Now stop the node.
            stopGrid(getTestGridName(3), true);

            leaveLatch.await();

            // Now check that new transactions will wait for new topology version to become available.
            Collection<IgniteFuture> txFuts = new ArrayList<>(nodes.length);

            for (final Ignite g : nodes) {
                txFuts.add(multithreadedAsync(new Runnable() {
                    @Override public void run() {
                        GridCache<Integer, Integer> cache = g.cache(null);

                        int key = (int)Thread.currentThread().getId();

                        try {
                            try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                // This method should block until all previous transactions are completed.
                                cache.put(key, key);

                                tx.commit();
                            }
                        }
                        catch (IgniteCheckedException e) {
                            info(">>> Failed to execute tx on new topology [key=" + key + ", e=" + e + ']');
                        }
                    }
                }, 1));
            }

            Thread.sleep(500);

            for (IgniteFuture txFut : txFuts)
                assertFalse("New transaction was completed before old transactions were committed", txFut.isDone());

            info(">>> Committing pending transactions.");

            commitLatch.countDown();

            for (IgniteFuture fut : futs)
                fut.get(1000);

            for (IgniteFuture txFut : txFuts)
                txFut.get(1000);

            for (int i = 0; i < 3; i++) {
                GridCacheConsistentHashAffinityFunction affinity = (GridCacheConsistentHashAffinityFunction)((GridKernal)grid(i))
                    .internalCache().context().config().getAffinity();

                ConcurrentMap addedNodes = U.field(affinity, "addedNodes");

                assertFalse(addedNodes.containsKey(leftNodeId));
            }
        }
        finally {
            info(">>> Shutting down the test.");

            commitLatch.countDown();

            U.sleep(1000);

            stopAllGrids();
        }
    }

    /**
     * Prints partition distribution for node.
     *
     * @param node Node to detect partitions for.
     */
    private void printDistribution(GridKernal node) {
        List<Integer> primary = partitions(node, PARTITION_PRIMARY);
        List<Integer> backup = partitions(node, PARTITION_BACKUP);
        List<Integer> reader = partitions(node, PARTITION_READER);

        info(">>> Partitions distribution calculated [nodeId=" + node.localNode().id() + ", primary=" + primary +
            ", backup=" + backup + ", reader=" + reader + ']');
    }

    /**
     * For each partition given calculates a key that belongs to this partition. Generated keys are
     * in ascending order.
     *
     * @param node Node to use.
     * @param parts Partitions to get keys for.
     * @return Map from partition to key.
     */
    private Map<Integer, Integer> keysFor(GridKernal node, Iterable<Integer> parts) {
        GridCacheContext<Object, Object> ctx = node.internalCache().context();

        Map<Integer, Integer> res = new HashMap<>();

        int key = 0;

        for (Integer part : parts) {
            while (ctx.affinity().partition(key) != part)
                key++;

            res.put(part, key);
        }

        return res;
    }

    /**
     * Gets partitions that are not resided on given node (neither as primary nor as backup).
     *
     * @param node Node to calculate partitions for.
     * @return List of partitions.
     */
    private List<Integer> partitions(Ignite node, int partType) {
        List<Integer> res = new LinkedList<>();

        GridCacheAffinity<Object> aff = node.cache(null).affinity();

        for (int partCnt = aff.partitions(), i = 0; i < partCnt; i++) {
            ClusterNode locNode = node.cluster().localNode();

            switch (partType) {
                // Near, partition should not belong to node.
                case PARTITION_READER: {
                    if (!aff.isPrimaryOrBackup(locNode, i))
                        res.add(i);

                    break;
                }

                // Node should be primary for partition.
                case PARTITION_PRIMARY: {
                    if (aff.isPrimary(locNode, i))
                        res.add(i);

                    break;
                }

                // Node should be backup for partition.
                case PARTITION_BACKUP: {
                    if (aff.isPrimaryOrBackup(locNode, i) && !aff.isPrimary(locNode, i))
                        res.add(i);

                    break;
                }

                default: {
                    assert false;
                }
            }
        }

        return res;
    }
}
