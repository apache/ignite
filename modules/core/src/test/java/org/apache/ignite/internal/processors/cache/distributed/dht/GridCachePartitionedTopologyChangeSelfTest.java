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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

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
    @Override protected void beforeTest() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-807");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        // Discovery.
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setAffinity(new RendezvousAffinityFunction(false, 18));
        cc.setBackups(1);
        cc.setRebalanceMode(SYNC);
        cc.setNearConfiguration(null);

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

            IgniteKernal[] nodes = new IgniteKernal[] {(IgniteKernal)grid(0), (IgniteKernal)grid(1)};

            Collection<IgniteInternalFuture> futs = new ArrayList<>();

            final CountDownLatch startLatch = new CountDownLatch(1);

            for (final IgniteKernal node : nodes) {
                List<Integer> parts = partitions(node, PARTITION_PRIMARY);

                Map<Integer, Integer> keyMap = keysFor(node, parts);

                for (final Integer key : keyMap.values()) {
                    futs.add(multithreadedAsync(new Runnable() {
                        @Override public void run() {
                            try {
                                Lock lock = node.cache(null).lock(key);

                                lock.lock();

                                try {

                                    info(">>> Acquired explicit lock for key: " + key);

                                    startLatch.await();

                                    info(">>> Acquiring explicit lock for key: " + key * 10);

                                    Lock lock10 = node.cache(null).lock(key * 10);

                                    lock10.lock();

                                    try {
                                        info(">>> Releasing locks [key1=" + key + ", key2=" + key * 10 + ']');
                                    }
                                    finally {
                                        lock10.unlock();
                                    }
                                }
                                finally {
                                    lock.unlock();
                                }
                            }
                            catch (CacheException e) {
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

            IgniteInternalFuture<?> startFut = multithreadedAsync(new Runnable() {
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

            for (IgniteInternalFuture fut : futs)
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

        final IgniteKernal g0 = (IgniteKernal)grid(0);
        final IgniteKernal g1 = (IgniteKernal)grid(1);
        final IgniteKernal g2 = (IgniteKernal)grid(2);

        IgniteKernal[] nodes = new IgniteKernal[] {g0, g1, g2};

        try {
            info(">>> Started nodes [g0=" + g0.localNode().id() + ", g1=" + g1.localNode().id() + ", g2=" +
                g2.localNode().id() + ']');

            final CountDownLatch commitLatch = new CountDownLatch(1);

            Collection<IgniteInternalFuture> futs = new ArrayList<>();

            for (final IgniteKernal node : nodes) {
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
                            IgniteCache<Integer, Integer> cache = node.cache(null);

                            try {
                                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                    cache.put(key, key);

                                    info(">>> Locked key, waiting for latch: " + key);

                                    commitLatch.await();

                                    tx.commit();
                                }
                            }
                            catch (CacheException e) {
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

            g0.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt.type() == EVT_NODE_JOINED;

                    info(">>> Node has joined: " + evt.node().id());

                    joinLatch.countDown();

                    g0.events().stopLocalListen(this, EVT_NODE_JOINED);

                    return true;
                }
            }, EVT_NODE_JOINED);

            // Now start new node. We do it in a separate thread since startGrid
            // should block until partition exchange completes.
            IgniteInternalFuture startFut = multithreadedAsync(new Runnable() {
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
            Collection<IgniteInternalFuture> txFuts = new ArrayList<>(nodes.length);

            for (final Ignite g : nodes) {
                txFuts.add(multithreadedAsync(new Runnable() {
                    @Override public void run() {
                        IgniteCache<Integer, Integer> cache = g.cache(null);

                        int key = (int)Thread.currentThread().getId();

                        try {
                            try (Transaction tx = g.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                // This method should block until all previous transactions are completed.
                                cache.put(key, key);

                                info(">>> Acquired second lock for key: " + key);

                                tx.commit();
                            }
                        }
                        catch (CacheException e) {
                            info(">>> Failed to execute tx on new topology [key=" + key + ", e=" + e + ']');
                        }
                    }
                }, 1));
            }

            Thread.sleep(500);

            for (IgniteInternalFuture txFut : txFuts)
                assertFalse("New transaction was completed before new node joined topology", txFut.isDone());

            info(">>> Committing pending transactions.");

            commitLatch.countDown();

            for (IgniteInternalFuture fut : futs)
                fut.get(1000);

            startFut.get(1000);

            for (IgniteInternalFuture txFut : txFuts)
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

        final IgniteKernal g0 = (IgniteKernal)grid(0);
        final IgniteKernal g1 = (IgniteKernal)grid(1);
        final IgniteKernal g2 = (IgniteKernal)grid(2);
        final IgniteKernal g3 = (IgniteKernal)grid(3);

        IgniteKernal[] nodes = new IgniteKernal[] {g0, g1, g2};

        final CountDownLatch commitLatch = new CountDownLatch(1);

        UUID leftNodeId = g3.localNode().id();

        try {
            info(">>> Started nodes [g0=" + g0.localNode().id() + ", g1=" + g1.localNode().id() + ", g2=" +
                g2.localNode().id() + ", g3=" + g3.localNode().id() + ']');

            Collection<IgniteInternalFuture> futs = new ArrayList<>();

            printDistribution(g3);

            for (final IgniteKernal node : nodes) {
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
                            IgniteCache<Integer, Integer> cache = node.cache(null);

                            try {
                                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                    cache.put(key, key);

                                    commitLatch.await();

                                    tx.commit();
                                }
                            }
                            catch (CacheException e) {
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

            g0.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
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
            Collection<IgniteInternalFuture> txFuts = new ArrayList<>(nodes.length);

            for (final Ignite g : nodes) {
                txFuts.add(multithreadedAsync(new Runnable() {
                    @Override public void run() {
                        IgniteCache<Integer, Integer> cache = g.cache(null);

                        int key = (int)Thread.currentThread().getId();

                        try {
                            try (Transaction tx = g.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                // This method should block until all previous transactions are completed.
                                cache.put(key, key);

                                tx.commit();
                            }
                        }
                        catch (CacheException e) {
                            info(">>> Failed to execute tx on new topology [key=" + key + ", e=" + e + ']');
                        }
                    }
                }, 1));
            }

            Thread.sleep(500);

            for (IgniteInternalFuture txFut : txFuts)
                assertFalse("New transaction was completed before old transactions were committed", txFut.isDone());

            info(">>> Committing pending transactions.");

            commitLatch.countDown();

            for (IgniteInternalFuture fut : futs)
                fut.get(1000);

            for (IgniteInternalFuture txFut : txFuts)
                txFut.get(1000);

            for (int i = 0; i < 3; i++) {
                Affinity affinity = grid(i).affinity(null);

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
    private void printDistribution(IgniteKernal node) {
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
    private Map<Integer, Integer> keysFor(IgniteKernal node, Iterable<Integer> parts) {
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

        Affinity<Object> aff = node.affinity(null);

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