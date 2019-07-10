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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.BlockTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test partitions consistency in various scenarios.
 */
public class TxPartitionCounterStateConsistencyTest extends TxPartitionCounterStateAbstractTest {
    /** */
    public static final int PARTITION_ID = 0;

    /** */
    public static final int SERVER_NODES = 3;

    /** */
    protected TcpDiscoverySpi customDiscoSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (customDiscoSpi != null) {
            cfg.setDiscoverySpi(customDiscoSpi);

            customDiscoSpi = null;
        }

        return cfg;
    }

    /**
     * Test if same updates order on all owners after txs are finished.
     */
    public void testSingleThreadedUpdateOrder() throws Exception {
        backups = 2;

        startGridsMultiThreaded(SERVER_NODES);

        Ignite client = startGrid("client");

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        List<Integer> keys = partitionKeys(cache, PARTITION_ID, 100, 0);

        LinkedList<T2<Integer, GridCacheOperation>> ops = new LinkedList<>();

        cache.put(keys.get(0), new TestVal(keys.get(0)));
        ops.add(new T2<>(keys.get(0), GridCacheOperation.CREATE));

        cache.put(keys.get(1), new TestVal(keys.get(1)));
        ops.add(new T2<>(keys.get(1), GridCacheOperation.CREATE));

        cache.put(keys.get(2), new TestVal(keys.get(2)));
        ops.add(new T2<>(keys.get(2), GridCacheOperation.CREATE));

        assertCountersSame(PARTITION_ID, false);

        cache.remove(keys.get(2));
        ops.add(new T2<>(keys.get(2), GridCacheOperation.DELETE));

        cache.remove(keys.get(1));
        ops.add(new T2<>(keys.get(1), GridCacheOperation.DELETE));

        cache.remove(keys.get(0));
        ops.add(new T2<>(keys.get(0), GridCacheOperation.DELETE));

        assertCountersSame(PARTITION_ID, false);

        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            checkWAL((IgniteEx)ignite, new LinkedList<>(ops), 6);
        }
    }

    /**
     * Test primary-backup partitions consistency while restarting primary node under load.
     */
    public void testPartitionConsistencyWithPrimaryRestart() throws Exception {
        backups = 2;

        Ignite prim = startGridsMultiThreaded(SERVER_NODES);

        Ignite client = startGrid("client");

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        List<Integer> primaryKeys = primaryKeys(prim.cache(DEFAULT_CACHE_NAME), 10_000);

        long stop = U.currentTimeMillis() + 60_000;

        Random r = new Random();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            while (U.currentTimeMillis() < stop) {
                doSleep(3000);

                stopGrid(true, prim.name());

                try {
                    awaitPartitionMapExchange();

                    startGrid(prim.name());

                    awaitPartitionMapExchange();

                    doSleep(5000);
                }
                catch (Exception e) {
                    fail(X.getFullStackTrace(e));
                }
            }
        }, 1, "node-restarter");

        doRandomUpdates(r, client, primaryKeys, cache, stop).get();
        fut.get();

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));
    }

    /**
     * Test primary-backup partitions consistency while restarting random backup nodes under load.
     */
    public void testPartitionConsistencyWithBackupsRestart() throws Exception {
        backups = 2;

        final int srvNodes = SERVER_NODES + 1; // Add one non-owner node to test to increase entropy.

        Ignite prim = startGrids(srvNodes);

        prim.cluster().active(true);

        IgniteCache<Object, Object> cache = prim.cache(DEFAULT_CACHE_NAME);

        List<Integer> primaryKeys = primaryKeys(cache, 10_000);

        List<Ignite> backups = backupNodes(primaryKeys.get(0), DEFAULT_CACHE_NAME);

        assertFalse(backups.contains(prim));

        long stop = U.currentTimeMillis() + 3 * 60_000;

        long seed = System.nanoTime();

        log.info("Seed: " + seed);

        Random r = new Random(seed);

        assertTrue(prim == grid(0));

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            while (U.currentTimeMillis() < stop) {
                doSleep(5_000);

                Ignite restartNode = grid(1 + r.nextInt(backups.size()));

                assertFalse(prim == restartNode);

                String name = restartNode.name();

                stopGrid(true, name);

                try {
                    waitForTopology(SERVER_NODES);

                    doSleep(15_000);

                    startGrid(name);

                    awaitPartitionMapExchange();
                }
                catch (Exception e) {
                    fail(X.getFullStackTrace(e));
                }
            }
        }, 1, "node-restarter");

        doRandomUpdates(r, prim, primaryKeys, cache, stop).get();
        fut.get();

        assertPartitionsSame(idleVerify(prim, DEFAULT_CACHE_NAME));
    }

    /**
     * Tests reproduces the problem: deferred removal queue should never be cleared during rebalance OR rebalanced
     * entries could undo deletion causing inconsistency.
     */
    public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_RemoveQueueCleared() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL, "1000");

        try {
            backups = 2;

            Ignite prim = startGridsMultiThreaded(SERVER_NODES);

            int[] primaryParts = prim.affinity(DEFAULT_CACHE_NAME).primaryPartitions(prim.cluster().localNode());

            List<Integer> keys = partitionKeys(prim.cache(DEFAULT_CACHE_NAME), primaryParts[0], 2, 0);

            prim.cache(DEFAULT_CACHE_NAME).put(keys.get(0), keys.get(0));
            prim.cache(DEFAULT_CACHE_NAME).put(keys.get(1), keys.get(1));

            forceCheckpoint();

            List<Ignite> backups = backupNodes(keys.get(0), DEFAULT_CACHE_NAME);

            assertFalse(backups.contains(prim));

            stopGrid(true, backups.get(0).name());

            prim.cache(DEFAULT_CACHE_NAME).put(keys.get(0), keys.get(0));

            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(prim);

            spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage);

            IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                try {
                    spi.waitForBlocked();
                }
                catch (InterruptedException e) {
                    fail(X.getFullStackTrace(e));
                }

                prim.cache(DEFAULT_CACHE_NAME).remove(keys.get(0));

                doSleep(2000);

                prim.cache(DEFAULT_CACHE_NAME).remove(keys.get(1)); // Ensure queue cleanup is triggered.

                spi.stopBlock();
            });

            startGrid(backups.get(0).name());

            awaitPartitionMapExchange();

            fut.get();

            assertPartitionsSame(idleVerify(prim, DEFAULT_CACHE_NAME));

            assertCountersSame(PARTITION_ID, true);
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL);
        }
    }

    /**
     * Tests reproduces the problem: in-place update in tree during rebalance in partition was not handled as update
     * causing missed WAL record which has to be processed on recovery.
     *
     * @throws Exception
     */
    public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_CheckpointDuringRebalance() throws Exception {
        backups = 2;

        Ignite crd = startGridsMultiThreaded(SERVER_NODES);

        int[] primaryParts = crd.affinity(DEFAULT_CACHE_NAME).primaryPartitions(crd.cluster().localNode());

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        List<Integer> p1Keys = partitionKeys(cache, primaryParts[0], 2, 0);
        List<Integer> p2Keys = partitionKeys(cache, primaryParts[1], 2, 0);

        cache.put(p1Keys.get(0), 0); // Will be historically rebalanced.
        cache.put(p1Keys.get(1), 1);

        forceCheckpoint();

        Ignite backup = backupNode(p1Keys.get(0), DEFAULT_CACHE_NAME);

        final String backupName = backup.name();

        assertTrue(backupNodes(p2Keys.get(0), DEFAULT_CACHE_NAME).contains(backup));

        stopGrid(true, backup.name());

        cache.put(p1Keys.get(1), 2);
        cache.put(p2Keys.get(1), 1); // Will be fully rebalanced.

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(crd);

        // Prevent rebalance completion.
        spi.blockMessages((node, msg) -> {
            String name = (String)node.attributes().get(ATTR_IGNITE_INSTANCE_NAME);

            if (name.equals(backupName) && msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)msg;

                Map<Integer, CacheEntryInfoCollection> infos = U.field(msg0, "infos");

                return infos.keySet().contains(primaryParts[0]); // Delay historical rebalance.
            }

            return false;
        });

        backup = startGrid(backupName);

        spi.waitForBlocked();

        forceCheckpoint(backup);

        spi.stopBlock();

        // While message is delayed topology version shouldn't change to ideal.
        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

        stopGrid(true, backupName);

        startGrid(backupName);

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /** */
    public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_SameAffinityPME() throws Exception {
        backups = 2;

        Ignite crd = startGridsMultiThreaded(SERVER_NODES);

        crd.cluster().active(true);

        Ignite client = startGrid("client");

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        int threads = 8;

        int keys = 200;
        int batch = 10;

        CyclicBarrier sync = new CyclicBarrier(threads + 1);

        AtomicBoolean done = new AtomicBoolean();

        Random r = new Random();

        LongAdder puts = new LongAdder();
        LongAdder restarts = new LongAdder();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            U.awaitQuiet(sync);

            while(!done.get()) {
                int batch0 = 1 + r.nextInt(batch - 1);
                int start = r.nextInt(keys - batch0);

                try(Transaction tx = client.transactions().txStart()) {
                    Map<Integer, Integer> map = new TreeMap<>();

                    IntStream.range(start, start + batch0).forEach(value -> map.put(value, value));

                    cache.putAll(map);

                    tx.commit();

                    puts.add(batch0);
                }
            }
        }, threads, "load-thread");

        IgniteInternalFuture fut2 = GridTestUtils.runAsync(() -> {
            U.awaitQuiet(sync);

            while(!done.get()) {
                try {
                    IgniteCache cache1 = client.createCache(cacheConfiguration(DEFAULT_CACHE_NAME + "2"));

                    cache1.destroy();

                    restarts.increment();
                }
                catch (Exception e) {
                    fail(X.getFullStackTrace(e));
                }
            }
        });

        doSleep(60_000);

        done.set(true);

        fut.get();
        fut2.get();

        log.info("TX: puts=" + puts.sum() + ", restarts=" + restarts.sum() + ", size=" + cache.size());

        assertPartitionsSame(idleVerify(client));
    }

    /**
     * Tests tx load concurrently with PME not changing tx topology.
     */
    public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_TxDuringPME() throws Exception {
        backups = 2;

        Ignite crd = startGrid(0);
        startGrid(1);
        startGrid(2);

        crd.cluster().active(true);

        Ignite client = startGrid("client");

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        // Put one key per partition.
        for (int k = 0; k < PARTS_CNT; k++)
            cache.put(k, 0);

        IgniteEx grid = grid(1);
        Integer key0 = primaryKey(grid.cache(DEFAULT_CACHE_NAME));
        Integer key = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(crd);

        spi.blockMessages((node, message) -> {
            if (message instanceof GridDhtPartitionsFullMessage) {
                GridDhtPartitionsFullMessage tmp = (GridDhtPartitionsFullMessage)message;

                return tmp.exchangeId() != null;
            }

            return false;
        });

        // Locks mapped wait.
        CountDownLatch l = new CountDownLatch(1);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            U.awaitQuiet(l);

            try {
                startGrid(SERVER_NODES);
            }
            catch (Exception e) {
                fail(X.getFullStackTrace(e));
            }
        });

        TestRecordingCommunicationSpi cliSpi = TestRecordingCommunicationSpi.spi(client);
        cliSpi.blockMessages((node, message) -> {
            // Block second lock map req.
            return message instanceof GridNearLockRequest && node.order() == crd.cluster().localNode().order();
        });

        IgniteInternalFuture txFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart()) {
                Map<Integer, Integer> map = new LinkedHashMap<>();

                map.put(key, key); // clientFirst=true in lockAll.
                map.put(key0, key0); // clientFirst=false in lockAll.

                cache.putAll(map);

                tx.commit(); // Will start preparing in the middle of PME.
            }
        });

        IgniteInternalFuture crdFut = GridTestUtils.runAsync(() -> {
            try {
                cliSpi.waitForBlocked(); // Delay first before PME.

                l.countDown();

                spi.waitForBlocked(); // Block PME after finish on crd and wait on others.

                cliSpi.stopBlock(); // Start remote lock mapping.
            }
            catch (InterruptedException e) {
                fail();
            }
        });

        txFut.get();
        crdFut.get();

        spi.stopBlock();

        fut.get();

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
    }

    /**
     * Tests tx load concurrently with PME for switching late affinity.
     * <p>
     * Scenario: two keys tx mapped locally on late affinity topology and when mapped and prepared remotely on ideal
     * topology, first key is mapped to non-moving partition, second is mapped on moving partition.
     * <p>
     * Success: key over moving partition is prepared on new owner (choosed after late affinity switch),
     * otherwise it's possible txs are prepared on different primaries after late affinity switch.
     */
    public void testPartitionConsistencyDuringRebalanceAndConcurrentUpdates_LateAffinitySwitch() throws Exception {
        backups = 1;

        customDiscoSpi = new BlockTcpDiscoverySpi().setIpFinder(IP_FINDER);

        Field rndAddrsField = U.findField(BlockTcpDiscoverySpi.class, "skipAddrsRandomization");
        assertNotNull(rndAddrsField);
        rndAddrsField.set(customDiscoSpi, true);

        Ignite crd = startGrid(0); // Start coordinator with custom discovery SPI.
        IgniteEx g1 = startGrid(1);
        startGrid(2);

        crd.cluster().active(true);

        // Same name pattern as in test configuration.
        String consistentId = "node" + getTestIgniteInstanceName(3);

        List<Integer> g1Keys = primaryKeys(g1.cache(DEFAULT_CACHE_NAME), 10);
        List<Integer> movingFromG1 = movingKeysAfterJoin(g1, DEFAULT_CACHE_NAME, 10, null, consistentId);

        // Retain only stable keys;
        g1Keys.removeAll(movingFromG1);

        // The key will move from grid0 to grid3.
        Integer key = movingKeysAfterJoin(crd, DEFAULT_CACHE_NAME, 1, null, consistentId).get(0);

        IgniteEx g3 = startGrid(3);

        assertEquals(consistentId, g3.localNode().consistentId());

        resetBaselineTopology();
        awaitPartitionMapExchange();

        assertTrue(crd.affinity(DEFAULT_CACHE_NAME).isPrimary(g1.localNode(), g1Keys.get(0)));

        stopGrid(3);

        Ignite client = startGrid("client");

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cache2 = client.getOrCreateCache(cacheConfiguration(DEFAULT_CACHE_NAME + "2"));

        // Put one key per partition.
        for (int k = 0; k < PARTS_CNT; k++) {
            cache.put(k, 0);
            cache2.put(k, 0);
        }

        CountDownLatch resumeDiscoSndLatch = new CountDownLatch(1);

        BlockTcpDiscoverySpi crdDiscoSpi = (BlockTcpDiscoverySpi)grid(0).configuration().getDiscoverySpi();
        CyclicBarrier sync = new CyclicBarrier(2);

        crdDiscoSpi.setClosure((node, msg) -> {
            if (msg instanceof CacheAffinityChangeMessage) {
                U.awaitQuiet(sync);
                U.awaitQuiet(resumeDiscoSndLatch);
            }

            return null;
        });

        // Locks mapped wait.
        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(SERVER_NODES);

                awaitPartitionMapExchange();
            }
            catch (Exception e) {
                fail(X.getFullStackTrace(e));
            }
        });

        sync.await();

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> msg instanceof GridNearLockRequest);

        IgniteInternalFuture txFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = client.transactions().txStart()) {
                    Map<Integer, Integer> map = new LinkedHashMap<>();

                    map.put(g1Keys.get(0), g1Keys.get(0)); // clientFirst=true in lockAll mapped to stable part.
                    map.put(key, key); // clientFirst=false in lockAll mapped to moving part.

                    cache.putAll(map);
                    cache2.putAll(new LinkedHashMap<>(map));

                    tx.commit(); // Will start preparing in the middle of PME.
                }
            }
        });

        IgniteInternalFuture lockFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    // Wait for first lock request sent on local (late) topology.
                    clientSpi.waitForBlocked();
                    // Continue late switch PME.
                    resumeDiscoSndLatch.countDown();
                    crdDiscoSpi.setClosure(null);

                    // Wait late affinity switch.
                    awaitPartitionMapExchange();
                    // Continue tx mapping and preparing.
                    clientSpi.stopBlock();
                }
                catch (InterruptedException e) {
                    fail(X.getFullStackTrace(e));
                }
            }
        });

        fut.get();
        txFut.get();
        lockFut.get();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

        // TX must be prepared over new owner.
        PartitionUpdateCounter cntr = counter(key, grid(3).name());
        assertNotNull(cntr);
        assertEquals(cntr.toString(), 2, cntr.reserved());

        PartitionUpdateCounter cntr2 = counter(key, DEFAULT_CACHE_NAME + "2", grid(3).name());
        assertNotNull(cntr2);
        assertEquals(cntr2.toString(), 2, cntr2.reserved());
    }

    /**
     * @param ignite Ignite.
     */
    private WALIterator walIterator(IgniteEx ignite) throws IgniteCheckedException {
        IgniteWriteAheadLogManager walMgr = ignite.context().cache().context().wal();

        return walMgr.replay(null);
    }

    /**
     * @param ig Ignite instance.
     * @param ops Ops queue.
     * @param exp Expected updates.
     */
    private void checkWAL(IgniteEx ig, Queue<T2<Integer, GridCacheOperation>> ops,
        int exp) throws IgniteCheckedException {
        WALIterator iter = walIterator(ig);

        long cntr = 0;

        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> tup = iter.next();

            if (tup.get2() instanceof DataRecord) {
                T2<Integer, GridCacheOperation> op = ops.poll();

                DataRecord rec = (DataRecord)tup.get2();

                assertEquals(1, rec.writeEntries().size());

                DataEntry entry = rec.writeEntries().get(0);

                assertEquals(op.get1(),
                    entry.key().value(internalCache(ig, DEFAULT_CACHE_NAME).context().cacheObjectContext(), false));

                assertEquals(op.get2(), entry.op());

                assertEquals(entry.partitionCounter(), ++cntr);
            }
        }

        assertEquals(exp, cntr);
        assertTrue(ops.isEmpty());
    }

    /**
     * @param r Random.
     * @param near Near node.
     * @param primaryKeys Primary keys.
     * @param cache Cache.
     * @param stop Time to stop.
     * @return Finish future.
     */
    private IgniteInternalFuture<?> doRandomUpdates(Random r, Ignite near, List<Integer> primaryKeys,
        IgniteCache<Object, Object> cache, long stop) throws Exception {
        LongAdder puts = new LongAdder();
        LongAdder removes = new LongAdder();

        final int max = 100;

        return multithreadedAsync(() -> {
            while (U.currentTimeMillis() < stop) {
                int rangeStart = r.nextInt(primaryKeys.size() - max);
                int range = 5 + r.nextInt(max - 5);

                List<Integer> keys = primaryKeys.subList(rangeStart, rangeStart + range);

                try (Transaction tx = near.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 0)) {
                    List<Integer> insertedKeys = new ArrayList<>();

                    for (Integer key : keys) {
                        cache.put(key, key);
                        insertedKeys.add(key);

                        puts.increment();

                        boolean rmv = r.nextFloat() < 0.4;
                        if (rmv) {
                            key = insertedKeys.get(r.nextInt(insertedKeys.size()));

                            cache.remove(key);

                            insertedKeys.remove(key);

                            removes.increment();
                        }
                    }

                    tx.commit();
                }
                catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), X.hasCause(e, ClusterTopologyException.class) ||
                        X.hasCause(e, TransactionRollbackException.class));
                }
            }

            log.info("TX: puts=" + puts.sum() + ", removes=" + removes.sum() + ", size=" + cache.size());

        }, Runtime.getRuntime().availableProcessors() * 2, "tx-update-thread");
    }

    /** */
    private static class TestVal {
        /** */
        int id;

        /**
         * @param id Id.
         */
        public TestVal(int id) {
            this.id = id;
        }
    }

    /**
     * Use increased timeout because history rebalance could take a while.
     * Better to have utility method allowing to wait for specific rebalance future.
     */
    @Override protected long getPartitionMapExchangeTimeout() {
        return getTestTimeout();
    }

}
