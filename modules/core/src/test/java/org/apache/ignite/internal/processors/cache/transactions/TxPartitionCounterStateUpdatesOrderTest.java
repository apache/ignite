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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests if updates using new counter implementation is applied in expected order.
 */
public class TxPartitionCounterStateUpdatesOrderTest extends TxPartitionCounterStateAbstractTest {
    /** */
    public static final int PARTITION_ID = 0;

    /** */
    public static final int SERVER_NODES = 3;

    /**
     * Should observe same order of updates on all owners.
     * @throws Exception
     */
    @Test
    public void testSingleThreadedUpdateOrder() throws Exception {
        backups = 2;

        Ignite crd = startGridsMultiThreaded(3);

        IgniteEx client = startGrid("client");

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
     * TODO same test with historical rebalanbce and different backups(1,2).
     * TODO missing updates and removes.
     */
    @Test
    public void testMultiThreadedUpdateOrderWithPrimaryRestart() throws Exception {
        backups = 2;

        Ignite prim = startGrids(SERVER_NODES);

        prim.cluster().active(true);

        // TODO FIXME enable CQ in the test.
//        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();
//
//        ConcurrentLinkedHashMap<Object, T2<Object, Long>> events = new ConcurrentLinkedHashMap<>();
//
//        qry.setLocalListener(evts -> {
//            for (CacheEntryEvent<?, ?> event : evts) {
//                CacheQueryEntryEvent e0 = (CacheQueryEntryEvent)event;
//
//                events.put(event.getKey(), new T2<>(event.getValue(), e0.getPartitionUpdateCounter()));
//            }
//        });
//
//        QueryCursor<Cache.Entry<Object, Object>> cur = client.cache(DEFAULT_CACHE_NAME).query(qry);

        assertFalse(isRemoteJvm(prim.name()));

        IgniteCache<Object, Object> cache = prim.cache(DEFAULT_CACHE_NAME);

        List<Integer> primaryKeys = primaryKeys(cache, 10000);

        //final List<Ignite> backups = backupNodes(primaryKeys.get(0), DEFAULT_CACHE_NAME);

        long stop = U.currentTimeMillis() + 3 * 60_000;

        Random r = new Random();

        assertTrue(prim == grid(0));

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            while(U.currentTimeMillis() < stop) {
                doSleep(5000);

                Ignite restartNode = grid(1 + r.nextInt(3));

                assertFalse(prim == restartNode);

                String name = restartNode.name();

                stopGrid(true, name);
                //IgniteProcessProxy.kill(name);

                try {
                    //waitForTopology(SERVER_NODES - 1);

                    doSleep(15000);

                    startGrid(name);

                    awaitPartitionMapExchange();
                }
                catch (Exception e) {
                    fail();
                }
            }
        }, 1, "node-restarter");

        //LongAdder cnt = new LongAdder();

        final int threads = 4; //Runtime.getRuntime().availableProcessors();

        LongAdder puts = new LongAdder();
        LongAdder removes = new LongAdder();

        IgniteInternalFuture<?> fut2 = multithreadedAsync(() -> {
            while (U.currentTimeMillis() < stop) {
                int rangeStart = r.nextInt(primaryKeys.size() - 3);
                int range = 1 + r.nextInt(3);

                List<Integer> keys = primaryKeys.subList(rangeStart, rangeStart + range);

                try(Transaction tx = prim.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 0)) {
                    for (Integer key : keys) {
                        boolean rmv = r.nextFloat() < 0.4;
                        if (rmv) {
                            cache.remove(key);

                            removes.increment();
                        }
                        else {
                            cache.put(key, key);

                            puts.increment();
                        }
                    }

                    tx.commit();
                }

                //cnt.increment();
            }
        }, threads, "tx-put-thread");

        fut.get();
        fut2.get();

        log.info("TX: puts=" + puts.sum() + ", removes=" + removes.sum() + ", size=" + cache.size());

        assertPartitionsSame(idleVerify(prim, DEFAULT_CACHE_NAME));

        //assertCountersSame(PARTITION_ID, true);

//        cur.close();
//
//        assertEquals(size, events.size());
    }

    @Test
    public void testDelete() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL, "1000");
        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        try {
            backups = 2;

            Ignite crd = startGridsMultiThreaded(SERVER_NODES);

            List<Integer> keys = partitionKeys(crd.cache(DEFAULT_CACHE_NAME), PARTITION_ID, 2, 0);

            Ignite prim = primaryNode(keys.get(0), DEFAULT_CACHE_NAME);

            prim.cache(DEFAULT_CACHE_NAME).put(keys.get(0), keys.get(0));
            prim.cache(DEFAULT_CACHE_NAME).put(keys.get(1), keys.get(1));

            forceCheckpoint();

            List<Ignite> backups = backupNodes(keys.get(0), DEFAULT_CACHE_NAME);

            stopGrid(true, backups.get(0).name());

            prim.cache(DEFAULT_CACHE_NAME).put(keys.get(0), keys.get(0));

            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(prim);

            spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionSupplyMessage);

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        spi.waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        fail(X.getFullStackTrace(e));
                    }

                    prim.cache(DEFAULT_CACHE_NAME).remove(keys.get(0));

                    doSleep(5000);

                    //prim.cache(DEFAULT_CACHE_NAME).remove(keys.get(1));

                    spi.stopBlock();
                }
            });

            startGrid(backups.get(0).name());

            awaitPartitionMapExchange();

            fut.get();

            assertPartitionsSame(idleVerify(prim, DEFAULT_CACHE_NAME));
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL);
            System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);
        }
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
    private void checkWAL(IgniteEx ig, Queue<T2<Integer, GridCacheOperation>> ops, int exp) throws IgniteCheckedException {
        WALIterator iter = walIterator(ig);

        long cntr = 0;

        while(iter.hasNext()) {
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
}
