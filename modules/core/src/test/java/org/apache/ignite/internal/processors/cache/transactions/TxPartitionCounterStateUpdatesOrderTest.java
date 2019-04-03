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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
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

        Ignite crd = startGridsMultiThreaded(SERVER_NODES);

        IgniteEx client = startGrid("client");

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

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        List<Integer> primaryKeys = partitionKeys(cache, PARTITION_ID, 10000, 0);

        Ignite prim = primaryNode(primaryKeys.get(0), DEFAULT_CACHE_NAME);

        final List<Ignite> backups = backupNodes(primaryKeys.get(0), DEFAULT_CACHE_NAME);

        long stop = U.currentTimeMillis() + 60_000;

        Random r = new Random();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            while(U.currentTimeMillis() < stop) {
                doSleep(5000);

                Ignite backup = backups.get(r.nextInt(backups.size()));

                assertFalse(prim == backup);

                String name = backup.name();

                stopGrid(true, name);

                try {
                    waitForTopology(SERVER_NODES);

                    doSleep(15000);

                    startGrid(name);
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
                int rangeStart = r.nextInt(primaryKeys.size() - 500);
                int range = 1 + r.nextInt(499);

                List<Integer> keys = primaryKeys.subList(rangeStart, rangeStart + range);

                try(Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 0)) {
                    for (Integer key : keys) {
                        boolean rmv = r.nextFloat() < 0.7;
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

        waitForTopology(SERVER_NODES + 1);

        awaitPartitionMapExchange();

        log.info("TX: puts=" + puts.sum() + ", removes=" + removes.sum());

        assertPartitionsSame(idleVerify(client, DEFAULT_CACHE_NAME));

        assertCountersSame(PARTITION_ID, true);

//        cur.close();
//
//        assertEquals(size, events.size());
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
