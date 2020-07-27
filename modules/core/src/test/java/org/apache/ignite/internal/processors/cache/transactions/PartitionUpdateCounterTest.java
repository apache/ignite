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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounterTrackingImpl;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounterVolatileImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Basic partition counter tests.
 */
public class PartitionUpdateCounterTest extends GridCommonAbstractTest {
    /** */
    private CacheAtomicityMode mode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
            )
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(8 * 1024 * 1024);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAffinity(new RendezvousAffinityFunction(false, 1)).
            setBackups(1).
            setCacheMode(CacheMode.REPLICATED).
            setAtomicityMode(mode));

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

        cleanPersistenceDir();
    }

    /**
     * Test applying update multiple times in random order.
     */
    @Test
    public void testRandomUpdates() {
        List<int[]> tmp = generateUpdates(1000, 5);

        long expTotal = tmp.stream().mapToInt(pair -> pair[1]).sum();

        PartitionUpdateCounter pc = null;

        for (int i = 0; i < 100; i++) {
            Collections.shuffle(tmp);

            PartitionUpdateCounter pc0 = new PartitionUpdateCounterTrackingImpl(null);

            for (int[] pair : tmp)
                pc0.update(pair[0], pair[1]);

            if (pc == null)
                pc = pc0;
            else {
                assertEquals(pc, pc0);
                assertEquals(expTotal, pc0.get());
                assertTrue(pc0.sequential());

                pc = pc0;
            }
        }
    }

    /**
     * Test if pc correctly reports stale (before current counter) updates.
     * This information is used for logging rollback records only once.
     */
    @Test
    public void testStaleUpdate() {
        PartitionUpdateCounter pc = new PartitionUpdateCounterTrackingImpl(null);

        assertTrue(pc.update(0, 1));
        assertFalse(pc.update(0, 1));

        assertTrue(pc.update(2, 1));
        assertFalse(pc.update(2, 1));

        assertTrue(pc.update(1, 1));
        assertFalse(pc.update(1, 1));
    }

    /**
     * Test multithreaded updates of pc in various modes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedModeMultithreaded() throws Exception {
        PartitionUpdateCounter pc = new PartitionUpdateCounterTrackingImpl(null);

        AtomicBoolean stop = new AtomicBoolean();

        Queue<long[]> reservations = new ConcurrentLinkedQueue<>();

        LongAdder reserveCntr = new LongAdder();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            while (!stop.get() || !reservations.isEmpty()) {
                if (!stop.get() && ThreadLocalRandom.current().nextBoolean()) {
                    int size = ThreadLocalRandom.current().nextInt(9) + 1;

                    reservations.add(new long[] {pc.reserve(size), size}); // Only update if stop flag is set.

                    reserveCntr.add(size);
                }
                else {
                    long[] reserved = reservations.poll();

                    if (reserved == null)
                        continue;

                    pc.update(reserved[0], reserved[1]);
                }
            }
        }, Runtime.getRuntime().availableProcessors() * 2, "updater-thread");

        doSleep(10_000);

        stop.set(true);

        fut.get();

        assertTrue(reservations.isEmpty());

        log.info("counter=" + pc.toString() + ", reserveCntrLocal=" + reserveCntr.sum());

        assertTrue(pc.sequential());

        assertTrue(pc.get() == pc.reserved());

        assertEquals(reserveCntr.sum(), pc.get());
    }

    /**
     * Test logic for handling gaps limit.
     */
    @Test
    public void testMaxGaps() {
        PartitionUpdateCounter pc = new PartitionUpdateCounterTrackingImpl(null);

        int i;
        for (i = 1; i <= PartitionUpdateCounterTrackingImpl.MAX_MISSED_UPDATES; i++)
            pc.update(i * 3, i * 3 + 1);

        i++;
        try {
            pc.update(i * 3, i * 3 + 1);

            fail();
        }
        catch (Exception e) {
            // Expected.
        }
    }

    /**
     *
     */
    @Test
    public void testFoldIntermediateUpdates() {
        PartitionUpdateCounter pc = new PartitionUpdateCounterTrackingImpl(null);

        pc.update(0, 59);

        pc.update(60, 5);

        pc.update(67, 3);

        pc.update(65, 2);

        Iterator<long[]> it = pc.iterator();

        it.next();

        assertFalse(it.hasNext());

        pc.update(59, 1);

        assertTrue(pc.sequential());
    }

    /**
     *
     */
    @Test
    public void testOutOfOrderUpdatesIterator() {
        PartitionUpdateCounter pc = new PartitionUpdateCounterTrackingImpl(null);

        pc.update(67, 3);

        pc.update(1, 58);

        pc.update(60, 5);

        Iterator<long[]> iter = pc.iterator();

        long[] upd = iter.next();

        assertEquals(1, upd[0]);
        assertEquals(58, upd[1]);

        upd = iter.next();

        assertEquals(60, upd[0]);
        assertEquals(5, upd[1]);

        upd = iter.next();

        assertEquals(67, upd[0]);
        assertEquals(3, upd[1]);

        assertFalse(iter.hasNext());
    }

    /**
     *
     */
    @Test
    public void testOverlap() {
        PartitionUpdateCounter pc = new PartitionUpdateCounterTrackingImpl(null);

        assertTrue(pc.update(13, 3));

        assertTrue(pc.update(6, 7));

        assertFalse(pc.update(13, 3));

        assertFalse(pc.update(6, 7));

        Iterator<long[]> iter = pc.iterator();
        assertTrue(iter.hasNext());

        long[] upd = iter.next();

        assertEquals(6, upd[0]);
        assertEquals(10, upd[1]);

        assertFalse(iter.hasNext());
    }

    /** */
    @Test
    public void testAtomicUpdateCounterMultithreaded() throws Exception {
        PartitionUpdateCounter cntr = new PartitionUpdateCounterVolatileImpl(null);

        AtomicInteger id = new AtomicInteger();

        final int max = 1000;

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int val;

                while ((val = id.incrementAndGet()) <= max) {
                    try {
                        cntr.update(val);
                    }
                    catch (IgniteCheckedException e) {
                        fail(X.getFullStackTrace(e));
                    }
                }
            }
        }, Runtime.getRuntime().availableProcessors() * 2, "updater");

        fut.get();

        assertEquals(max, cntr.get());
    }

    /**
     *
     */
    @Test
    public void testWithPersistentNodeTx() throws Exception {
        testWithPersistentNode(CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     *
     */
    @Test
    public void testWithPersistentNodeAtomic() throws Exception {
        testWithPersistentNode(CacheAtomicityMode.ATOMIC);
    }

    /**
     *
     */
    @Test
    public void testGapsSerialization() {
        PartitionUpdateCounter pc = new PartitionUpdateCounterTrackingImpl(null);

        Random r = new Random();

        for (int c = 1; c < 500; c++)
            pc.update(c * 4, r.nextInt(3) + 1);

        final byte[] bytes = pc.getBytes();

        PartitionUpdateCounter pc2 = new PartitionUpdateCounterTrackingImpl(null);
        pc2.init(0, bytes);

        NavigableMap q0 = U.field(pc, "queue");
        NavigableMap q1 = U.field(pc2, "queue");

        assertEquals(q0, q1);
    }

    /**
     * @param mode Mode.
     */
    private void testWithPersistentNode(CacheAtomicityMode mode) throws Exception {
        this.mode = mode;

        try {
            IgniteEx grid0 = startGrid(0);

            grid0.cluster().baselineAutoAdjustEnabled(false);
            grid0.cluster().active(true);
            grid0.cluster().baselineAutoAdjustEnabled(false);

            grid0.cache(DEFAULT_CACHE_NAME).put(0, 0);

            startGrid(1);

            grid0.cluster().setBaselineTopology(2);

            awaitPartitionMapExchange();

            grid0.cache(DEFAULT_CACHE_NAME).put(1, 1);

            assertPartitionsSame(idleVerify(grid0, DEFAULT_CACHE_NAME));

            printPartitionState(DEFAULT_CACHE_NAME, 0);

            stopGrid(grid0.name(), false);

            grid0 = startGrid(grid0.name());

            awaitPartitionMapExchange();

            PartitionUpdateCounter cntr = counter(0, grid0.name());

            if (mode == CacheAtomicityMode.TRANSACTIONAL)
                assertTrue(cntr instanceof PartitionUpdateCounterTrackingImpl);
            else if (mode == CacheAtomicityMode.ATOMIC)
                assertTrue(cntr instanceof PartitionUpdateCounterVolatileImpl);

            assertEquals(cntr.initial(), cntr.get());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cnt Count.
     * @param maxTxSize Max tx size.
     */
    private List<int[]> generateUpdates(int cnt, int maxTxSize) {
        int[] ints = new Random().ints(cnt, 1, maxTxSize + 1).toArray();

        int off = 0;

        List<int[]> res = new ArrayList<>(cnt);

        for (int i = 0; i < ints.length; i++) {
            int val = ints[i];

            res.add(new int[] {off, val});

            off += val;
        }

        return res;
    }
}
