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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Failover tests for cache data structures.
 */
public abstract class GridCacheAbstractDataStructuresFailoverSelfTest extends IgniteCollectionAbstractTest {
    /** */
    private static final long TEST_TIMEOUT = 2 * 60 * 1000;

    /** */
    private static final String NEW_GRID_NAME = "newGrid";

    /** */
    private static final String STRUCTURE_NAME = "structure";

    /** */
    private static final String TRANSACTIONAL_CACHE_NAME = "tx_cache";

    /** */
    private static final int TOP_CHANGE_CNT = 5;

    /** */
    private static final int TOP_CHANGE_THREAD_CNT = 3;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @return Grids count to start.
     */
    @Override public int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCount());

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        atomicCfg.setCacheMode(collectionCacheMode());
        atomicCfg.setBackups(collectionConfiguration().getBackups());

        cfg.setAtomicConfiguration(atomicCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(TRANSACTIONAL_CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongTopologyChange() throws Exception {
        try (IgniteAtomicLong atomic = grid(0).atomicLong(STRUCTURE_NAME, 10, true)) {
            Ignite g = startGrid(NEW_GRID_NAME);

            assertEquals(10, g.atomicLong(STRUCTURE_NAME, 10, false).get());

            assertEquals(20, g.atomicLong(STRUCTURE_NAME, 10, false).addAndGet(10));

            stopGrid(NEW_GRID_NAME);

            assertEquals(20, grid(0).atomicLong(STRUCTURE_NAME, 10, true).get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongConstantTopologyChange() throws Exception {
        doTestAtomicLong(new ConstantTopologyChangeWorker());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongConstantMultipleTopologyChange() throws Exception {
        doTestAtomicLong(new ConstantMultipleTopologyChangeWorker());
    }

    /**
     * Tests IgniteAtomicLong.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestAtomicLong(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteAtomicLong s = grid(0).atomicLong(STRUCTURE_NAME, 1, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    assert ignite.atomicLong(STRUCTURE_NAME, 1, true).get() > 0;

                    return null;
                }
            });

            long val = s.get();

            while (!fut.isDone()) {
                assertEquals(val, s.get());

                assertEquals(++val, s.incrementAndGet());
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assertEquals(val, g.atomicLong(STRUCTURE_NAME, 1, false).get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReferenceTopologyChange() throws Exception {
        try (IgniteAtomicReference atomic = grid(0).atomicReference(STRUCTURE_NAME, 10, true)) {
            Ignite g = startGrid(NEW_GRID_NAME);

            assertEquals((Integer)10, g.atomicReference(STRUCTURE_NAME, 10, false).get());

            g.atomicReference(STRUCTURE_NAME, 10, false).set(20);

            stopGrid(NEW_GRID_NAME);

            assertEquals((Integer)20, grid(0).atomicReference(STRUCTURE_NAME, 10, true).get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReferenceConstantTopologyChange() throws Exception {
        doTestAtomicReference(new ConstantTopologyChangeWorker());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReferenceConstantMultipleTopologyChange() throws Exception {
        doTestAtomicReference(new ConstantMultipleTopologyChangeWorker());
    }

    /**
     * Tests atomic reference.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestAtomicReference(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteAtomicReference<Integer> s = grid(0).atomicReference(STRUCTURE_NAME, 1, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    assert ignite.atomicReference(STRUCTURE_NAME, 1, false).get() > 0;

                    return null;
                }
            });

            int val = s.get();

            while (!fut.isDone()) {
                assertEquals(val, (int)s.get());

                s.set(++val);
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assertEquals(val, (int)g.atomicReference(STRUCTURE_NAME, 1, true).get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicStampedTopologyChange() throws Exception {
        try (IgniteAtomicStamped atomic = grid(0).atomicStamped(STRUCTURE_NAME, 10, 10, true)) {
            Ignite g = startGrid(NEW_GRID_NAME);

            IgniteBiTuple<Integer, Integer> t = g.atomicStamped(STRUCTURE_NAME, 10, 10, false).get();

            assertEquals((Integer)10, t.get1());
            assertEquals((Integer)10, t.get2());

            g.atomicStamped(STRUCTURE_NAME, 10, 10, false).set(20, 20);

            stopGrid(NEW_GRID_NAME);

            t = grid(0).atomicStamped(STRUCTURE_NAME, 10, 10, false).get();

            assertEquals((Integer)20, t.get1());
            assertEquals((Integer)20, t.get2());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicStampedConstantTopologyChange() throws Exception {
        doTestAtomicStamped(new ConstantTopologyChangeWorker());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicStampedConstantMultipleTopologyChange() throws Exception {
        doTestAtomicStamped(new ConstantMultipleTopologyChangeWorker());
    }

    /**
     * Tests atomic stamped value.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestAtomicStamped(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteAtomicStamped<Integer, Integer> s = grid(0).atomicStamped(STRUCTURE_NAME, 1, 1, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    IgniteBiTuple<Integer, Integer> t = ignite.atomicStamped(STRUCTURE_NAME, 1, 1, false).get();

                    assert t.get1() > 0;
                    assert t.get2() > 0;

                    return null;
                }
            });

            int val = s.value();

            while (!fut.isDone()) {
                IgniteBiTuple<Integer, Integer> t = s.get();

                assertEquals(val, (int)t.get1());
                assertEquals(val, (int)t.get2());

                ++val;

                s.set(val, val);
            }

            fut.get();

            for (Ignite g : G.allGrids()) {
                IgniteBiTuple<Integer, Integer> t = g.atomicStamped(STRUCTURE_NAME, 1, 1, false).get();

                assertEquals(val, (int)t.get1());
                assertEquals(val, (int)t.get2());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountDownLatchTopologyChange() throws Exception {
        try (IgniteCountDownLatch latch = grid(0).countDownLatch(STRUCTURE_NAME, 20, true, true)) {
            try {
                Ignite g = startGrid(NEW_GRID_NAME);

                assertEquals(20, g.countDownLatch(STRUCTURE_NAME, 20, true, false).count());

                g.countDownLatch(STRUCTURE_NAME, 20, true, false).countDown(10);

                stopGrid(NEW_GRID_NAME);

                assertEquals(10, grid(0).countDownLatch(STRUCTURE_NAME, 20, true, false).count());
            }
            finally {
                grid(0).countDownLatch(STRUCTURE_NAME, 20, true, false).countDownAll();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountDownLatchConstantTopologyChange() throws Exception {
        doTestCountDownLatch(new ConstantTopologyChangeWorker());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountDownLatchConstantMultipleTopologyChange() throws Exception {
        doTestCountDownLatch(new ConstantMultipleTopologyChangeWorker());
    }

    /**
     * Tests distributed count down latch.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestCountDownLatch(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteCountDownLatch s = grid(0).countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, true)) {
            try {
                IgniteInternalFuture<?> fut = topWorker.startChangingTopology(
                    new IgniteClosure<Ignite, Object>() {
                        @Override public Object apply(Ignite ignite) {
                            assert ignite.countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, false).count() > 0;

                            return null;
                        }
                    });

                int val = s.count();

                while (!fut.isDone()) {
                    assertEquals(val, s.count());
                    assertEquals(--val, s.countDown());
                }

                fut.get();

                for (Ignite g : G.allGrids())
                    assertEquals(val, g.countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, true).count());
            }
            finally {
                grid(0).countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, false).countDownAll();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFifoQueueTopologyChange() throws Exception {
        try {
            grid(0).queue(STRUCTURE_NAME, 0, config(false)).put(10);

            Ignite g = startGrid(NEW_GRID_NAME);

            assertEquals(10, (int)g.<Integer>queue(STRUCTURE_NAME, 0, null).poll());

            g.queue(STRUCTURE_NAME, 0, null).put(20);

            stopGrid(NEW_GRID_NAME);

            assertEquals(20, (int)grid(0).<Integer>queue(STRUCTURE_NAME, 0, null).peek());
        }
        finally {
            grid(0).<Integer>queue(STRUCTURE_NAME, 0, null).close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueueConstantTopologyChange() throws Exception {
        doTestQueue(new ConstantTopologyChangeWorker());
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueueConstantMultipleTopologyChange() throws Exception {
        doTestQueue(new ConstantMultipleTopologyChangeWorker());
    }

    /**
     * Tests the queue.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestQueue(ConstantTopologyChangeWorker topWorker) throws Exception {
        int queueMaxSize = 100;

        try (IgniteQueue<Integer> s = grid(0).queue(STRUCTURE_NAME, 0, config(false))) {
            s.put(1);

            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    assert ignite.<Integer>queue(STRUCTURE_NAME, 0, null).peek() > 0;

                    return null;
                }
            });

            int val = s.peek();

            while (!fut.isDone()) {
                if (s.size() == queueMaxSize) {
                    int last = 0;

                    for (int i = 0, size = s.size() - 1; i < size; i++) {
                        int cur = s.poll();

                        if (i == 0) {
                            last = cur;

                            continue;
                        }

                        assertEquals(last, cur - 1);

                        last = cur;
                    }
                }

                s.put(++val);
            }

            fut.get();

            val = s.peek();

            for (Ignite g : G.allGrids())
                assertEquals(val, (int)g.<Integer>queue(STRUCTURE_NAME, 0, null).peek());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSequenceInitialization() throws Exception {
        int threadCnt = 3;

        final AtomicInteger idx = new AtomicInteger(gridCount());

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
            @Override public void apply() {
                int id = idx.getAndIncrement();

                try {
                    startGrid(id);

                    Thread.sleep(1000);

                }
                catch (Exception e) {
                    throw F.wrap(e);
                }
                finally {
                    stopGrid(id);

                    info("Thread finished.");
                }
            }
        }, threadCnt, "test-thread");

        while (!fut.isDone()) {
            grid(0).compute().call(new IgniteCallable<Object>() {
                /** */
                @IgniteInstanceResource
                private Ignite g;

                @Override public Object call() throws Exception {
                    IgniteAtomicSequence seq = g.atomicSequence(STRUCTURE_NAME, 1, true);

                    assert seq != null;

                    for (int i = 0; i < 1000; i++)
                        seq.getAndIncrement();

                    return null;
                }
            });
        }

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSequenceTopologyChange() throws Exception {
        try (IgniteAtomicSequence s = grid(0).atomicSequence(STRUCTURE_NAME, 10, true)) {
            Ignite g = startGrid(NEW_GRID_NAME);

            assertEquals(1010, g.atomicSequence(STRUCTURE_NAME, 10, false).get());

            assertEquals(1020, g.atomicSequence(STRUCTURE_NAME, 10, false).addAndGet(10));

            stopGrid(NEW_GRID_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSequenceConstantTopologyChange() throws Exception {
        doTestAtomicSequence(new ConstantTopologyChangeWorker());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSequenceConstantMultipleTopologyChange() throws Exception {
        doTestAtomicSequence(new ConstantMultipleTopologyChangeWorker());
    }

    /**
     * Tests atomic sequence.
     *
     * @param topWorker Topology change worker.
     * @throws Exception If failed.
     */
    private void doTestAtomicSequence(ConstantTopologyChangeWorker topWorker) throws Exception {
        try (IgniteAtomicSequence s = grid(0).atomicSequence(STRUCTURE_NAME, 1, true)) {
            IgniteInternalFuture<?> fut = topWorker.startChangingTopology(new IgniteClosure<Ignite, Object>() {
                @Override public Object apply(Ignite ignite) {
                    assertTrue(ignite.atomicSequence(STRUCTURE_NAME, 1, false).get() > 0);

                    return null;
                }
            });

            long old = s.get();

            while (!fut.isDone()) {
                assertEquals(old, s.get());

                long val = s.incrementAndGet();

                assertTrue(val > old);

                old = val;
            }

            fut.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUncommitedTxLeave() throws Exception {
        final int val = 10;

        grid(0).atomicLong(STRUCTURE_NAME, val, true);

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Ignite g = startGrid(NEW_GRID_NAME);

                try {
                    g.transactions().txStart();

                    g.cache(TRANSACTIONAL_CACHE_NAME).put(1, 1);

                    assertEquals(val + 1, g.atomicLong(STRUCTURE_NAME, val, false).incrementAndGet());
                }
                finally {
                    stopGrid(NEW_GRID_NAME);
                }

                return null;
            }
        }).get();

        waitForDiscovery(G.allGrids().toArray(new Ignite[gridCount()]));

        assertEquals(val + 1, grid(0).atomicLong(STRUCTURE_NAME, val, false).get());
    }

    /**
     *
     */
    private class ConstantTopologyChangeWorker {
        /** */
        protected final AtomicBoolean failed = new AtomicBoolean(false);

        /**
         * Starts changing cluster's topology.
         *
         * @return Future.
         */
        IgniteInternalFuture<?> startChangingTopology(final IgniteClosure<Ignite, ?> callback) {
            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            if (failed.get())
                                return;

                            String name = UUID.randomUUID().toString();

                            try {
                                Ignite g = startGrid(name);

                                callback.apply(g);
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true))
                            throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            return fut;
        }
    }

    /**
     *
     */
    private class ConstantMultipleTopologyChangeWorker extends ConstantTopologyChangeWorker {
        /**
         * Starts changing cluster's topology.
         *
         * @return Future.
         */
        @Override IgniteInternalFuture<?> startChangingTopology(final IgniteClosure<Ignite, ?> callback) {
            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            if (failed.get())
                                return;

                            Collection<String> names = new GridLeanSet<>(3);

                            try {
                                for (int j = 0; j < 3; j++) {
                                    if (failed.get())
                                        return;

                                    String name = UUID.randomUUID().toString();

                                    Ignite g = startGrid(name);

                                    names.add(name);

                                    callback.apply(g);
                                }
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1) {

                                    for (String name : names)
                                        stopGrid(name);
                                }
                            }
                        }
                    }
                    catch (Exception e) {
                        if (failed.compareAndSet(false, true))
                            throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            return fut;
        }
    }
}
