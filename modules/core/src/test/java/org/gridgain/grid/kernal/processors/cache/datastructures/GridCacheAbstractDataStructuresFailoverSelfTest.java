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

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Failover tests for cache data structures.
 */
public abstract class GridCacheAbstractDataStructuresFailoverSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final long TEST_TIMEOUT = 2 * 60 * 1000;

    /** */
    private static final String NEW_GRID_NAME = "newGrid";

    /** */
    private static final String STRUCTURE_NAME = "structure";

    /** */
    private static final int TOP_CHANGE_CNT = 5;

    /** */
    private static final int TOP_CHANGE_THREAD_CNT = 3;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setPreloadMode(SYNC);

        return cfg;
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

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongTopologyChange() throws Exception {
        try {
            cache().dataStructures().atomicLong(STRUCTURE_NAME, 10, true);

            Ignite g = startGrid(NEW_GRID_NAME);

            assert g.cache(null).dataStructures().atomicLong(STRUCTURE_NAME, 10, true).get() == 10;

            assert g.cache(null).dataStructures().atomicLong(STRUCTURE_NAME, 10, true).addAndGet(10) == 20;

            stopGrid(NEW_GRID_NAME);

            assert cache().dataStructures().atomicLong(STRUCTURE_NAME, 10, true).get() == 20;
        }
        finally {
            cache().dataStructures().removeAtomicLong(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongConstantTopologyChange() throws Exception {
        try {
            GridCacheAtomicLong s = cache().dataStructures().atomicLong(STRUCTURE_NAME, 1, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            String name = UUID.randomUUID().toString();

                            try {
                                Ignite g = startGrid(name);

                                assert g.cache(null).dataStructures().atomicLong(STRUCTURE_NAME, 1, true).get() > 0;
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            long val = s.get();

            while (!fut.isDone()) {
                assert s.get() == val;

                assert s.incrementAndGet() == val + 1;

                val++;
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assert g.cache(null).dataStructures().atomicLong(STRUCTURE_NAME, 1, true).get() == val;
        }
        finally {
            cache().dataStructures().removeAtomicLong(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongConstantMultipleTopologyChange() throws Exception {
        try {
            GridCacheAtomicLong s = cache().dataStructures().atomicLong(STRUCTURE_NAME, 1, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            Collection<String> names = new GridLeanSet<>(3);

                            try {
                                for (int j = 0; j < 3; j++) {
                                    String name = UUID.randomUUID().toString();

                                    names.add(name);

                                    Ignite g = startGrid(name);

                                    assert g.cache(null).dataStructures().atomicLong(STRUCTURE_NAME, 1, true).get() > 0;
                                }
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    for (String name : names)
                                        stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            long val = s.get();

            while (!fut.isDone()) {
                assert s.get() == val;

                assert s.incrementAndGet() == val + 1;

                val++;
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assert g.cache(null).dataStructures().atomicLong(STRUCTURE_NAME, 1, true).get() == val;
        }
        finally {
            cache().dataStructures().removeAtomicLong(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReferenceTopologyChange() throws Exception {
        try {
            cache().dataStructures().atomicReference(STRUCTURE_NAME, 10, true);

            Ignite g = startGrid(NEW_GRID_NAME);

            assert g.cache(null).dataStructures().<Integer>atomicReference(STRUCTURE_NAME, 10, true).get() == 10;

            g.cache(null).dataStructures().<Integer>atomicReference(STRUCTURE_NAME, 10, true).set(20);

            stopGrid(NEW_GRID_NAME);

            assert cache().dataStructures().atomicReference(STRUCTURE_NAME, 10, true).get().equals(20);
        }
        finally {
            cache().dataStructures().removeAtomicReference(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReferenceConstantTopologyChange() throws Exception {
        try {
            GridCacheAtomicReference<Integer> s = cache().dataStructures().atomicReference(STRUCTURE_NAME, 1, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            String name = UUID.randomUUID().toString();

                            try {
                                Ignite g = startGrid(name);

                                assert g.cache(null).dataStructures().<Integer>atomicReference(STRUCTURE_NAME, 1, true)
                                    .get() > 0;
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            int val = s.get();

            while (!fut.isDone()) {
                assert s.get() == val;

                s.set(++val);
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assert g.cache(null).dataStructures().<Integer>atomicReference(STRUCTURE_NAME, 1, true).get() == val;
        }
        finally {
            cache().dataStructures().removeAtomicReference(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReferenceConstantMultipleTopologyChange() throws Exception {
        try {
            GridCacheAtomicReference<Integer> s = cache().dataStructures().atomicReference(STRUCTURE_NAME, 1, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            Collection<String> names = new GridLeanSet<>(3);

                            try {
                                for (int j = 0; j < 3; j++) {
                                    String name = UUID.randomUUID().toString();

                                    names.add(name);

                                    Ignite g = startGrid(name);

                                    assert
                                        g.cache(null).dataStructures().<Integer>atomicReference(STRUCTURE_NAME, 1, true)
                                            .get() > 0;
                                }
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    for (String name : names)
                                        stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            int val = s.get();

            while (!fut.isDone()) {
                assert s.get() == val;

                s.set(++val);
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assert g.cache(null).dataStructures().<Integer>atomicReference(STRUCTURE_NAME, 1, true).get() == val;
        }
        finally {
            cache().dataStructures().removeAtomicReference(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicStampedTopologyChange() throws Exception {
        try {
            cache().dataStructures().atomicStamped(STRUCTURE_NAME, 10, 10, true);

            Ignite g = startGrid(NEW_GRID_NAME);

            IgniteBiTuple<Integer, Integer> t = g.cache(null).dataStructures()
                .<Integer, Integer>atomicStamped(STRUCTURE_NAME, 10, 10, true).get();

            assert t.get1() == 10;
            assert t.get2() == 10;

            g.cache(null).dataStructures().<Integer, Integer>atomicStamped(STRUCTURE_NAME, 10, 10, true).set(20, 20);

            stopGrid(NEW_GRID_NAME);

            t = cache().dataStructures().<Integer, Integer>atomicStamped(STRUCTURE_NAME, 10, 10, true).get();

            assert t.get1() == 20;
            assert t.get2() == 20;
        }
        finally {
            cache().dataStructures().removeAtomicStamped(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicStampedConstantTopologyChange() throws Exception {
        try {
            GridCacheAtomicStamped<Integer, Integer> s = cache().dataStructures().atomicStamped(STRUCTURE_NAME, 1, 1, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            String name = UUID.randomUUID().toString();

                            try {
                                Ignite g = startGrid(name);

                                IgniteBiTuple<Integer, Integer> t =
                                    g.cache(null).dataStructures()
                                        .<Integer, Integer>atomicStamped(STRUCTURE_NAME, 1, 1, true).get();

                                assert t.get1() > 0;
                                assert t.get2() > 0;
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            int val = s.value();

            while (!fut.isDone()) {
                IgniteBiTuple<Integer, Integer> t = s.get();

                assert t.get1() == val;
                assert t.get2() == val;

                val++;

                s.set(val, val);
            }

            fut.get();

            for (Ignite g : G.allGrids()) {
                IgniteBiTuple<Integer, Integer> t = g.cache(null).dataStructures()
                    .<Integer, Integer>atomicStamped(STRUCTURE_NAME, 1, 1, true).get();

                assert t.get1() == val;
                assert t.get2() == val;
            }
        }
        finally {
            cache().dataStructures().removeAtomicStamped(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicStampedConstantMultipleTopologyChange() throws Exception {
        try {
            GridCacheAtomicStamped<Integer, Integer> s = cache().dataStructures().atomicStamped(STRUCTURE_NAME, 1, 1, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            Collection<String> names = new GridLeanSet<>(3);

                            try {
                                for (int j = 0; j < 3; j++) {
                                    String name = UUID.randomUUID().toString();

                                    names.add(name);

                                    Ignite g = startGrid(name);

                                    IgniteBiTuple<Integer, Integer> t =
                                        g.cache(null).dataStructures()
                                            .<Integer, Integer>atomicStamped(STRUCTURE_NAME, 1, 1, true).get();

                                    assert t.get1() > 0;
                                    assert t.get2() > 0;
                                }
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    for (String name : names)
                                        stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            int val = s.value();

            while (!fut.isDone()) {
                IgniteBiTuple<Integer, Integer> t = s.get();

                assert t.get1() == val;
                assert t.get2() == val;

                val++;

                s.set(val, val);
            }

            fut.get();

            for (Ignite g : G.allGrids()) {
                IgniteBiTuple<Integer, Integer> t = g.cache(null).dataStructures()
                    .<Integer, Integer>atomicStamped(STRUCTURE_NAME, 1, 1, true).get();

                assert t.get1() == val;
                assert t.get2() == val;
            }
        }
        finally {
            cache().dataStructures().removeAtomicStamped(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountDownLatchTopologyChange() throws Exception {
        try {
            cache().dataStructures().countDownLatch(STRUCTURE_NAME, 20, true, true);

            Ignite g = startGrid(NEW_GRID_NAME);

            assert g.cache(null).dataStructures().countDownLatch(STRUCTURE_NAME, 20, true, true).count() == 20;

            g.cache(null).dataStructures().countDownLatch(STRUCTURE_NAME, 20, true, true).countDown(10);

            stopGrid(NEW_GRID_NAME);

            assert cache().dataStructures().countDownLatch(STRUCTURE_NAME, 20, true, true).count() == 10;
        }
        finally {
            cache().dataStructures().countDownLatch(STRUCTURE_NAME, 20, true, true).countDownAll();

            cache().dataStructures().removeCountDownLatch(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountDownLatchConstantTopologyChange() throws Exception {
        try {
            GridCacheCountDownLatch s = cache().dataStructures().countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE,
                false, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            String name = UUID.randomUUID().toString();

                            try {
                                Ignite g = startGrid(name);

                                assert g.cache(null).dataStructures().countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE,
                                    false, false) != null;
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            int val = s.count();

            while (!fut.isDone()) {
                assert s.count() == val;

                assert s.countDown() == val - 1;

                val--;
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assert g.cache(null).dataStructures().countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, true)
                    .count() == val;
        }
        finally {
            cache().dataStructures().countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, true).countDownAll();

            cache().dataStructures().removeCountDownLatch(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountDownLatchConstantMultipleTopologyChange() throws Exception {
        try {
            GridCacheCountDownLatch s = cache().dataStructures()
                .countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            Collection<String> names = new GridLeanSet<>(3);

                            try {
                                for (int j = 0; j < 3; j++) {
                                    String name = UUID.randomUUID().toString();

                                    names.add(name);

                                    Ignite g = startGrid(name);

                                    assert g.cache(null).dataStructures()
                                        .countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, false) != null;
                                }
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    for (String name : names)
                                        stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            int val = s.count();

            while (!fut.isDone()) {
                assert s.count() == val;

                assert s.countDown() == val - 1;

                val--;
            }

            fut.get();

            for (Ignite g : G.allGrids())
                assert g.cache(null).dataStructures()
                    .countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, false).count() == val;
        }
        finally {
            cache().dataStructures().countDownLatch(STRUCTURE_NAME, Integer.MAX_VALUE, false, false).countDownAll();

            cache().dataStructures().removeCountDownLatch(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFifoQueueTopologyChange() throws Exception {
        try {
            cache().dataStructures().queue(STRUCTURE_NAME, 0, false, true).put(10);

            Ignite g = startGrid(NEW_GRID_NAME);

            assert g.cache(null).dataStructures().<Integer>queue(STRUCTURE_NAME, 0, false, false).poll() == 10;

            g.cache(null).dataStructures().queue(STRUCTURE_NAME, 0, false, false).put(20);

            stopGrid(NEW_GRID_NAME);

            assert cache().dataStructures().<Integer>queue(STRUCTURE_NAME, 0, false, false).peek() == 20;
        }
        finally {
            cache().dataStructures().removeQueue(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFifoQueueConstantTopologyChange() throws Exception {
        try {
            GridCacheQueue<Integer> s = cache().dataStructures().queue(STRUCTURE_NAME, 0, false, true);

            s.put(1);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            String name = UUID.randomUUID().toString();

                            try {
                                Ignite g = startGrid(name);

                                assert g.cache(null).dataStructures().<Integer>queue(STRUCTURE_NAME, 0, false,
                                    false).peek() > 0;
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            int val = s.peek();

            int origVal = val;

            while (!fut.isDone())
                s.put(++val);

            fut.get();

            for (Ignite g : G.allGrids())
                assert g.cache(null).dataStructures().<Integer>queue(STRUCTURE_NAME, 0, false, false).peek() == origVal;
        }
        finally {
            cache().dataStructures().removeQueue(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFifoQueueConstantMultipleTopologyChange() throws Exception {
        try {
            GridCacheQueue<Integer> s = cache().dataStructures().queue(STRUCTURE_NAME, 0, false, true);

            s.put(1);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            Collection<String> names = new GridLeanSet<>(3);

                            try {
                                for (int j = 0; j < 3; j++) {
                                    String name = UUID.randomUUID().toString();

                                    names.add(name);

                                    Ignite g = startGrid(name);

                                    assert g.cache(null).dataStructures()
                                        .<Integer>queue(STRUCTURE_NAME, 0, false, false).peek() > 0;
                                }
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    for (String name : names)
                                        stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            int val = s.peek();

            int origVal = val;

            while (!fut.isDone())
                s.put(++val);

            fut.get();

            for (Ignite g : G.allGrids())
                assert g.cache(null).dataStructures().<Integer>queue(STRUCTURE_NAME, 0, false, false).peek() == origVal;
        }
        finally {
            cache().dataStructures().removeQueue(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSequenceTopologyChange() throws Exception {
        try {
            cache().dataStructures().atomicSequence(STRUCTURE_NAME, 10, true);

            Ignite g = startGrid(NEW_GRID_NAME);

            assert g.cache(null).dataStructures().atomicSequence(STRUCTURE_NAME, 10, false).get() == 1010;

            assert g.cache(null).dataStructures().atomicSequence(STRUCTURE_NAME, 10, false).addAndGet(10) == 1020;

            stopGrid(NEW_GRID_NAME);
        }
        finally {
            cache().dataStructures().removeAtomicSequence(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSequenceConstantTopologyChange() throws Exception {
        try {
            GridCacheAtomicSequence s = cache().dataStructures().atomicSequence(STRUCTURE_NAME, 1, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        String name = UUID.randomUUID().toString();

                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            try {
                                Ignite g = startGrid(name);

                                assertTrue(g.cache(null).dataStructures().atomicSequence(STRUCTURE_NAME, 1, false).
                                    get() > 0);
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            long old = s.get();

            while (!fut.isDone()) {
                assertEquals(old, s.get());

                long val = s.incrementAndGet();

                assertTrue(val > old);

                old = val;
            }

            fut.get();
        }
        finally {
            cache().dataStructures().removeAtomicSequence(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicSequenceInitialization() throws Exception {
        int threadCnt = 3;

        final AtomicInteger idx = new AtomicInteger(gridCount());

        IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
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
            grid(0).compute().call(new Callable<Object>() {
                    /** */
                    @IgniteInstanceResource
                    private Ignite g;

                    @Override public Object call() throws Exception {
                        GridCacheAtomicSequence seq = g.cache(null).dataStructures().atomicSequence(STRUCTURE_NAME, 1,
                            true);

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
    public void testAtomicSequenceConstantMultipleTopologyChange() throws Exception {
        try {
            GridCacheAtomicSequence s = cache().dataStructures().atomicSequence(STRUCTURE_NAME, 1, true);

            IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
                @Override public void apply() {
                    try {
                        for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                            Collection<String> names = new GridLeanSet<>(3);

                            try {
                                for (int j = 0; j < 3; j++) {
                                    String name = UUID.randomUUID().toString();

                                    names.add(name);

                                    Ignite g = startGrid(name);

                                    assertTrue(g.cache(null).dataStructures().atomicSequence(STRUCTURE_NAME, 1, false)
                                        .get() > 0);
                                }
                            }
                            finally {
                                if (i != TOP_CHANGE_CNT - 1)
                                    for (String name : names)
                                        stopGrid(name);
                            }
                        }
                    }
                    catch (Exception e) {
                        throw F.wrap(e);
                    }
                }
            }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

            long old = s.get();

            while (!fut.isDone()) {
                assertEquals(old, s.get());

                long val = s.incrementAndGet();

                assertTrue(val > old);

                old = val;
            }

            fut.get();
        }
        finally {
            cache().dataStructures().removeAtomicSequence(STRUCTURE_NAME);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUncommitedTxLeave() throws Exception {
        final int val = 10;

        cache().dataStructures().atomicLong(STRUCTURE_NAME, val, true);

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Ignite g = startGrid(NEW_GRID_NAME);

                try {
                    g.cache(null).txStart();

                    assert g.cache(null).dataStructures().atomicLong(STRUCTURE_NAME, val, false).
                        incrementAndGet() == val + 1;
                }
                finally {
                    stopGrid(NEW_GRID_NAME);
                }

                return null;
            }
        }).get();

        waitForDiscovery(G.allGrids().toArray(new Ignite[gridCount()]));

        assert cache().dataStructures().atomicLong(STRUCTURE_NAME, val, false).get() == val + 1;
    }
}
