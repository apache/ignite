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

package org.apache.ignite.internal.processors.cache.datastructures.partitioned;

import java.util.Random;
import java.util.UUID;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteAtomicsAbstractTest;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicSequenceImpl;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Cache partitioned multi-threaded tests.
 */
public class GridCachePartitionedAtomicSequenceMultiThreadedTest extends IgniteAtomicsAbstractTest {
    /** Number of threads for multithreaded test. */
    private static final int THREAD_NUM = 30;

    /** Number of iterations per thread for multithreaded test. */
    private static final int ITERATION_NUM = 4000;

    /** {@inheritDoc} */
    @Override protected CacheMode atomicsCacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected AtomicConfiguration atomicConfiguration() {
        AtomicConfiguration cfg = super.atomicConfiguration();

        cfg.setBackups(1);
        cfg.setAtomicSequenceReserveSize(10);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testValues() throws Exception {
        String seqName = UUID.randomUUID().toString();

        final GridCacheAtomicSequenceImpl seq = (GridCacheAtomicSequenceImpl)grid(0).atomicSequence(seqName, 0, true);

        // Local reservations.
        assertEquals(1, seq.incrementAndGet());
        assertEquals(1, seq.getAndIncrement()); // Seq = 2
        assertEquals(3L, seq.incrementAndGet());
        assertEquals(3L, seq.getAndIncrement()); // Seq=4

        assertEquals(4, seq.getAndAdd(3));
        assertEquals(9, seq.addAndGet(2));

        assertEquals(new Long(9L), U.field(seq, "locVal"));
        assertEquals(new Long(9L), U.field(seq, "upBound"));

        // Cache calls.
        assertEquals(10, seq.incrementAndGet());

        assertEquals(new Long(10L), U.field(seq, "locVal"));
        assertEquals(new Long(19L), U.field(seq, "upBound"));

        seq.addAndGet(9);

        assertEquals(new Long(19L), U.field(seq, "locVal"));
        assertEquals(new Long(19L), U.field(seq, "upBound"));

        assertEquals(20L, seq.incrementAndGet());

        assertEquals(new Long(20L), U.field(seq, "locVal"));
        assertEquals(new Long(29L), U.field(seq, "upBound"));

        seq.addAndGet(9);

        assertEquals(new Long(29L), U.field(seq, "locVal"));
        assertEquals(new Long(29L), U.field(seq, "upBound"));

        assertEquals(29, seq.getAndIncrement());

        assertEquals(new Long(30L), U.field(seq, "locVal"));
        assertEquals(new Long(39L), U.field(seq, "upBound"));

        seq.addAndGet(9);

        assertEquals(new Long(39L), U.field(seq, "locVal"));
        assertEquals(new Long(39L), U.field(seq, "upBound"));

        assertEquals(39L, seq.getAndIncrement());

        assertEquals(new Long(40L), U.field(seq, "locVal"));
        assertEquals(new Long(49L), U.field(seq, "upBound"));

        seq.addAndGet(9);

        assertEquals(new Long(49L), U.field(seq, "locVal"));
        assertEquals(new Long(49L), U.field(seq, "upBound"));

        assertEquals(50, seq.addAndGet(1));

        assertEquals(new Long(50L), U.field(seq, "locVal"));
        assertEquals(new Long(59L), U.field(seq, "upBound"));

        seq.addAndGet(9);

        assertEquals(new Long(59L), U.field(seq, "locVal"));
        assertEquals(new Long(59L), U.field(seq, "upBound"));

        assertEquals(59, seq.getAndAdd(1));

        assertEquals(new Long(60L), U.field(seq, "locVal"));
        assertEquals(new Long(69L), U.field(seq, "upBound"));
    }

    /** @throws Exception If failed. */
    public void testUpdatedSync() throws Exception {
        checkUpdate(true);
    }

    /** @throws Exception If failed. */
    public void testPreviousSync() throws Exception {
        checkUpdate(false);
    }

    /** @throws Exception If failed. */
    public void testIncrementAndGet() throws Exception {
        // Random sequence names.
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        runSequenceClosure(new GridInUnsafeClosure<IgniteAtomicSequence>() {
            @Override public void apply(IgniteAtomicSequence t) {
                t.incrementAndGet();
            }
        }, seq, ITERATION_NUM, THREAD_NUM);

        assertEquals(ITERATION_NUM * THREAD_NUM, seq.get());
    }

    /** @throws Exception If failed. */
    public void testIncrementAndGetAsync() throws Exception {
        // Random sequence names.
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        runSequenceClosure(new GridInUnsafeClosure<IgniteAtomicSequence>() {
            @Override public void apply(IgniteAtomicSequence t) {
                t.incrementAndGet();
            }
        }, seq, ITERATION_NUM, THREAD_NUM);

        assertEquals(ITERATION_NUM * THREAD_NUM, seq.get());
    }

    /** @throws Exception If failed. */
    public void testGetAndIncrement() throws Exception {
        // Random sequence names.
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        runSequenceClosure(new GridInUnsafeClosure<IgniteAtomicSequence>() {
            @Override public void apply(IgniteAtomicSequence t) {
                t.getAndIncrement();
            }
        }, seq, ITERATION_NUM, THREAD_NUM);

        assertEquals(ITERATION_NUM * THREAD_NUM, seq.get());
    }

    /** @throws Exception If failed. */
    public void testGetAndIncrementAsync() throws Exception {
        // Random sequence names.
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        runSequenceClosure(new GridInUnsafeClosure<IgniteAtomicSequence>() {
            @Override public void apply(IgniteAtomicSequence t) {
                t.getAndIncrement();
            }
        }, seq, ITERATION_NUM, THREAD_NUM);

        assertEquals(ITERATION_NUM * THREAD_NUM, seq.get());
    }

    /** @throws Exception If failed. */
    public void testAddAndGet() throws Exception {
        // Random sequence names.
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        runSequenceClosure(new GridInUnsafeClosure<IgniteAtomicSequence>() {
            @Override public void apply(IgniteAtomicSequence t) {
                t.addAndGet(5);
            }
        }, seq, ITERATION_NUM, THREAD_NUM);

        assertEquals(5 * ITERATION_NUM * THREAD_NUM, seq.get());
    }

    /** @throws Exception If failed. */
    public void testGetAndAdd() throws Exception {
        // Random sequence names.
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        runSequenceClosure(new GridInUnsafeClosure<IgniteAtomicSequence>() {
            @Override public void apply(IgniteAtomicSequence t) {
                t.getAndAdd(5);
            }
        }, seq, ITERATION_NUM, THREAD_NUM);

        assertEquals(5 * ITERATION_NUM * THREAD_NUM, seq.get());
    }

    /** @throws Exception If failed. */
    public void testMixed1() throws Exception {
        // Random sequence names.
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        runSequenceClosure(new GridInUnsafeClosure<IgniteAtomicSequence>() {
            @Override public void apply(IgniteAtomicSequence t) {
                t.incrementAndGet();
                t.getAndIncrement();
                t.incrementAndGet();
                t.getAndIncrement();
                t.getAndAdd(3);
                t.addAndGet(3);
            }
        }, seq, ITERATION_NUM, THREAD_NUM);

        assertEquals(10 * ITERATION_NUM * THREAD_NUM, seq.get());
    }

    /** @throws Exception If failed. */
    public void testMixed2() throws Exception {
        // Random sequence names.
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        runSequenceClosure(new GridInUnsafeClosure<IgniteAtomicSequence>() {
            @Override public void apply(IgniteAtomicSequence t) {
                t.getAndAdd(2);
                t.addAndGet(3);
                t.addAndGet(5);
                t.getAndAdd(7);
            }
        }, seq, ITERATION_NUM, THREAD_NUM);

        assertEquals(17 * ITERATION_NUM * THREAD_NUM, seq.get());
    }

    /**
     * Executes given closure in a given number of threads given number of times.
     *
     * @param c Closure to execute.
     * @param seq Sequence to pass into closure.
     * @param cnt Count of iterations per thread.
     * @param threadCnt Thread count.
     * @throws Exception If failed.
     */
    protected void runSequenceClosure(final GridInUnsafeClosure<IgniteAtomicSequence> c,
        final IgniteAtomicSequence seq, final int cnt, final int threadCnt) throws Exception {
        multithreaded(new Runnable() {
            @Override public void run() {
                try {
                    for (int i = 0; i < cnt; i++)
                        c.apply(seq);
                }
                catch (IgniteCheckedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, threadCnt);
    }

    /**
     * @param updated Whether use updated values.
     * @throws Exception If failed.
     */
    @SuppressWarnings("IfMayBeConditional")
    private void checkUpdate(boolean updated) throws Exception {
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        long curVal = 0;

        Random r = new Random();

        for (int i = 0; i < ITERATION_NUM; i++) {
            long delta = r.nextInt(10) + 1;

            long retVal = updated ? seq.addAndGet(delta) : seq.getAndAdd(delta);

            assertEquals(updated ? curVal + delta : curVal, retVal);

            curVal += delta;
        }
    }

    /**
     * Closure that throws exception.
     *
     * @param <E> Closure argument type.
     */
    private abstract static class GridInUnsafeClosure<E> {
        public abstract void apply(E p) throws IgniteCheckedException;
    }
}