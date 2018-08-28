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

import java.util.Random;
import java.util.UUID;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicSequenceImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Cache partitioned multi-threaded tests.
 */
public abstract class GridCacheAtomicSequenceMultiThreadedAbstractTest extends IgniteAtomicsAbstractTest {
    /** Number of threads for multithreaded test. */
    private static final int THREAD_NUM = 30;

    /** Number of iterations per thread for multithreaded test. */
    private static final int ITERATION_NUM = 10000;

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

    /**
     * @throws Exception If failed.
     */
    public void testValues() throws Exception {
        String seqName = UUID.randomUUID().toString();

        IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0, true);

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

    /**
     * @throws Exception If failed.
     */
    public void testValues2() throws Exception {
        AtomicConfiguration acfg = atomicConfiguration();

        acfg.setAtomicSequenceReservePercentage(80);

        String seqName = UUID.randomUUID().toString();

        IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, acfg, 10, true);

        assertSeqFields(seq, /*locVal*/ 10, /*upBound*/ 19, /*resBound*/ 17, /*resBottomBound*/ 10, /*resUpBound*/ 19);

        assertEquals(16, seq.addAndGet(6));

        assertSeqFields(seq, /*locVal*/ 16, /*upBound*/ 19, /*resBound*/ 17, /*resBottomBound*/ 10, /*resUpBound*/ 19);

        assertEquals(17, seq.incrementAndGet());

        assertEquals(18, seq.incrementAndGet());

        assertSeqFields(seq, /*locVal*/ 18, /*upBound*/ 19, /*resBound*/ 17, /*resBottomBound*/ 10, /*resUpBound*/ 19);

        assertEquals(19, seq.incrementAndGet());

        assertSeqFields(seq, /*locVal*/ 19, /*upBound*/ 19, /*resBound*/ 17, /*resBottomBound*/ 10, /*resUpBound*/ 19);

        assertEquals(20, seq.incrementAndGet());

        assertSeqFields(seq, /*locVal*/ 20, /*upBound*/ 29, /*resBound*/ 27, /*resBottomBound*/ 20, /*resUpBound*/ 29);
    }

    /** @throws Exception If failed. */
    public void testValuesPercentage50() throws Exception {
        AtomicConfiguration acfg = atomicConfiguration();

        acfg.setAtomicSequenceReservePercentage(50);

        String seqName = UUID.randomUUID().toString();

        IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, acfg, 0, true);

        assertSeqFields(seq, /*locVal*/ 0, /*upBound*/ 9, /*resBound*/ 4, /*resBottomBound*/ 0, /*resUpBound*/ 9);

        // Exhaust a first reserved range to get recalculated values according to new reserve percentage.
        assertEquals(10, seq.addAndGet(10));

        assertSeqFields(seq, /*locVal*/ 10, /*upBound*/ 19, /*resBound*/ 14, /*resBottomBound*/ 10, /*resUpBound*/ 19);

        assertEquals(15, seq.addAndGet(5));

        assertSeqFields(seq, /*locVal*/ 15, /*upBound*/ 20, /*resBound*/ 25, /*resBottomBound*/ 20, /*resUpBound*/ 30);
    }

    /** @throws Exception If failed. */
    public void testValuesPercentage0() throws Exception {
        String seqName = UUID.randomUUID().toString();

        final GridCacheAtomicSequenceImpl seq = (GridCacheAtomicSequenceImpl)grid(0).atomicSequence(seqName, 0, true);

        seq.reservePercentage(0);

        assertSeqFields(seq, /*locVal*/ 0, /*upBound*/ 10, /*resBound*/ 8, /*resBottomBound*/ 0, /*resUpBound*/ 0);

        // Exhaust a first reserved range to get recalculated values according to new reserve percentage.
        assertEquals(10, seq.addAndGet(10));

        assertSeqFields(seq, /*locVal*/ 10, /*upBound*/ 20, /*resBound*/ 10, /*resBottomBound*/ 10, /*resUpBound*/ 20);

        assertEquals(11, seq.addAndGet(1));

        waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !F.eq(U.field(seq, "isReserveFutResultsProcessed"), true);
            }
        }, 1000);

        assertSeqFields(seq, /*locVal*/ 11, /*upBound*/ 20, /*resBound*/ 20, /*resBottomBound*/ 20, /*resUpBound*/ 30);

        assertEquals(12, seq.incrementAndGet());

        assertSeqFields(seq, /*locVal*/ 12, /*upBound*/ 20, /*resBound*/ 20, /*resBottomBound*/ 20, /*resUpBound*/ 30);

        assertEquals(20, seq.addAndGet(8));

        assertSeqFields(seq, /*locVal*/ 20, /*upBound*/ 30, /*resBound*/ 20, /*resBottomBound*/ 20, /*resUpBound*/ 30);
    }

    /** @throws Exception If failed. */
    public void testValuesPercentage100() throws Exception {
        String seqName = UUID.randomUUID().toString();

        final GridCacheAtomicSequenceImpl seq = (GridCacheAtomicSequenceImpl)grid(0).atomicSequence(seqName, 0, true);

        seq.reservePercentage(100);

        assertSeqFields(seq, /*locVal*/ 0, /*upBound*/ 10, /*resBound*/ 8, /*resBottomBound*/ 0, /*resUpBound*/ 0);

        // Exhaust a first reserved range to get recalculated values according to new reserve percentage.
        assertEquals(10, seq.addAndGet(10));

        assertSeqFields(seq, /*locVal*/ 10, /*upBound*/ 20, /*resBound*/ 20, /*resBottomBound*/ 10, /*resUpBound*/ 20);

        assertEquals(19, seq.addAndGet(9));

        assertSeqFields(seq, /*locVal*/ 19, /*upBound*/ 20, /*resBound*/ 20, /*resBottomBound*/ 10, /*resUpBound*/ 20);

        assertEquals(20, seq.incrementAndGet());

        assertSeqFields(seq, /*locVal*/ 20, /*upBound*/ 30, /*resBound*/ 30, /*resBottomBound*/ 20, /*resUpBound*/ 30);
    }

    /** @throws Exception If failed. */
    public void testValuesDoubleReservation() throws Exception {
        String seqName = UUID.randomUUID().toString();

        final GridCacheAtomicSequenceImpl seq = (GridCacheAtomicSequenceImpl)grid(0).atomicSequence(seqName, 0, true);

        assertSeqFields(seq, /*locVal*/ 0, /*upBound*/ 10, /*resBound*/ 8, /*resBottomBound*/ 0, /*resUpBound*/ 0);

        assertEquals(30, seq.addAndGet(30));
    }

    /** @throws Exception If failed. */
    public void testValues2Nodes() throws Exception {
        String seqName = UUID.randomUUID().toString();

        startGrid(1);

        try {
            final GridCacheAtomicSequenceImpl seq1 = (GridCacheAtomicSequenceImpl)grid(0).atomicSequence(seqName, 0, true);
            final GridCacheAtomicSequenceImpl seq2 = (GridCacheAtomicSequenceImpl)grid(1).atomicSequence(seqName, 0, false);

            assertSeqFields(seq1, /*locVal*/ 0, /*upBound*/ 10, /*resBound*/ 8, /*resBottomBound*/ 0, /*resUpBound*/ 0);
            assertSeqFields(seq2, /*locVal*/ 10, /*upBound*/ 20, /*resBound*/ 18, /*resBottomBound*/ 0, /*resUpBound*/ 0);

            assertEquals(1, seq1.incrementAndGet());
            assertEquals(11, seq2.incrementAndGet());

            assertSeqFields(seq1, /*locVal*/ 1, /*upBound*/ 10, /*resBound*/ 8, /*resBottomBound*/ 0, /*resUpBound*/ 0);
            assertSeqFields(seq2, /*locVal*/ 11, /*upBound*/ 20, /*resBound*/ 18, /*resBottomBound*/ 0, /*resUpBound*/ 0);

            assertEquals(7, seq1.addAndGet(6));
            assertEquals(17, seq2.addAndGet(6));

            assertSeqFields(seq1, /*locVal*/ 7, /*upBound*/ 10, /*resBound*/ 8, /*resBottomBound*/ 0, /*resUpBound*/ 0);
            assertSeqFields(seq2, /*locVal*/ 17, /*upBound*/ 20, /*resBound*/ 18, /*resBottomBound*/ 0, /*resUpBound*/ 0);

            // New reservation (reverse order)
            assertEquals(18, seq2.incrementAndGet());

            assertTrue(waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return !F.eq(U.field(seq2, "isReserveFutResultsProcessed"), true);
                }
            }, 1000));

            assertEquals(8, seq1.incrementAndGet());

            assertTrue(waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return !F.eq(U.field(seq1, "isReserveFutResultsProcessed"), true);
                }
            }, 1000));

            assertSeqFields(seq1, /*locVal*/ 8, /*upBound*/ 10, /*resBound*/ 38, /*resBottomBound*/ 30, /*resUpBound*/ 40);
            assertSeqFields(seq2, /*locVal*/ 18, /*upBound*/ 20, /*resBound*/ 28, /*resBottomBound*/ 20, /*resUpBound*/ 30);

            assertEquals(30, seq1.addAndGet(7));
            assertEquals(20, seq2.addAndGet(2));

            assertSeqFields(seq1, /*locVal*/ 30, /*upBound*/ 40, /*resBound*/ 38, /*resBottomBound*/ 30, /*resUpBound*/ 40);
            assertSeqFields(seq2, /*locVal*/ 20, /*upBound*/ 30, /*resBound*/ 28, /*resBottomBound*/ 20, /*resUpBound*/ 30);
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * @param seq Sequence.
     * @param locVal Local value.
     * @param upBound Up bound.
     * @param newReservationLine Reservation bnound.
     * @param reservedBottomBound Reservation bottom bound.
     * @param reservedUpBound Reservation up bound.
     */
    private void assertSeqFields(
        IgniteAtomicSequence seq,
        long locVal,
        long upBound,
        long newReservationLine,
        long reservedBottomBound,
        long reservedUpBound
    ) {
        assertEquals(new Long(locVal), U.field(seq, "locVal"));
        assertEquals(new Long(upBound), U.field(seq, "upBound"));

        Object bounds = U.field(U.<Object>field(seq, "reservationCtx"), "bounds");

        assertEquals(new Long(newReservationLine), U.field(bounds, "newReservationLine"));
        assertEquals(new Long(reservedBottomBound), U.field(bounds, "reservedBottomBound"));
        assertEquals(new Long(reservedUpBound), U.field(bounds, "reservedUpBound"));
    }

    /** @throws Exception If failed. */
    public void testValues2NodesDoubleReservation() throws Exception {
        String seqName = UUID.randomUUID().toString();

        startGrid(1);

        try {
            final GridCacheAtomicSequenceImpl seq1 = (GridCacheAtomicSequenceImpl)grid(0).atomicSequence(seqName, 0, true);
            final GridCacheAtomicSequenceImpl seq2 = (GridCacheAtomicSequenceImpl)grid(1).atomicSequence(seqName, 0, false);

            assertEquals(1, seq1.incrementAndGet());
            assertEquals(11, seq2.incrementAndGet());

            assertEquals(new Long(1L), U.field(seq1, "locVal"));
            assertEquals(new Long(10L), U.field(seq1, "upBound"));
            assertEquals(new Long(11L), U.field(seq2, "locVal"));
            assertEquals(new Long(20L), U.field(seq2, "upBound"));

            assertEquals(31, seq2.addAndGet(20));

            assertEquals(new Long(1L), U.field(seq1, "locVal"));
            assertEquals(new Long(10L), U.field(seq1, "upBound"));
            assertEquals(new Long(31L), U.field(seq2, "locVal"));
            assertEquals(new Long(40L), U.field(seq2, "upBound"));

            // Jump
            assertEquals(40, seq1.addAndGet(23));
        }
        finally {
            stopGrid(1);
        }
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
    public void testIncrementAndGet2Nodes() throws Exception {
        startGrid(1);

        try {
            String seqName = UUID.randomUUID().toString();

            final IgniteAtomicSequence seq1 = grid(0).atomicSequence(seqName, 0L, true);
            final IgniteAtomicSequence seq2 = grid(1).atomicSequence(seqName, 0L, true);

            multithreaded(new Runnable() {
                @Override public void run() {
                    for (int i = 0; i < ITERATION_NUM; i++) {
                        if (i % 2 == 0)
                            seq1.incrementAndGet();
                        else
                            seq2.incrementAndGet();
                    }
                }
            }, THREAD_NUM);

            long seq1Val = seq1.get();
            long seq2Val = seq2.get();

            assertEquals(ITERATION_NUM * THREAD_NUM + (seq1Val < seq2Val ? 0 : 10), seq1Val);
            assertEquals(ITERATION_NUM * THREAD_NUM + (seq1Val < seq2Val ? 10 : 0), seq2Val);
        }
        finally {
            stopGrid(1);
        }
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
        checkGetAndAdd(5);
    }

    /** @throws Exception If failed. */
    public void testGetAndAdd2() throws Exception {
        checkGetAndAdd(3);
    }

    /**
     * @param val Value.
     * @throws Exception If failed.
     */
    private void checkGetAndAdd(final int val) throws Exception {
        // Random sequence names.
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0L, true);

        runSequenceClosure(new GridInUnsafeClosure<IgniteAtomicSequence>() {
            @Override public void apply(IgniteAtomicSequence t) {
                t.getAndAdd(val);
            }
        }, seq, ITERATION_NUM, THREAD_NUM);

        assertEquals(val * ITERATION_NUM * THREAD_NUM, seq.get());
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
