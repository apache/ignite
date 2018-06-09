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

import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteEx;

/**
 * DataStructures re-creation tests.
 */
public class IgniteDataStructuresRecreateTest extends IgniteAtomicsAbstractTest {
    /** */
    public static final String SEQUENCE_NAME = "sequence";

    /** */
    public static final String ATOMIC_LONG_NAME = "atomicLong";

    /** */
    public static final String SEMAPHORE_NAME = "semaphore";

    /** */
    public static final String COUNT_DOWN_LATCH_NAME = "cdl";

    /** */
    public static final String QUEUE_NAME = "queue";

    /** */
    public static final String LOCK_NAME = "lock";

    /** */
    public static final String STAMP_NAME = "stamp";

    /** */
    public static final String SET_NAME = "set";

    /** Number of re-creation attempts*/
    public static final int ITERATION_COUNT = 100;

    /** {@inheritDoc} */
    @Override protected CacheMode atomicsCacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /**
     * @return collection configuration.
     */
    private CollectionConfiguration getColConfiguration() {
        return new CollectionConfiguration()
            .setCacheMode(atomicsCacheMode());
    }

    /** */
    public void testAtomicSequence() {
        for (int i = 0; i < ITERATION_COUNT; i++) {
            recreateAtomicSequence(grid(0), grid(0));
            recreateAtomicSequence(grid(0), grid(1));
        }
    }

    /** */
    public void testAtomicLong() {
        for (int i = 0; i < ITERATION_COUNT; i++) {
            recreateAtomicLong(grid(0), grid(0));
            recreateAtomicLong(grid(0), grid(1));
        }
    }

    /** */
    public void testSemaphore() {
        for (int i = 0; i < ITERATION_COUNT; i++) {
            recreateSemaphore(grid(0), grid(0));
            recreateSemaphore(grid(0), grid(1));
        }
    }

    /** */
    public void testCountDownLatch() {
        for (int i = 0; i < ITERATION_COUNT; i++) {
            recreateCoundDownLatch(grid(0), grid(0));
            recreateCoundDownLatch(grid(0), grid(1));
        }
    }

    /** */
    public void testQueueRecreate() {
        for (int i = 0; i < ITERATION_COUNT; i++) {
            recreateQueue(grid(0), grid(0));
            recreateQueue(grid(0), grid(1));
        }
    }

    /** */
    public void testLock() {
        for (int i = 0; i < ITERATION_COUNT; i++) {
            recreateLock(grid(0), grid(0));
            recreateLock(grid(0), grid(1));
        }
    }

    /** */
    public void testStamp() {
        for (int i = 0; i < ITERATION_COUNT; i++) {
            recreateAtomicStamp(grid(0), grid(0));
            recreateAtomicStamp(grid(0), grid(1));
        }
    }

    /** */
    public void testSet() {
        for (int i = 0; i < ITERATION_COUNT; i++) {
            recreateSet(grid(0), grid(0));
            recreateSet(grid(0), grid(1));
        }
    }

    /**
     * Checks re-creation for a semaphore.
     *
     * @param ex1 ignite node that creates data structure.
     * @param ex2 ignite node that re-creates data structure.
     */
    protected void recreateSemaphore(IgniteEx ex1, IgniteEx ex2) {
        IgniteSemaphore sem = ex1.semaphore(SEMAPHORE_NAME, 5, true, true);

        assertFalse(sem.removed());

        sem.close();

        IgniteSemaphore newSem = ex2.semaphore(SEMAPHORE_NAME, 10, true, true);

        assertFalse(newSem.removed());
        assertEquals(10, newSem.availablePermits());

        newSem.close();
    }

    /**
     * Checks re-creation for an atomic long.
     *
     * @param ex1 ignite node that creates data structure.
     * @param ex2 ignite node that re-creates data structure.
     */
    protected void recreateAtomicLong(IgniteEx ex1, IgniteEx ex2) {
        IgniteAtomicLong atomicLong = ex1.atomicLong(ATOMIC_LONG_NAME, 0, true);

        assertFalse(atomicLong.removed());

        atomicLong.close();

        long val = System.currentTimeMillis();

        IgniteAtomicLong newAtomicLong = ex2.atomicLong(ATOMIC_LONG_NAME, val, true);

        assertFalse(newAtomicLong.removed());
        assertEquals(val + 1, newAtomicLong.incrementAndGet());

        newAtomicLong.close();
    }

    /**
     * Checks re-creation for an atomic sequence.
     *
     * @param ex1 ignite node that creates data structure.
     * @param ex2 ignite node that re-creates data structure.
     */
    protected void recreateAtomicSequence(IgniteEx ex1, IgniteEx ex2) {
        IgniteAtomicSequence seq = ex1.atomicSequence(SEQUENCE_NAME, 0, true);

        assertFalse(seq.removed());

        seq.close();

        long val = System.currentTimeMillis();

        IgniteAtomicSequence newSeq = ex2.atomicSequence(SEQUENCE_NAME, val, true);

        assertFalse(newSeq.removed());
        assertEquals(val + 1, newSeq.incrementAndGet());

        newSeq.close();
    }

    /**
     * Checks re-creation for a count down latch.
     *
     * @param ex1 ignite node that creates data structure.
     * @param ex2 ignite node that re-creates data structure.
     */
    protected void recreateCoundDownLatch(IgniteEx ex1, IgniteEx ex2) {
        IgniteCountDownLatch latch = ex1.countDownLatch(COUNT_DOWN_LATCH_NAME, 5, false, true);

        assertFalse(latch.removed());

        latch.countDownAll();
        latch.close();

        IgniteCountDownLatch newLatch = ex2.countDownLatch(COUNT_DOWN_LATCH_NAME, 10, false, true);

        assertFalse(newLatch.removed());
        assertEquals(10, newLatch.count());

        newLatch.countDownAll();
        newLatch.close();
    }

    /**
     * Checks re-creation for a queue.
     *
     * @param ex1 ignite node that creates data structure.
     * @param ex2 ignite node that re-creates data structure.
     */
    private void recreateQueue(IgniteEx ex1, IgniteEx ex2) {
        IgniteQueue queue = ex1.queue(QUEUE_NAME, 5, getColConfiguration());

        assertFalse(queue.removed());

        queue.close();

        IgniteQueue newQueue = ex2.queue(QUEUE_NAME, 10, getColConfiguration());

        assertFalse(newQueue.removed());
        assertEquals(10, newQueue.capacity());

        newQueue.close();
    }

    /**
     * Checks re-creation for a lock.
     *
     * @param ex1 ignite node that creates data structure.
     * @param ex2 ignite node that re-creates data structure.
     */
    private void recreateLock(IgniteEx ex1, IgniteEx ex2) {
        IgniteLock lock = ex1.reentrantLock(LOCK_NAME, true, false, true);

        assertFalse(lock.isFair());
        assertFalse(lock.removed());

        lock.close();

        IgniteLock newLock = ex2.reentrantLock(LOCK_NAME, true, true, true);

        assertFalse(newLock.removed());
        assertTrue(newLock.isFair());

        newLock.close();
    }

    /**
     * Checks re-creation for an atomic stamp.
     *
     * @param ex1 ignite node that creates data structure.
     * @param ex2 ignite node that re-creates data structure.
     */
    private void recreateAtomicStamp(IgniteEx ex1, IgniteEx ex2) {
        IgniteAtomicStamped seq = ex1.atomicStamped(STAMP_NAME, 0, 0, true);

        assertFalse(seq.removed());

        seq.close();

        IgniteAtomicStamped newSeq = ex2.atomicStamped(STAMP_NAME, 10, 20, true);

        assertFalse(newSeq.removed());
        assertEquals(10, newSeq.value());

        newSeq.close();
    }

    /**
     * Checks re-creation for a set.
     *
     * @param ex1 ignite node that creates data structure.
     * @param ex2 ignite node that re-creates data structure.
     */
    private void recreateSet(IgniteEx ex1, IgniteEx ex2) {
        IgniteSet<String> seq = ex1.set(SET_NAME, getColConfiguration());

        seq.add("val");

        assertFalse(seq.removed());

        seq.close();

        IgniteSet newSet = ex2.set(SET_NAME, getColConfiguration());

        assertFalse(newSet.removed());
        assertEquals(0, newSet.size());

        newSet.close();
    }

}
