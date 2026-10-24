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

package org.apache.ignite.internal.benchmarks.jmh.sequence;

import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.openjdk.jmh.annotations.Benchmark;

/** Operations of the data structures, called from a client node; each structure is created in setup. */
public class JmhDataStructuresAllocationBenchmark extends JmhCacheAbstractBenchmark {
    /** Element of the queue and the set, and the first value of the atomic reference. */
    private static final Integer ITEM = 1_000_000;

    /** Second value of the atomic reference. */
    private static final Integer OTHER = 1_000_001;

    /** */
    private IgniteAtomicLong atomicLong;

    /** */
    private IgniteAtomicSequence seq;

    /** */
    private IgniteAtomicReference<Integer> ref;

    /** Whether {@link #ref} holds {@link #ITEM}. */
    private boolean refItem = true;

    /** */
    private IgniteQueue<Integer> queue;

    /** */
    private IgniteSet<Integer> set;

    /** */
    private IgniteSemaphore semaphore;

    /** */
    private IgniteLock lock;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        CollectionConfiguration colCfg = new CollectionConfiguration().setBackups(1);

        atomicLong = node.atomicLong("long", 0, true);
        seq = node.atomicSequence("seq", 0, true);
        ref = node.atomicReference("ref", ITEM, true);
        queue = node.queue("queue", 0, colCfg);
        set = node.set("set", colCfg);
        semaphore = node.semaphore("semaphore", 1, false, true);
        lock = node.reentrantLock("lock", false, false, true);
    }

    /** {@inheritDoc} */
    @Override protected int dataNodes() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected boolean clientMode() {
        return true;
    }

    /** @return New value of the atomic long. */
    @Benchmark
    public long atomicLongIncrement() {
        return atomicLong.incrementAndGet();
    }

    /** @return New value of the atomic sequence, which reserves a range of values at a time. */
    @Benchmark
    public long atomicSequenceIncrement() {
        return seq.incrementAndGet();
    }

    /** @return {@code true}: the reference changes between two values. */
    @Benchmark
    public boolean atomicReferenceCompareAndSet() {
        boolean res = refItem ? ref.compareAndSet(ITEM, OTHER) : ref.compareAndSet(OTHER, ITEM);

        refItem = !refItem;

        return res;
    }

    /** @return The element, offered to the queue and polled back. */
    @Benchmark
    public Integer queueOfferPoll() {
        queue.offer(ITEM);

        return queue.poll();
    }

    /** @return {@code true}: the element, added to the set and removed. */
    @Benchmark
    public boolean setAddRemove() {
        set.add(ITEM);

        return set.remove(ITEM);
    }

    /** Acquires and releases the semaphore's only permit. */
    @Benchmark
    public void semaphoreAcquireRelease() {
        semaphore.acquire();
        semaphore.release();
    }

    /** Locks and unlocks the reentrant lock. */
    @Benchmark
    public void lockUnlock() {
        lock.lock();
        lock.unlock();
    }
}
