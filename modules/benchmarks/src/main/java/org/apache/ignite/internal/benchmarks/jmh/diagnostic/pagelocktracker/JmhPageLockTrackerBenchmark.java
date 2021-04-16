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
package org.apache.ignite.internal.benchmarks.jmh.diagnostic.pagelocktracker;

import org.apache.ignite.internal.benchmarks.jmh.diagnostic.pagelocktracker.stack.LockTrackerNoBarrier;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.HEAP_STACK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.OFF_HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.OFF_HEAP_STACK;

/**
 * Benchmark PageLockTracker (factory LockTrackerFactory)
 */
public class JmhPageLockTrackerBenchmark {
    /**
     * @param args Params.
     */
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(JmhPageLockTrackerBenchmark.class.getSimpleName())
            .build();

        new Runner(opt).run();
    }

    /** */
    @State(Scope.Thread)
    public static class ThreadLocalState {
        PageLockListener pl;

        @Param({"2", "4", "8", "16"})
        int stackSize;

        @Param({
            "HeapArrayLockStack",
            "HeapArrayLockLog",
            "OffHeapLockStack",
            "OffHeapLockLog"
        })
        String type;

        @Param({"true", "false"})
        boolean barrier;

        int StructureId = 123;

        @Setup
        public void doSetup() {
            pl = create(Thread.currentThread().getName(), type, barrier);
        }
    }

    /**
     *  Mesure cost for (beforelock -> lock -> unlock) operation.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Fork(1)
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    //@OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void lockUnlock(ThreadLocalState localState) {
        PageLockListener pl = localState.pl;

        for (int i = 0; i < localState.stackSize; i++) {
            int pageId = i + 1;

            pl.onBeforeReadLock(localState.StructureId, pageId, pageId);

            pl.onReadLock(localState.StructureId, pageId, pageId, pageId);
        }

        for (int i = localState.stackSize; i > 0; i--) {
            int pageId = i;

            pl.onReadUnlock(localState.StructureId, pageId, pageId, pageId);
        }
    }

    /**
     * Factory method.
     *
     * @param name Lock tracer name.
     * @param type Lock tracer type.
     * @param barrier If {@code True} use real implementation,
     * if {@code False} use implementation with safety dump barrier.
     * @return Page lock tracker as PageLockListener.
     */
    private static PageLockListener create(String name, String type, boolean barrier) {
        PageLockTracker tracker;

        switch (type) {
            case "HeapArrayLockStack":
                tracker = LockTrackerFactory.create(HEAP_STACK, name);
                break;
            case "HeapArrayLockLog":
                tracker = LockTrackerFactory.create(HEAP_LOG, name);
                break;
            case "OffHeapLockStack":
                tracker = LockTrackerFactory.create(OFF_HEAP_STACK, name);
                break;

            case "OffHeapLockLog":
                tracker = LockTrackerFactory.create(OFF_HEAP_LOG, name);
                break;
            default:
                throw new IllegalArgumentException("type:" + type);
        }

        return barrier ? tracker : new LockTrackerNoBarrier(tracker);
    }
}
