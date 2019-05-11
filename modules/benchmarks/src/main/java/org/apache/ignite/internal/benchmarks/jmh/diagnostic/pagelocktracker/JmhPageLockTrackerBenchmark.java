package org.apache.ignite.internal.benchmarks.jmh.diagnostic.pagelocktracker;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.benchmarks.jmh.diagnostic.pagelocktracker.stack.LockTrackerNoBarrier;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTracerFactory;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTracerFactory.HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTracerFactory.HEAP_STACK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTracerFactory.OFF_HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTracerFactory.OFF_HEAP_STACK;

public class JmhPageLockTrackerBenchmark {

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
            .include(JmhPageLockTrackerBenchmark.class.getSimpleName())
            .build();

        new Runner(opt).run();
    }

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

    private static PageLockListener create(String name, String type, boolean barrier) {
        PageLockTracker tracker;

        switch (type) {
            case "HeapArrayLockStack":
                tracker = LockTracerFactory.create(HEAP_STACK, name);
                break;
            case "HeapArrayLockLog":
                tracker = LockTracerFactory.create(HEAP_LOG, name);
                break;
            case "OffHeapLockStack":
                tracker = LockTracerFactory.create(OFF_HEAP_STACK, name);
                break;

            case "OffHeapLockLog":
                tracker = LockTracerFactory.create(OFF_HEAP_LOG, name);
                break;
            default:
                throw new IllegalArgumentException("type:" + type);
        }

        return barrier ? tracker : new LockTrackerNoBarrier(tracker);
    }
}
