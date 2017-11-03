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

package org.apache.ignite.internal.benchmarks.jmh.notify;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** */
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode({/*Mode.AverageTime,*/ Mode.Throughput})
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Group)
public class JmhParkVsNotifyBenchmark {
    /** level of concurrency */
    private static final int THREAD_COUNT = 16;

    /** Thread. */
    private volatile Thread thread;

    /**
     *
     */
    @Setup(Level.Iteration)
    public void setup() {
        thread = null;
    }

    /**
     *
     */
    @Benchmark
    @Group("park")
    public void park() {
        if (thread == null)
            thread = Thread.currentThread();

        LockSupport.park(thread);
    }

    /**
     *
     */
    @Benchmark
    @GroupThreads(THREAD_COUNT)
    @Group("park")
    public void unpark() {
        LockSupport.unpark(thread);
    }

    /** Mutex. */
    private final Object mux = new Object();

    /**
     *
     */
    @Benchmark
    @Group("condition")
    @GroupThreads(THREAD_COUNT)
    public void notifyAll0() {
        synchronized (mux) {
            mux.notify();
        }
    }

    /**
     *
     */
    @Benchmark
    @Group("condition")
    public void wait0() throws InterruptedException {
        synchronized (mux) {
            mux.wait();
        }
    }
}
