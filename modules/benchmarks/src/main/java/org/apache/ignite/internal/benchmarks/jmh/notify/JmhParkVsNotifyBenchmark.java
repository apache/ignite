/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
