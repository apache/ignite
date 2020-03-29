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

package org.apache.ignite.internal.benchmarks.jmh.future;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/**
 *
 */
public class JmhFutureAdapterBenchmark extends JmhAbstractBenchmark {
    /** */
    private static final IgniteInClosure<IgniteInternalFuture<Long>> LSNR = new IgniteInClosure<IgniteInternalFuture<Long>>() {
        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture<Long> fut) {
            // No-op
        }
    };

    /** */
    private static final Long RES = 0L;

    /**
     *
     */
    @State(Scope.Thread)
    public static class CompleteState {
        /** */
        private final BlockingQueue<GridFutureAdapter<Long>> queue = new ArrayBlockingQueue<>(10);

        /** */
        private final Thread compleete = new Thread() {

            /** {@inheritDoc} */
            @Override public void run() {
                while (!Thread.interrupted()) {
                    GridFutureAdapter<Long> fut = queue.poll();
                    if (fut != null)
                        fut.onDone(RES);
                }
            }
        };

        /**
         *
         */
        @Setup public void setup() {
            compleete.start();
        }

        /**
         * @throws InterruptedException If failed.
         */
        @TearDown public void destroy() throws InterruptedException {
            compleete.interrupt();
            compleete.join();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Benchmark
    public void testSimpleGet() throws Exception {
        GridFutureAdapter<Long> fut = new GridFutureAdapter<>();
        fut.onDone(RES);
        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Benchmark
    public void testSimpleGetWithListener() throws Exception {
        GridFutureAdapter<Long> fut = new GridFutureAdapter<>();
        fut.listen(LSNR);
        fut.onDone(RES);
        fut.get();
    }

    /**
     * @param state Benchmark context.
     * @throws Exception If failed.
     */
    @Benchmark
    @Threads(4)
    public void completeFutureGet(CompleteState state) throws Exception {
        GridFutureAdapter<Long> fut = new GridFutureAdapter<>();
        state.queue.put(fut);
        fut.get();
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run(8);
    }

    /**
     * Run benchmark.
     *
     * @param threads Amount of threads.
     * @throws Exception If failed.
     */
    private static void run(int threads) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(threads)
            .warmupIterations(30)
            .measurementIterations(30)
            .benchmarks(JmhFutureAdapterBenchmark.class.getSimpleName())
            .jvmArguments("-Xms4g", "-Xmx4g")
            .run();
    }
}
