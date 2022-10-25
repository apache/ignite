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

package org.apache.ignite.internal.benchmarks.jmh.streamer;

import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.WALMode;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * For research of the streamer throughput with different settings and the receivers and with persistent cache.
 */
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Benchmark)
@Threads(1)
@Measurement(iterations = 5)
@Warmup(iterations = 3)
public class JmhPersistentStreamerReceiverBenchmark extends JmhAbstractStreamerReceiverBenchmark {
    /** */
    public JmhPersistentStreamerReceiverBenchmark() {
        super(false, -1);
    }

    /** */
    @Setup(Level.Trial)
    public void setupPersistent(PersistentParams params){
        setup(params);
    }

    /**
     * Run benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        new Runner(options(JmhPersistentStreamerReceiverBenchmark.class)).run();
    }

    /** */
    @State(Scope.Benchmark)
    public static class PersistentParams implements Params {
        @Param({"1", "2"})
        private int servers;

        @Param({"NONE", "LOG_ONLY"})
        private WALMode walMode;

        @Param({"PRIMARY_SYNC", "FULL_SYNC"})
        private CacheWriteSynchronizationMode cacheWriteMode;

        @Param({"50", "500"})
        private int avgDataSize;

        @Param({"256", "512"})
        private int dsBatchSize;

        @Param({"4", "8", "16"})
        private int maxDsOps;

        /** {@inheritDoc} */
        @Override public int servers() {
            return servers;
        }

        /** {@inheritDoc} */
        @Override public WALMode walMode() {
            return walMode;
        }

        /** {@inheritDoc} */
        @Override public CacheWriteSynchronizationMode cacheWriteMode() {
            return cacheWriteMode;
        }

        /** {@inheritDoc} */
        @Override public int avgDataSize() {
            return avgDataSize;
        }

        /** {@inheritDoc} */
        @Override public int dsBatchSize() {
            return dsBatchSize;
        }

        /** {@inheritDoc} */
        @Override public int maxDsOps() {
            return maxDsOps;
        }
    }
}
