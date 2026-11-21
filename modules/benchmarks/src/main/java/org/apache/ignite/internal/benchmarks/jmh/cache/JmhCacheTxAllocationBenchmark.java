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

package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationHistogramProfiler;
import org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationProfiler;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.TimeValue;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/** Puts in an explicit transaction, by concurrency, isolation and keys per transaction. */
@SuppressWarnings("unchecked")
public class JmhCacheTxAllocationBenchmark extends JmhCacheAllocationAbstractBenchmark {
    /** Transaction concurrency. */
    @Param("PESSIMISTIC")
    private TransactionConcurrency concurrency;

    /** Transaction isolation. */
    @Param("REPEATABLE_READ")
    private TransactionIsolation isolation;

    /** Keys per transaction. */
    @Param("2")
    private int txKeys;

    /** Keys. */
    private Integer[] keys;

    /** Values. */
    private IntValue[] vals;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        keys = new Integer[CNT];
        vals = new IntValue[CNT];

        for (int i = 0; i < CNT; i++) {
            keys[i] = i;
            vals[i] = new IntValue(i);
        }

        populate(keys, vals);
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return super.cacheConfiguration().setAtomicityMode(TRANSACTIONAL);
    }

    /** Replaces the values of {@link #txKeys} consecutive keys with the same values in a transaction. */
    @Benchmark
    public void txPut() {
        int first = ThreadLocalRandom.current().nextInt(CNT - txKeys + 1);

        try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {
            for (int i = first; i < first + txKeys; i++)
                cache.put(keys[i], vals[i]);

            tx.commit();
        }
    }

    /**
     * Runs the benchmark with both profilers; writes {@code <class name>.json} and the tables to {@code <class name>/}.
     *
     * @param args Arguments.
     */
    public static void main(String[] args) throws Exception {
        String name = JmhCacheTxAllocationBenchmark.class.getSimpleName();

        Options opts = JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(1)
            .warmupIterations(5)
            .warmupTime(TimeValue.seconds(3))
            .measurementIterations(5)
            .measurementTime(TimeValue.seconds(3))
            .benchmarks(name)
            .jvmArguments("-Xms2g", "-Xmx2g", "-XX:-DoEscapeAnalysis", "-XX:-UseTLAB",
                "-XX:FlightRecorderOptions:memorysize=512m")
            .optionsBuilder()
            .addProfiler(JmhAllocationHistogramProfiler.class, "dir=" + name)
            .addProfiler(JmhAllocationProfiler.class)
            .resultFormat(ResultFormatType.JSON)
            .result(name + ".json")
            .shouldFailOnError(true)
            .build();

        new Runner(opts).run();
    }
}
