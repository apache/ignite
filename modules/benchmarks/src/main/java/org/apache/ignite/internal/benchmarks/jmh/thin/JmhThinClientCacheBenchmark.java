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

package org.apache.ignite.internal.benchmarks.jmh.thin;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;

/**
 * Thin client cache benchmark.
 *
 * Results on i7-9700K, Ubuntu 20.04.1, JDK 1.8.0_275:
 * Benchmark                         Mode  Cnt      Score      Error  Units
 * JmhThinClientCacheBenchmark.get  thrpt   10  92501.557 ± 1380.384  ops/s
 * JmhThinClientCacheBenchmark.put  thrpt   10  82907.446 ± 7572.537  ops/s
 *
 * JmhThinClientCacheBenchmark.get  avgt    10  41.505 ± 1.018        us/op
 * JmhThinClientCacheBenchmark.put  avgt    10  44.623 ± 0.779        us/op
 */
public class JmhThinClientCacheBenchmark extends JmhThinClientAbstractBenchmark {
    /**
     * Cache put benchmark.
     */
    @Benchmark
    public void put() {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        cache.put(key, PAYLOAD);
    }

    /**
     * Cache get benchmark.
     */
    @Benchmark
    public Object get() {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        return cache.get(key);
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        JmhIdeBenchmarkRunner runner = JmhIdeBenchmarkRunner.create()
                .forks(1)
                .threads(4)
                .benchmarks(JmhThinClientCacheBenchmark.class.getSimpleName())
                .jvmArguments("-Xms4g", "-Xmx4g");

        runner
                .benchmarkModes(Mode.Throughput)
                .run();

        runner
                .benchmarkModes(Mode.AverageTime)
                .outputTimeUnit(TimeUnit.MICROSECONDS)
                .run();
    }
}
