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
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Compare put with expiry policy and without expiry policy.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(16)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 3, time = 10)
public class JmhCacheExpireBenchmark {
    /** Items count. */
    private static final int CNT = 100000;

    /** Ignite. */
    private Ignite ignite;

    /** Cache without expire policy. */
    private IgniteCache<Integer, Integer> cacheReg;

    /** Cache with expire policy. */
    private IgniteCache<Integer, Integer> cacheExp;

    /** */
    @Benchmark
    public void putWithExpire() {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        cacheExp.put(key, key);
    }

    /** */
    @Benchmark
    public void putWithoutExpire() {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        cacheReg.put(key, key);
    }

    /**
     * Initiate Ignite and caches.
     */
    @Setup(Level.Trial)
    public void setup() {
        ignite = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("test"));

        cacheReg = ignite.getOrCreateCache(new CacheConfiguration<>("CACHE_REG"));

        cacheExp = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>("CACHE_EXP")
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, 1)))
        );
    }

    /**
     * Clear caches.
     */
    @Setup(Level.Iteration)
    public void setupIteration() {
        cacheReg.clear();
        cacheExp.clear();
    }

    /**
     * Stop Ignite instance.
     */
    @TearDown
    public void tearDown() {
        ignite.close();
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhCacheExpireBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }
}
