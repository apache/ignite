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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compare put with in-place update and without in-place update.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 3, time = 10)
public class JmhCacheInPlaceUpdateBenchmark {
    /** Items count. */
    private static final int CNT = 100;

    /** Entry size. */
    private static final int ENTRY_SIZE = 100 * 1024;

    /** Ignite. */
    private Ignite ignite;

    /** Cache with in-place updates. */
    private IgniteCache<Integer, byte[]> cache0;

    /** Cache without in-place updates. */
    private IgniteCache<Integer, byte[]> cache1;

    /** Entry payloads for cache 0. */
    private final byte[][] payloads0 = new byte[CNT][ENTRY_SIZE];

    /** Entry payloads for cache 1. */
    private final byte[][] payloads1 = new byte[CNT][ENTRY_SIZE];

    /** Persistence enabled. */
    @Param({"FALSE", "TRUE"})
    private String persistence;

    /** */
    @Benchmark
    public void putWithInPlaceUpdate() {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        changeAndPutPayload(cache0, key, payloads0[key]);
    }

    /** */
    @Benchmark
    public void putWithoutInPlaceUpdate() {
        int key = ThreadLocalRandom.current().nextInt(CNT);

        changeAndPutPayload(cache1, key, payloads1[key]);
    }

    /** */
    private void changeAndPutPayload(IgniteCache<Integer, byte[]> cache, int key, byte[] payload) {
        // Change 1 byte.
        payload[ThreadLocalRandom.current().nextInt(payload.length)] = (byte)ThreadLocalRandom.current().nextInt(256);

        cache.put(key, payload);
    }

    /**
     * Initiate Ignite and caches.
     */
    @Setup(Level.Trial)
    public void setup() {
        ignite = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("test")
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(Boolean.parseBoolean(persistence))
            ))
        );

        ignite.cluster().state(ClusterState.ACTIVE);

        cache0 = ignite.getOrCreateCache(new CacheConfiguration<>("CACHE0"));

        // Enable expiration for second cache, but set eager ttl to false, this will disable in-place updates,
        // but without performance overhead to maintain expiration.
        cache1 = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, byte[]>("CACHE1")
                .setEagerTtl(false)
                .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_DAY))
        );
    }

    /**
     * Clear caches.
     */
    @Setup(Level.Iteration)
    public void setupIteration() {
        for (int i = 0; i < CNT; i++) {
            ThreadLocalRandom.current().nextBytes(payloads0[i]);
            ThreadLocalRandom.current().nextBytes(payloads1[i]);
            cache0.put(i, payloads0[i]);
            cache1.put(i, payloads1[i]);
        }
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
        JmhIdeBenchmarkRunner.create()
            .benchmarks(JmhCacheInPlaceUpdateBenchmark.class.getSimpleName())
            .run();
    }
}
