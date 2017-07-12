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

import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * IgniteCache.lock() vs Ignite.reentrantLock().
 */
@Warmup(iterations = 40)
@Measurement(iterations = 20)
@Fork(1)
public class JmhCacheLocksBenchmark extends JmhCacheAbstractBenchmark {
    /** Fixed lock key for Ignite.reentrantLock() and IgniteCache.lock(). */
    private static final String lockKey = "key0";

    /** Parameter for Ignite.reentrantLock(). */
    private static final boolean failoverSafe = false;

    /** Parameter for Ignite.reentrantLock(). */
    private static final boolean fair = false;

    /** IgniteCache.lock() with a fixed lock key. */
    private Lock cacheLock;

    /** Ignite.reentrantLock() with a fixed lock key. */
    private IgniteLock igniteLock;

    /**
     * Test IgniteCache.lock() with fixed key and no-op inside.
     */
    @Benchmark
    public void cacheLock() {
        cacheLock.lock();
        cacheLock.unlock();
    }

    /**
     * Test Ignite.reentrantLock() with fixed key and no-op inside.
     */
    @Benchmark
    public void igniteLock() {
        igniteLock.lock();
        igniteLock.unlock();
    }

    /**
     * Create locks and put values in the cache.
     */
    @Setup(Level.Trial)
    public void createLock() {
        cacheLock = cache.lock(lockKey);

        igniteLock = node.reentrantLock(lockKey, failoverSafe, fair, true);
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final String simpleClsName = JmhCacheLocksBenchmark.class.getSimpleName();
        final int threads = 4;
        final boolean client = true;
        final CacheAtomicityMode atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
        final CacheWriteSynchronizationMode writeSyncMode = CacheWriteSynchronizationMode.FULL_SYNC;

        final String output = simpleClsName +
            "-" + threads + "-threads" +
            "-" + (client ? "client" : "data") +
            "-" + atomicityMode +
            "-" + writeSyncMode;

        final Options opt = new OptionsBuilder()
            .threads(threads)
            .include(simpleClsName)
            .output(output + ".jmh.log")
            .jvmArgs(
                "-Xms1g",
                "-Xmx1g",
                "-XX:+UnlockCommercialFeatures",
                JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, atomicityMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, writeSyncMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 4),
                JmhIdeBenchmarkRunner.createProperty(PROP_CLIENT_MODE, client)).build();

        new Runner(opt).run();
    }
}
