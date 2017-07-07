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
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
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

    /** Fixed key. */
    private static final String lockKey1 = "key1";

    /** Number of locks for randomly tests. */
    private static final int N = 128;

    /** Parameter for Ignite.reentrantLock(). */
    private static final boolean failoverSafe = false;

    /** Parameter for Ignite.reentrantLock(). */
    private static final boolean fair = false;

    /** IgniteCache.lock() with a fixed lock key. */
    private Lock cacheLock;

    /** Ignite.reentrantLock() with a fixed lock key. */
    private IgniteLock igniteLock;

    /** Generate a random key for a benchmark. */
    @State(Scope.Benchmark)
    public static class Key {
        /** */
        final String key;

        /** */
        public Key() {
            key = "key" + ThreadLocalRandom.current().nextInt(N);
        }
    }

    /** Generate two random keys for a benchmark. */
    @State(Scope.Benchmark)
    public static class KeyWithOther {
        /** */
        final String key;

        /** */
        final String otherKey;

        /** */
        public KeyWithOther() {
            final int n = ThreadLocalRandom.current().nextInt(N);

            key = "key" + n;
            otherKey = "key" + (n + 1);
        }
    }

    /**
     * Test IgniteCache.lock() with fixed key and increment operation inside.
     */
    @Benchmark
    public void cacheLocks() {
        cacheLock.lock();

        try {
            cache.put(lockKey, (Integer)cache.get(lockKey) + 1);
        }
        finally {
            cacheLock.unlock();
        }
    }

    /**
     * Test Ignite.reentrantLock() with fixed key and increment operation inside.
     */
    @Benchmark
    public void igniteLocks() {
        igniteLock.lock();

        try {
            cache.put(lockKey, (Integer)cache.get(lockKey) + 1);
        }
        finally {
            igniteLock.unlock();
        }
    }

    /**
     * Test IgniteCache.lock() with fixed key and increment operation inside.
     */
    @Benchmark
    public void cacheLocksOtherKey() {
        cacheLock.lock();

        try {
            cache.put(lockKey1, (Integer)cache.get(lockKey1) + 1);
        }
        finally {
            cacheLock.unlock();
        }
    }

    /**
     * Test Ignite.reentrantLock() with fixed key and increment operation inside.
     */
    @Benchmark
    public void igniteLocksOtherKey() {
        igniteLock.lock();

        try {
            cache.put(lockKey1, (Integer)cache.get(lockKey1) + 1);
        }
        finally {
            igniteLock.unlock();
        }
    }

    /**
     * Test IgniteCache.lock() with fixed key and no-op inside.
     */
    @Benchmark
    public void cacheNopLocks() {
        cacheLock.lock();
        cacheLock.unlock();
    }

    /**
     * Test Ignite.reentrantLock() with fixed key and no-op inside.
     */
    @Benchmark
    public void igniteNopLocks() {
        igniteLock.lock();
        igniteLock.unlock();
    }

    /**
     * Test IgniteCache.lock() with randomly keys.
     */
    @Benchmark
    public void cacheRandomLocks(Key key) {
        final String k = key.key;
        final Lock lock = cache.lock(k);

        lock.lock();

        try {
            cache.put(k, (Integer)cache.get(k) + 1);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Test Ignite.reentrantLock() with randomly keys.
     */
    @Benchmark
    public void igniteRandomLocks(Key key) {
        final String k = key.key;
        final IgniteLock lock = node.reentrantLock(k, failoverSafe, fair, true);

        lock.lock();

        try {
            cache.put(k, (Integer)cache.get(k) + 1);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Test IgniteCache.lock() with randomly keys.
     */
    @Benchmark
    public void cacheRandomLocksOtherKey(KeyWithOther key) {
        final String k = key.key;
        final String other = key.otherKey;
        final Lock lock = cache.lock(k);

        lock.lock();

        try {
            cache.put(other, (Integer)cache.get(other) + 1);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Test Ignite.reentrantLock() with randomly keys.
     */
    @Benchmark
    public void igniteRandomLocksOtherKey(KeyWithOther key) {
        final String k = key.key;
        final String other = key.otherKey;
        final IgniteLock lock = node.reentrantLock(k, failoverSafe, fair, true);

        lock.lock();

        try {
            cache.put(other, (Integer)cache.get(other) + 1);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Create locks and put values in the cache.
     */
    @Setup(Level.Trial)
    public void createLock() {
        for (int i = 0; i < 2 * N; i++)
            cache.put("key" + i, new Integer(i));

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
