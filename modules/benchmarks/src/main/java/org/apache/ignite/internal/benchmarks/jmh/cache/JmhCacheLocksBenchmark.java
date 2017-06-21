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
@Fork(value = 1)
public class JmhCacheLocksBenchmark extends JmhCacheAbstractBenchmark {
    /** Fixed lock key for IgniteCache.lock(). */
    final static Integer cacheLockKey = new Integer(0);
    /** Fixed lock key for Ignite.reentrantLock(). */
    final static String igniteLockKey = "key";
    /** Number of locks for randomly tests. */
    final static int N = 128;
    /** Parameter for Ignite.reentrantLock(). */
    final static boolean failoverSafe = false;
    /** Parameter for Ignite.reentrantLock(). */
    final static boolean fair = false;
    /** IgniteCache.lock() with a fixed lock key. */
    Lock cacheLock;
    /** Ignite.reentrantLock() with a fixed lock key. */
    IgniteLock igniteLock;

    /**
     * Test IgniteCache.lock() with fixed key and increment operation inside.
     */
    @Benchmark
    public void cacheLocks() {
        cacheLock.lock();
        cache.put(cacheLockKey, ((Integer)cache.get(cacheLockKey)) + 1);
        cacheLock.unlock();
    }

    /**
     * Test Ignite.reentrantLock() with fixed key and increment operation inside.
     */
    @Benchmark
    public void igniteLocks() {
        igniteLock.lock();
        cache.put(cacheLockKey, ((Integer)cache.get(cacheLockKey)) + 1);
        igniteLock.unlock();
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
    public void cacheRandomLocks() {
        final Integer key = ThreadLocalRandom.current().nextInt(N);
        final Lock lock = cache.lock(key);
        lock.lock();
        cache.put(key, ((Integer)cache.get(key)) + 1);
        lock.unlock();
    }

    /**
     * Test Ignite.reentrantLock() with randomly keys.
     */
    @Benchmark
    public void igniteRandomLocks() {
        final int key = ThreadLocalRandom.current().nextInt(N);
        final IgniteLock lock = node.reentrantLock(String.valueOf(key), failoverSafe, fair, true);
        lock.lock();
        cache.put(key, ((Integer)cache.get(key)) + 1);
        lock.unlock();
    }

    /**
     * Create locks and put values in the cache.
     */
    @Setup(Level.Trial)
    public void createLock() {
        for (int i = 0; i < N; i++)
            cache.put(new Integer(i), new Integer(i));
        cacheLock = cache.lock(cacheLockKey);
        igniteLock = node.reentrantLock(igniteLockKey, failoverSafe, fair, true);
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

        String output = simpleClsName +
            "-" + threads + "-threads" +
            "-" + (client ? "client" : "data") +
            "-" + atomicityMode +
            "-" + writeSyncMode;

        Options opt = new OptionsBuilder()
            .threads(threads)
            .include(simpleClsName)
            .output(output + ".jmh.log")
            .jvmArgs(
                "-Xms1g",
                "-Xmx1g",
                "-XX:+UnlockCommercialFeatures",
                JmhIdeBenchmarkRunner.createProperty(PROP_ATOMICITY_MODE, atomicityMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_WRITE_SYNC_MODE, writeSyncMode),
                JmhIdeBenchmarkRunner.createProperty(PROP_DATA_NODES, 2),
                JmhIdeBenchmarkRunner.createProperty(PROP_CLIENT_MODE, client)).build();

        new Runner(opt).run();
    }
}
