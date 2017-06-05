package org.apache.ignite.yardstick.cache;

import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that does put and get operations protected by ignite reentrant lock.
 */
public class IgniteReentrantLockBenchmark extends IgniteCacheAbstractBenchmark<Integer, Integer> {
    /** */
    private static final String CACHE_NAME = "atomic";

    /** */
    private static final int PRE_CREATED_REENTRANT_LOCK_COUNT = 1000;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        for (int i = 0; i < PRE_CREATED_REENTRANT_LOCK_COUNT; i++)
            ignite().reentrantLock(CACHE_NAME + "EXTRA" + i, true, false, true);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final ThreadRange r = threadRange();

        IgniteCache<Integer, Integer> cache = cache();

        int key = r.nextRandom();

        IgniteLock lock = null;

        boolean locked = false;

        while (!locked) {
            try {
                lock = ignite().reentrantLock(CACHE_NAME + key, true, false, true);

                locked = lock.tryLock();
            } catch (Exception e) {
                // no-op
            }
        }

        try {
            Integer val = cache.get(key);

            val = val == null ? 0 : val+1;

            cache.put(key, val);
        }
        finally {
            try {
                lock.unlock();
                lock.close();
            } catch (IgniteException e) {
                // no-op
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Integer> cache() {
        return ignite().cache(CACHE_NAME);
    }
}
