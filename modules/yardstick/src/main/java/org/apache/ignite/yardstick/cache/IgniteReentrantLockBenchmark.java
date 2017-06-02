package org.apache.ignite.yardstick.cache;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.yardstick.IgniteBenchmarkUtils.doInTransaction;

public class IgniteReentrantLockBenchmark extends IgniteCacheAbstractBenchmark<Integer, Integer> {
    /** */
    private static final String CACHE_NAME = "atomic";

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final ThreadRange r = threadRange();

        IgniteCache<Integer, Integer> cache = cache();

        int key = r.nextRandom();

        IgniteLock lock = ignite().reentrantLock(CACHE_NAME + key, true, false, true);

        lock.lock();

        try {
            Integer val = cache.get(key);

            if(val == null)
                val = 0;
            else
                val++;

            cache.put(key, val);
        }
        finally {
            lock.unlock();
            lock.removed();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Integer> cache() {
        return ignite().cache(CACHE_NAME);
    }
}
