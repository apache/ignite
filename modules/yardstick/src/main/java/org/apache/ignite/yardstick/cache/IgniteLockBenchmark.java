package org.apache.ignite.yardstick.cache;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLock;

/**
 * Ignite benchmark that performs Ignite.reentrantLock operations.
 */
public class IgniteLockBenchmark extends IgniteCacheLockBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        final int n = nextRandom(args.range());
        final String key = "key"+n;
        final String otherKey = "other_"+key;
        final IgniteCache<String, Integer> cache = cacheForOperation();
        final IgniteLock lock = ignite().reentrantLock(key, false, false, true);

        lock.lock();
        try {
            cache.put(otherKey, cache.get(otherKey)+1);
        }
        finally {
            lock.unlock();
        }
        return true;
    }
}
