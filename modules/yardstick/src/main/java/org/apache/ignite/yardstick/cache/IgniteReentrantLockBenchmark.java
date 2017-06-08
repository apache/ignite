package org.apache.ignite.yardstick.cache;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLock;

/**
 * Ignite benchmark that does put and get operations protected by ignite reentrant lock.
 */
public class IgniteReentrantLockBenchmark extends IgniteCacheAbstractBenchmark<Integer, Integer> {
    /** */
    private static final String CACHE_NAME = "atomic";

    /** */
    private static final int MAX_BUF_SIZE = 10;

    /** */
    private static final String BUFFER_KEY = "cycleBuffer";

    /** */
    private static final String COUNTER_KEY = "counter";

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Queue<Closeable> buf = (Queue<Closeable>)ctx.get(BUFFER_KEY);
        if (buf == null) {
            buf = new LinkedList<>();
            ctx.put(BUFFER_KEY, buf);
        }
        if(buf.size() == MAX_BUF_SIZE) {
            Closeable closeable = buf.poll();
            closeable.close();
        }

        Integer cntr = (Integer)ctx.get(COUNTER_KEY);
        if (cntr == null)
            cntr = 0;

        IgniteLock lock = ignite().reentrantLock(Thread.currentThread().getName() + cntr++, true, false, true);

        buf.offer(lock);

        ctx.put(COUNTER_KEY, cntr);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Integer> cache() {
        return ignite().cache(CACHE_NAME);
    }
}
