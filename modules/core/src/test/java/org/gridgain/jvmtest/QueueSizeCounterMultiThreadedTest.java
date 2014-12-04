/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jvmtest;

import junit.framework.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Test to check strange assertion in eviction manager.
 */
public class QueueSizeCounterMultiThreadedTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void testQueueSizeCounter() throws Exception {
        final ConcurrentLinkedQueue<Integer> q = new ConcurrentLinkedQueue<>();

        final AtomicInteger sizeCnt = new AtomicInteger();

        final AtomicBoolean done = new AtomicBoolean();

        final AtomicBoolean guard = new AtomicBoolean();

        final ReadWriteLock lock = new ReentrantReadWriteLock();

        IgniteFuture fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @SuppressWarnings( {"BusyWait"})
                @Nullable @Override public Object call() throws Exception {
                    int cleanUps = 0;

                    while (!done.get()) {
                        lock.readLock().lock();

                        try {
                            q.add(1);

                            sizeCnt.incrementAndGet();
                        }
                        finally {
                            lock.readLock().unlock();
                        }

                        if (sizeCnt.get() > 100 && guard.compareAndSet(false, true)) {
                            lock.writeLock().lock();

                            try {
                                for (Integer i = q.poll(); i != null; i = q.poll())
                                    sizeCnt.decrementAndGet();

                                cleanUps++;

                                assert sizeCnt.get() == 0 : "Invalid count [cnt=" + sizeCnt.get() +
                                    ", size=" + q.size() + ", entries=" + q + ']';
                            }
                            finally {
                                lock.writeLock().unlock();

                                guard.set(false);
                            }
                        }
                    }

                    X.println("Cleanups count (per thread): " + cleanUps);

                    return null;
                }
            },
            100,
            "test-thread"
        );

        Thread.sleep(3 * 60 * 1000);

        done.set(true);

        fut1.get();
    }
}
