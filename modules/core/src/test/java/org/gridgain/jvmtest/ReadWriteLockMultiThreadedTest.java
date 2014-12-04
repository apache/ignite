/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jvmtest;

import junit.framework.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * JDK read write lock test.
 */
public class ReadWriteLockMultiThreadedTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void testReadThenWriteLockAcquire() throws Exception {
        ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().lock();

        lock.writeLock().lock();
    }

    /**
     *
     */
    public void testNotOwnedLockRelease() {
        ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().unlock();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void testWriteLockAcquire() throws Exception {
        final ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().lock();

        X.println("Read lock acquired.");

        IgniteFuture fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    X.println("Attempting to acquire write lock: " + lock);

                    lock.writeLock().lock();

                    try {
                        X.println("Write lock acquired: " + lock);

                        return null;
                    }
                    finally {
                        lock.writeLock().unlock();
                    }
                }
            },
            1,
            "write-lock"
        );

        Thread.sleep(2000);

        IgniteFuture fut2 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    X.println("Attempting to acquire read lock: " + lock);

                    lock.readLock().lock();

                    try {
                        X.println("Read lock acquired: " + lock);

                        return null;
                    }
                    finally {
                        lock.readLock().unlock();
                    }
                }
            },
            1,
            "read-lock"
        );

        Thread.sleep(2000);

        X.println(">>> Dump threads now! <<<");

        Thread.sleep(15 * 1000);

        X.println("Read lock released: " + lock);

        lock.readLock().unlock();

        fut1.get();
        fut2.get();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void testReadLockAcquire() throws Exception {
        final ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.writeLock().lock();

        X.println("Write lock acquired: " + lock);

        IgniteFuture fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    X.println("Attempting to acquire read lock: " + lock);

                    lock.readLock().lock();

                    try {
                        X.println("Read lock acquired: " + lock);

                        return null;
                    }
                    finally {
                        lock.readLock().unlock();
                    }
                }
            },
            1,
            "read-lock"
        );

        Thread.sleep(2000);

        X.println(">>> Dump threads now! <<<");

        Thread.sleep(15 * 1000);

        X.println("Write lock released.");

        lock.writeLock().unlock();

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void testTryWriteLock() throws Exception {
        final ReadWriteLock lock = new ReentrantReadWriteLock();

        lock.readLock().lock();

        X.println("Read lock acquired.");

        IgniteFuture fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    boolean res = lock.writeLock().tryLock();

                    X.println("Attempting to try write lock: " + res);

                    try {
                        return null;
                    }
                    finally {
                        if (res)
                            lock.writeLock().unlock();
                    }
                }
            },
            1,
            "write-lock"
        );

        Thread.sleep(2000);

        X.println("Read lock released: " + lock);

        lock.readLock().unlock();

        fut.get();
    }
}
