// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

/**
 * This is an utility class for 'splitting' locking of some resources.
 * <p>
 * Map resources values to some number of locks,
 * and supply convenience methods to obtain and release these locks using key values.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridKeyLock {
    /** Underlying locks. */
    private final ConcurrentMap<Object, Sync> locks = new ConcurrentHashMap8<>();

    /**
     * @param key Key to lock.
     * @return Sync object that should be passed to {@link #unlock(Object, Object)} method.
     * @throws InterruptedException If interrupted while acquiring lock.
     */
    public <T> Object lockInterruptibly(T key) throws InterruptedException {
        assert key != null;

        Sync t = new Sync();

        while (true) {
            Sync old = locks.putIfAbsent(key, t);

            if (old != null)
                old.await();
            else
                return t;
        }
    }

    /**
     * @param key Key to lock.
     * @return Sync object that should be passed to {@link #unlock(Object, Object)} method.
     */
    public <T> Object lock(T key) {
        assert key != null;

        boolean interrupted = false;

        Sync t = new Sync();

        try {
            while (true) {
                Sync old = locks.putIfAbsent(key, t);

                if (old != null) {
                    while (true) {
                        try {
                            old.await();

                            break;
                        }
                        catch (InterruptedException ignored) {
                            interrupted = true;
                        }
                    }
                }
                else
                    return t;
            }
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * @param key Key to lock.
     * @return Sync object that should be passed to {@link #unlock(Object, Object)} method or
     *      {@code null} if try lock failed.
     */
    @Nullable public <T> Object tryLock(T key) {
        assert key != null;

        Sync t = new Sync();

        Sync old = locks.putIfAbsent(key, t);

        return old != null ? null : t;
    }

    /**
     * @param key Key.
     * @param sync Sync that got from {@link #lockInterruptibly(Object)} call.
     */
    public <T> void unlock(T key, Object sync) {
        if (!locks.remove(key, sync))
            throw new IllegalStateException("Lock has not been acquired for key: " + key);

        ((Sync)sync).finish();
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridKeyLock.class, this, "locksSize", locks.size());
    }

    /**
     *
     */
    private static class Sync {
        /** */
        private volatile boolean finished;

        /**
         * @throws InterruptedException If failed.
         */
        void await() throws InterruptedException {
            if (finished)
                return;

            synchronized (this) {
                while (!finished)
                    wait();
            }
        }

        /**
         *
         */
        synchronized void finish() {
            finished = true;

            notifyAll();
        }
    }
}
