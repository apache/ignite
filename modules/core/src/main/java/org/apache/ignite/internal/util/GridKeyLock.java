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

package org.apache.ignite.internal.util;

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * This is an utility class for 'splitting' locking of some resources.
 * <p>
 * Map resources values to some number of locks,
 * and supply convenience methods to obtain and release these locks using key values.
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