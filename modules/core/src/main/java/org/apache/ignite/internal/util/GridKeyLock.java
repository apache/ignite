/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util;

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is an utility class for 'splitting' locking of some resources.
 * <p>
 * Map resources values to some number of locks,
 * and supply convenience methods to obtain and release these locks using key values.
 */
public class GridKeyLock {
    /** Underlying locks. */
    private final ConcurrentMap<Object, Sync> locks = new ConcurrentHashMap<>();

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