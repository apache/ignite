/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Local storage used to keep partition {@code data}.
 */
class PartitionDataStorage implements AutoCloseable {
    /** Storage of a partition {@code data} with usage stat. */
    private final ConcurrentMap<Integer, ObjectWithUsageStat> storage = new ConcurrentHashMap<>();

    /** Storage of locks correspondent to partition {@code data} objects. */
    private final ConcurrentMap<Integer, Lock> locks = new ConcurrentHashMap<>();

    /** Schedured thread pool executor for cleaners. */
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

    /** Time-to-live in milliseconds (-1 for an infinite lifetime). */
    private final long ttl;

    /**
     * Constructs a new instance of partition data storage.
     *
     * @param ttl Time-to-live in seconds (-1 for an infinite lifetime).
     */
    public PartitionDataStorage(long ttl) {
       this.ttl = ttl * 1_000;
    }

    /**
     * Retrieves partition {@code data} correspondent to specified partition index if it exists in local storage or
     * loads it using the specified {@code supplier}. Unlike {@link ConcurrentMap#computeIfAbsent(Object, Function)},
     * this method guarantees that function will be called only once.
     *
     * @param <D> Type of data.
     * @param part Partition index.
     * @param supplier Partition {@code data} supplier.
     * @return Partition {@code data}.
     */
    <D> D computeDataIfAbsent(int part, Supplier<D> supplier) {
        ObjectWithUsageStat objWithStat = storage.get(part);

        if (objWithStat == null) {
            Lock lock = locks.computeIfAbsent(part, p -> new ReentrantLock());

            lock.lock();
            try {
                objWithStat = storage.get(part);
                if (objWithStat == null) {
                    objWithStat = new ObjectWithUsageStat(supplier.get());
                    storage.put(part, objWithStat);
                    if (ttl > -1)
                        executor.schedule(new Cleaner(part), ttl, TimeUnit.MILLISECONDS);
                }
            }
            finally {
                lock.unlock();
            }
        }

        objWithStat.updateLastAccessTime();

        return (D)objWithStat.data;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // We assume that after close all objects stored in this storage will be collected by GC.
        executor.shutdownNow();
    }

    /**
     * Cleaner that removes partition data.
     */
    private class Cleaner implements Runnable  {
        /** Partition number. */
        private final int part;

        /**
         * Constructs a new instance of cleaner.
         *
         * @param part Partition number.
         */
        public Cleaner(int part) {
            this.part = part;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ObjectWithUsageStat objWithStat = storage.get(part);

            if (objWithStat.isExpired()) {
                removeFromStorage();
                objWithStat.close();
            }
            else
                reschedule(objWithStat.lastAccessTime);
        }

        /**
         * Removes partition data and correspondent lock from storage.
         */
        private void removeFromStorage() {
            storage.remove(part);
            locks.remove(part);
        }

        /**
         * Reschedules this cleaner using the last access time as a reference point. The next attepmt to clean up will
         * be {@link #ttl} milliseconds after the last access.
         *
         * @param lastAccessTime Last access time in milliseconds (difference between the last access time and midnight,
         *                       January 1, 1970 UTC).
         */
        private void reschedule(long lastAccessTime) {
            long delay = lastAccessTime - System.currentTimeMillis() + ttl;
            executor.schedule(this, Math.max(0, delay), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Util container that keeps an object and the last access time to it. Allows to check if the object is already
     * expired and to clean it up.
     */
    private class ObjectWithUsageStat implements AutoCloseable {
        /** Data object. */
        private final Object data;

        /** Last access time in milliseconds (see {@link System#currentTimeMillis()}). */
        private volatile long lastAccessTime;

        /**
         * Constructs a new instance of object with usage stat.
         *
         * @param data Data object.
         */
        ObjectWithUsageStat(Object data) {
            this.data = data;
        }

        /**
         * Updates last access time.
         */
        void updateLastAccessTime() {
            lastAccessTime = System.currentTimeMillis();
        }

        /**
         * Checks if the object is already expired. In ohter words, checks if the last access to the object occured more
         * than {@link #ttl} milliseconds ago.
         *
         * @return {@code true} if object is already expired, otherwise {@code false}.
         */
        boolean isExpired() {
            return lastAccessTime + ttl <= System.currentTimeMillis();
        }

        /** {@inheritDoc} */
        @Override public void close() {
            if (data instanceof AutoCloseable) {
                AutoCloseable closeableData = (AutoCloseable)data;
                try {
                    closeableData.close();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
