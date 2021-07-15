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

package org.apache.ignite.internal.tx.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.Waiter;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link LockManager} implementation which stores lock queues in the heap.
 *
 * Lock waiters are placed in the queue, ordered from oldest to yongest (highest Timestamp).
 * When a new waiter is placed in the queue, it's validated against current lock owner: if where is an owner with a
 * higher timestamp lock request is denied.
 */
public class HeapLockManager implements LockManager {
    /** */
    private ConcurrentHashMap<Object, LockState> locks = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> tryAcquire(Object key, Timestamp timestamp) throws LockException {
        return lockState(key).tryAcquire(timestamp);
    }

    /** {@inheritDoc} */
    @Override public void tryRelease(Object key, Timestamp timestamp) throws LockException {
        lockState(key).tryRelease(timestamp);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> tryAcquireShared(Object key, Timestamp timestamp) throws LockException {
        return lockState(key).tryAcquireShared(timestamp);
    }

    /** {@inheritDoc} */
    @Override public void tryReleaseShared(Object key, Timestamp timestamp) throws LockException {
        lockState(key).tryReleaseShared(timestamp);
    }

    /**
     * @param key The key.
     * @return The lock state for the key.
     */
    private @NotNull LockState lockState(Object key) {
        return locks.computeIfAbsent(key, k -> new LockState());
    }

    /** {@inheritDoc} */
    @Override public Collection<Timestamp> queue(Object key) {
        return lockState(key).queue();
    }

    @Override public Waiter waiter(Object key, Timestamp timestamp) {
        return lockState(key).waiter(timestamp);
    }

    /** A lock state. */
    private static class LockState {
        /** Waiters. */
        private TreeMap<Timestamp, WaiterImpl> waiters = new TreeMap<>();

        /**
         * @param timestamp The timestamp.
         * @return The future.
         */
        public CompletableFuture<Void> tryAcquire(Timestamp timestamp) throws LockException {
            WaiterImpl w = new WaiterImpl(timestamp, false);

            synchronized (waiters) {
                waiters.put(timestamp, w);

                // Check lock compatibility.
                Map.Entry<Timestamp, WaiterImpl> nextEntry = waiters.higherEntry(timestamp);

                // If we have a younger waiter in a locked state, when refuse to wait for lock.
                if (nextEntry != null && nextEntry.getValue().locked()) {
                    waiters.remove(timestamp);

                    throw new LockException(nextEntry.getValue());
                }

                // Lock if oldest.
                if (waiters.firstKey() == timestamp)
                    w.lock();
            }

            // Notify outside the monitor.
            if (w.locked())
                w.notifyLocked();

            return w.fut;
        }

        /**
         * @param timestamp The timestamp.
         */
        public void tryRelease(Timestamp timestamp) throws LockException {
            Collection<WaiterImpl> locked = new ArrayList<>();

            synchronized (waiters) {
                Map.Entry<Timestamp, WaiterImpl> first = waiters.firstEntry();

                if (first == null ||
                    !first.getKey().equals(timestamp) ||
                    !first.getValue().locked() ||
                    first.getValue().isForRead())
                    throw new LockException("Not exclusively locked by " + timestamp);

                waiters.pollFirstEntry();

                if (waiters.isEmpty())
                    return;

                // Lock next waiter(s).
                WaiterImpl waiter = waiters.firstEntry().getValue();

                if (!waiter.isForRead()) {
                    waiter.lock();

                    locked.add(waiter);
                }
                else {
                    // Grant lock to all adjasent readers.
                    for (Map.Entry<Timestamp, WaiterImpl> entry : waiters.entrySet()) {
                        if (!entry.getValue().isForRead())
                            break;
                        else {
                            entry.getValue().lock();

                            locked.add(entry.getValue());
                        }
                    }
                }
            }

            // Notify outside the monitor.
            for (WaiterImpl waiter : locked) {
                waiter.notifyLocked();
            }
        }

        /**
         * @param timestamp The timestamp.
         * @return The future.
         */
        public CompletableFuture<Void> tryAcquireShared(Timestamp timestamp) throws LockException {
            WaiterImpl waiter = new WaiterImpl(timestamp, true);

            // Grant a lock to the oldest waiter.
            synchronized (waiters) {
                waiters.put(timestamp, waiter);

                // Check lock compatibility.
                Map.Entry<Timestamp, WaiterImpl> nextEntry = waiters.higherEntry(timestamp);

                if (nextEntry != null) {
                    WaiterImpl nextWaiter = nextEntry.getValue();

                    if (nextWaiter.locked() && !nextWaiter.isForRead()) {
                        waiters.remove(timestamp);

                        throw new LockException(nextWaiter);
                    }
                }

                Map.Entry<Timestamp, WaiterImpl> prevEntry = waiters.lowerEntry(timestamp);

                if (prevEntry == null || prevEntry.getValue().isForRead())
                    waiter.lock();
            }

            // Notify outside the monitor.
            if (waiter.locked())
                waiter.notifyLocked();

            return waiter.fut;
        }

        public void tryReleaseShared(Timestamp timestamp) throws LockException {
            WaiterImpl locked = null;

            synchronized (waiters) {
                WaiterImpl waiter = waiters.get(timestamp);

                if (waiter == null || !waiter.locked() || !waiter.isForRead())
                    throw new LockException("Not shared locked by " + timestamp);

                Map.Entry<Timestamp, WaiterImpl> nextEntry = waiters.higherEntry(timestamp);

                waiters.remove(timestamp);

                // Queue is empty.
                if (nextEntry == null)
                    return;

                // Lock next exclusive waiter.
                WaiterImpl nextWaiter = nextEntry.getValue();

                if (!nextWaiter.isForRead() && nextWaiter.timestamp().equals(waiters.firstEntry().getKey())) {
                    nextWaiter.lock();

                    locked = nextWaiter;
                }
            }

            if (locked != null)
                locked.notifyLocked();
        }

        /**
         * @return Waiters queue.
         */
        public Collection<Timestamp> queue() {
            synchronized (waiters) {
                return new ArrayList<>(waiters.keySet());
            }
        }

        /**
         * @param timestamp The timestamp.
         * @return The waiter.
         */
        public Waiter waiter(Timestamp timestamp) {
            synchronized (waiters) {
                return waiters.get(timestamp);
            }
        }
    }

    /**
     * The waiter implementation.
     */
    private static class WaiterImpl implements Comparable<WaiterImpl>, Waiter {
        /** Locked future. */
        @IgniteToStringExclude
        private final CompletableFuture<Void> fut;

        /** Waiter timestamp. */
        private final Timestamp timestamp;

        /** {@code True} if a read request. */
        private boolean forRead;

        /** The state. */
        private boolean locked = false;

        /**
         * @param timestamp The timestamp.
         * @param forRead {@code True} to request a read lock.
         */
        WaiterImpl(Timestamp timestamp, boolean forRead) {
            this.fut = new CompletableFuture<>();
            this.timestamp = timestamp;
            this.forRead = forRead;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull WaiterImpl o) {
            return timestamp.compareTo(o.timestamp);
        }

        /**
         * Notifies a future listeners.
         */
        private void notifyLocked() {
            fut.complete(null);
        }

        /** {@inheritDoc} */
        @Override public boolean locked() {
            return this.locked;
        }

        /**
         * Grant a lock.
         */
        private void lock() {
            locked = true;
        }

        /** {@inheritDoc} */
        @Override public Timestamp timestamp() {
            return timestamp;
        }

        /**
         * @return {@code True} if is locked for read.
         */
        public boolean isForRead() {
            return forRead;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (!(o instanceof WaiterImpl)) return false;

            return compareTo((WaiterImpl) o) == 0;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return timestamp.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(WaiterImpl.class, this, "isDone", fut.isDone());
        }
    }
}
