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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgnitePartitionCatchUpLog;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

import static java.util.Optional.of;

/**
 *
 */
public class InMemoryPartitionCatchUpLog implements IgnitePartitionCatchUpLog {
    /** */
    private final CacheGroupContext grp;

    /** The absolute WAL pointer per temp WAL partition storage. */
    private final AtomicLong savedPtr = new AtomicLong();

    /** */
    private final Queue<IgniteBiTuple<WALPointer, WALRecord>> savedRecs = new ConcurrentLinkedQueue<>();

    /** */
    private volatile boolean cathed;

    /** */
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * @param grp The cache group.
     */
    public InMemoryPartitionCatchUpLog(CacheGroupContext grp) {
        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override public WALPointer log(WALRecord entry) throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            QueueWALPointer ptr = new QueueWALPointer(savedPtr.incrementAndGet());

            if (cathed)
                ((GridCacheDatabaseSharedManager)grp.shared().database()).applyWALRecord(entry, (e) -> true);
            else
                savedRecs.add(new IgniteBiTuple<>(ptr, entry));

            return ptr;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public WALIterator replay() throws IgniteCheckedException {
        return new CatchUpWALIterator();
    }

    /** {@inheritDoc} */
    @Override public boolean catched() {
        return cathed;
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            savedRecs.clear();

            savedPtr.set(0);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     *
     */
    private class CatchUpWALIterator
        extends GridCloseableIteratorAdapterEx<IgniteBiTuple<WALPointer, WALRecord>>
        implements WALIterator {
        /** */
        private IgniteBiTuple<WALPointer, WALRecord> lastRead;

        /** {@inheritDoc} */
        @Override public Optional<WALPointer> lastRead() {
            return of(lastRead.get1());
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<WALPointer, WALRecord> onNext() throws IgniteCheckedException {
            return lastRead;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            assert ((QueueWALPointer)lastRead.get1()).index() <= savedPtr.get();

            IgniteBiTuple<WALPointer, WALRecord> rec;

            rec = savedRecs.poll();

            if (rec == null) {
                if (lock.isWriteLockedByCurrentThread()) {
                    cathed = true;

                    lock.writeLock().unlock();
                }
                else {
                    lock.writeLock().tryLock();

                    rec = savedRecs.poll();

                    if (rec == null) {
                        cathed = true;

                        lock.writeLock().unlock();
                    }
                    else
                        lastRead = rec;
                }
            }
            else
                lastRead = rec;

            return !cathed;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            if (lock.isWriteLockedByCurrentThread())
                lock.writeLock().unlock();
        }
    }

    /**
     *
     */
    private static class QueueWALPointer implements WALPointer, Comparable<WALPointer> {
        /** The index of particular pointer. */
        private final long idx;

        /**
         * @param idx Absoule index.
         */
        public QueueWALPointer(long idx) {
            this.idx = idx;
        }

        /**
         * @return The index of associated pointer.
         */
        public long index() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override public WALPointer next() {
            return new QueueWALPointer(idx + 1);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull WALPointer o) {
            QueueWALPointer pointer = (QueueWALPointer)o;

            return Long.compare(idx, pointer.index());
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueueWALPointer pointer = (QueueWALPointer)o;

            return idx == pointer.idx;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(idx);
        }
    }
}
