/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util.concurrent;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A {@link ReadWriteLock} that can report some info when the lock is held by a thread very long time.
 */
public abstract class LongHeldDetectingReadWriteLock implements ReadWriteLock {
    public enum AcquireMode {
        Read, Write
    }

    private final Lock rLock;
    private final Lock wLock;

    public LongHeldDetectingReadWriteLock(long maxBlockingTimeToReport, TimeUnit unit) {
        this(false, maxBlockingTimeToReport, unit);
    }

    public LongHeldDetectingReadWriteLock(boolean fair, long maxBlockingTimeToReport, TimeUnit unit) {
        final RwLock rwLock = new RwLock(fair);
        final long maxBlockingNanos = unit.toNanos(maxBlockingTimeToReport);
        if (maxBlockingNanos > 0) {
            this.rLock = new LongHeldDetectingLock(AcquireMode.Read, rwLock, maxBlockingNanos);
            this.wLock = new LongHeldDetectingLock(AcquireMode.Write, rwLock, maxBlockingNanos);
        }
        else {
            this.rLock = rwLock.readLock();
            this.wLock = rwLock.writeLock();
        }
    }

    @Override
    public Lock readLock() {
        return this.rLock;
    }

    @Override
    public Lock writeLock() {
        return this.wLock;
    }

    public abstract void report(final AcquireMode acquireMode, final Thread heldThread,
        final Collection<Thread> queuedThreads, final long blockedNanos);

    static class RwLock extends ReentrantReadWriteLock {
        private static final long serialVersionUID = -1783358548846940445L;

        RwLock(boolean fair) {
            super(fair);
        }

        @Override
        public Thread getOwner() {
            return super.getOwner();
        }

        @Override
        public Collection<Thread> getQueuedThreads() {
            return super.getQueuedThreads();
        }
    }

    class LongHeldDetectingLock implements Lock {

        private final AcquireMode mode;
        private final RwLock parent;
        private final Lock delegate;
        private final long maxBlockingNanos;

        LongHeldDetectingLock(AcquireMode mode, RwLock parent, long maxBlockingNanos) {
            this.mode = mode;
            this.parent = parent;
            this.delegate = mode == AcquireMode.Read ? parent.readLock() : parent.writeLock();
            this.maxBlockingNanos = maxBlockingNanos;
        }

        @Override
        public void lock() {
            final long start = System.nanoTime();
            final Thread owner = this.parent.getOwner();
            try {
                this.delegate.lock();
            }
            finally {
                final long elapsed = System.nanoTime() - start;
                if (elapsed > this.maxBlockingNanos) {
                    report(this.mode, owner, this.parent.getQueuedThreads(), elapsed);
                }
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            final long start = System.nanoTime();
            final Thread owner = this.parent.getOwner();
            try {
                this.delegate.lockInterruptibly();
            }
            finally {
                final long elapsed = System.nanoTime() - start;
                if (elapsed > this.maxBlockingNanos) {
                    report(this.mode, owner, this.parent.getQueuedThreads(), elapsed);
                }
            }
        }

        @Override
        public boolean tryLock() {
            return this.delegate.tryLock();
        }

        @Override
        public boolean tryLock(final long time, final TimeUnit unit) throws InterruptedException {
            return this.delegate.tryLock(time, unit);
        }

        @Override
        public void unlock() {
            this.delegate.unlock();
        }

        @Override
        public Condition newCondition() {
            return this.delegate.newCondition();
        }
    }
}
