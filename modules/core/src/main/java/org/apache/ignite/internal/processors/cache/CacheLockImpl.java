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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 *
 */
class CacheLockImpl<K> implements Lock {
    /** */
    private final GridCacheProjectionEx<K, ?> delegate;

    /** */
    private final Collection<? extends K> keys;

    /** */
    private int cntr;

    /** */
    private volatile Thread lockedThread;

    /**
     * @param delegate Delegate.
     * @param keys Keys.
     */
    CacheLockImpl(GridCacheProjectionEx<K, ?> delegate, Collection<? extends K> keys) {
        this.delegate = delegate;
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override public void lock() {
        try {
            delegate.lockAll(keys, 0);

            incrementLockCounter();
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e.getMessage(), e);
        }
    }

    /**
     *
     */
    private void incrementLockCounter() {
        assert (lockedThread == null && cntr == 0) || (lockedThread == Thread.currentThread() && cntr > 0);

        lockedThread = Thread.currentThread();

        cntr++;
    }

    /** {@inheritDoc} */
    @Override public void lockInterruptibly() throws InterruptedException {
        tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock() {
        try {
            boolean res = delegate.lockAll(keys, -1);

            if (res)
                incrementLockCounter();

            return res;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (Thread.interrupted())
            throw new InterruptedException();

        if (time <= 0)
            return tryLock();

        try {
            IgniteFuture<Boolean> fut = delegate.lockAllAsync(keys, unit.toMillis(time));

            try {
                boolean res = fut.get();

                if (res)
                    incrementLockCounter();

                return res;
            }
            catch (IgniteInterruptedException e) {
                if (!fut.cancel()) {
                    if (fut.isDone()) {
                        Boolean res = fut.get();

                        Thread.currentThread().interrupt();

                        if (res)
                            incrementLockCounter();

                        return res;
                    }
                }

                if (e.getCause() instanceof InterruptedException)
                    throw (InterruptedException)e.getCause();

                throw new InterruptedException();
            }
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e.getMessage(), e);
        }
    }

    /**
     *
     */
    private boolean isKeysLocked() {
        for (K key : keys) {
            if (delegate.isLocked(key))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void unlock() {
        try {
            Thread lockedThread = this.lockedThread;

            if (lockedThread != Thread.currentThread()) {
                if (lockedThread == null) {
                    if (isKeysLocked()) {
                        throw new IllegalStateException("Failed to unlock keys, looks like lock has been obtain on " +
                            "another instance of Lock, that was returned by IgniteCache.lock(key). You have to call " +
                            "lock() and unlock() methods on the same instance of Lock [keys=" + keys + ']');
                    }
                } else {
                    throw new IllegalStateException("Failed to unlock cache keys, keys are locked by another thread " +
                        "any threads [keys=" + keys + ", lockOwnerThread=" + lockedThread.getName() + ']');
                }
            }

            assert cntr > 0;

            cntr--;

            if (cntr == 0)
                this.lockedThread = null;

            delegate.unlockAll(keys);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
