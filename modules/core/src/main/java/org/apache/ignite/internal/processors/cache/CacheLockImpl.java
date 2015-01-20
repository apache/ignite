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
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 *
 * @param <K>
 */
class CacheLockImpl<K> implements CacheLock {
    /** */
    private final GridCacheProjectionEx<K, ?> delegate;

    /** */
    private final Collection<? extends K> keys;

    /** */
    private volatile CacheLockAsyncImpl<K> async;

    /**
     * @param delegate Delegate.
     * @param keys Keys.
     */
    CacheLockImpl(GridCacheProjectionEx<K, ?> delegate, Collection<? extends K> keys) {
        this.delegate = delegate;
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked() {
        for (K key : keys) {
            if (!delegate.isLocked(key))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread() {
        for (K key : keys) {
            if (!delegate.isLockedByThread(key))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void lock() {
        try {
            delegate.lockAll(keys, 0);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void lockInterruptibly() throws InterruptedException {
        tryLock(-1, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock() {
        try {
            return delegate.lockAll(keys, -1);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (Thread.interrupted())
            throw new InterruptedException();

        try {
            if (time <= 0)
                return delegate.lockAll(keys, -1);

            IgniteFuture<Boolean> fut = null;

            try {
                fut = delegate.lockAllAsync(keys, time <= 0 ? -1 : unit.toMillis(time));

                return fut.get();
            }
            catch (GridInterruptedException e) {
                if (fut != null) {
                    if (!fut.cancel()) {
                        if (fut.isDone()) {
                            Boolean res = fut.get();

                            Thread.currentThread().interrupt();

                            return res;
                        }
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

    /** {@inheritDoc} */
    @Override public void unlock() {
        try {
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

    /** {@inheritDoc} */
    @Override public CacheLock enableAsync() {
        CacheLockAsyncImpl<K> async = this.async;

        if (async == null) {
            async = new CacheLockAsyncImpl<>(delegate, keys);

            this.async = async;
        }

        return async;
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        throw new IllegalStateException("Asynchronous mode is not enabled.");
    }
}
