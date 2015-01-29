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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 *
 */
class CacheLockImpl<K, V> implements Lock {
    /** Gateway. */
    private final GridCacheGateway<K, V> gate;

    /** */
    private final GridCacheProjectionEx<K, V> delegate;

    /** Projection. */
    private final GridCacheProjectionImpl<K, V> prj;

    /** */
    private final Collection<? extends K> keys;

    /** */
    private int cntr;

    /** */
    private volatile Thread lockedThread;

    /**
     * @param gate Gate.
     * @param delegate Delegate.
     * @param prj Projection.
     * @param keys Keys.
     */
    CacheLockImpl(GridCacheGateway<K, V> gate, GridCacheProjectionEx<K, V> delegate, GridCacheProjectionImpl<K, V> prj,
        Collection<? extends K> keys) {
        this.gate = gate;
        this.delegate = delegate;
        this.prj = prj;
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override public void lock() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.lockAll(keys, 0);

            incrementLockCounter();
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e.getMessage(), e);
        }
        finally {
            gate.leave(prev);
        }
    }

    /**
     *
     */
    private void incrementLockCounter() {
        assert (lockedThread == null && cntr == 0) || (lockedThread == Thread.currentThread() && cntr > 0);

        cntr++;

        lockedThread = Thread.currentThread();
    }

    /** {@inheritDoc} */
    @Override public void lockInterruptibly() throws InterruptedException {
        tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            boolean res = delegate.lockAll(keys, -1);

            if (res)
                incrementLockCounter();

            return res;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e.getMessage(), e);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (Thread.interrupted())
            throw new InterruptedException();

        if (time <= 0)
            return tryLock();

        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            IgniteInternalFuture<Boolean> fut = delegate.lockAllAsync(keys, unit.toMillis(time));

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
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void unlock() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            if (lockedThread != Thread.currentThread()) {
                throw new IllegalStateException("Failed to unlock keys (did current thread acquire lock " +
                    "with this lock instance?).");
            }

            assert cntr > 0;

            cntr--;

            if (cntr == 0)
                lockedThread = null;

            delegate.unlockAll(keys);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e.getMessage(), e);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheLockImpl.class, this);
    }
}
