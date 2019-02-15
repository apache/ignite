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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
class CacheLockImpl<K, V> implements Lock {
    /** Gateway. */
    private final GridCacheGateway<K, V> gate;

    /** */
    private final IgniteInternalCache<K, V> delegate;

    /** Operation context. */
    private final CacheOperationContext opCtx;

    /** */
    private final Collection<? extends K> keys;

    /** */
    private int cntr;

    /** */
    private volatile Thread lockedThread;

    /**
     * @param gate Gate.
     * @param delegate Delegate.
     * @param opCtx Operation context.
     * @param keys Keys.
     */
    CacheLockImpl(GridCacheGateway<K, V> gate, IgniteInternalCache<K, V> delegate, CacheOperationContext opCtx,
        Collection<? extends K> keys) {
        this.gate = gate;
        this.delegate = delegate;
        this.opCtx = opCtx;
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override public void lock() {
        CacheOperationContext prev = gate.enter(opCtx);

        try {
            checkTx();

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
        CacheOperationContext prev = gate.enter(opCtx);

        try {
            checkTx();

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

        CacheOperationContext prev = gate.enter(opCtx);

        try {
            checkTx();

            IgniteInternalFuture<Boolean> fut = delegate.lockAllAsync(keys, unit.toMillis(time));

            try {
                boolean res = fut.get();

                if (res)
                    incrementLockCounter();

                return res;
            }
            catch (IgniteInterruptedCheckedException e) {
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
        CacheOperationContext prev = gate.enter(opCtx);

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

    /**
     * Verifies there is no ongoing user transaction.
     *
     * @throws CacheException
     */
    private void checkTx() throws CacheException {
        if (delegate.context().tm().inUserTx())
            throw new CacheException("Explicit lock can't be acquired within a transaction.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheLockImpl.class, this);
    }
}
