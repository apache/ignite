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

package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;
import static org.apache.ignite.internal.util.typedef.internal.CU.*;

/**
 * Cache sequence implementation.
 */
public final class GridCacheAtomicSequenceImpl implements GridCacheAtomicSequenceEx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** De-serialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return F.t2();
            }
        };

    /** Logger. */
    private IgniteLogger log;

    /** Sequence name. */
    private String name;

    /** Removed flag. */
    private volatile boolean rmvd;

    /** Check removed flag. */
    private boolean rmvCheck;

    /** Sequence key. */
    private GridCacheInternalKey key;

    /** Sequence projection. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicSequenceValue> seqView;

    /** Cache context. */
    private volatile GridCacheContext ctx;

    /** Local value of sequence. */
    private long locVal;

    /**  Upper bound of local counter. */
    private long upBound;

    /**  Sequence batch size */
    private volatile int batchSize;

    /** Synchronization lock. */
    private final Lock lock = new ReentrantLock();

    /** Await condition. */
    private Condition cond = lock.newCondition();

    /** Callable for execution {@link #incrementAndGet} operation in async and sync mode.  */
    private final Callable<Long> incAndGetCall = internalUpdate(1, true);

    /** Callable for execution {@link #getAndIncrement} operation in async and sync mode.  */
    private final Callable<Long> getAndIncCall = internalUpdate(1, false);

    /** Add and get cache call guard. */
    private final AtomicBoolean updateGuard = new AtomicBoolean();

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheAtomicSequenceImpl() {
        // No-op.
    }

    /**
     * Default constructor.
     *
     * @param name Sequence name.
     * @param key Sequence key.
     * @param seqView Sequence projection.
     * @param ctx CacheContext.
     * @param batchSize Sequence batch size.
     * @param locVal Local counter.
     * @param upBound Upper bound.
     */
    public GridCacheAtomicSequenceImpl(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicSequenceValue> seqView,
        GridCacheContext ctx,
        int batchSize,
        long locVal,
        long upBound)
    {
        assert key != null;
        assert seqView != null;
        assert ctx != null;
        assert locVal <= upBound;

        this.batchSize = batchSize;
        this.ctx = ctx;
        this.key = key;
        this.seqView = seqView;
        this.upBound = upBound;
        this.locVal = locVal;
        this.name = name;

        log = ctx.gridConfig().getGridLogger().getLogger(getClass());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long get() {
        checkRemoved();

        lock.lock();

        try {
            return locVal;
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long incrementAndGet() {
        try {
            return internalUpdate(1, incAndGetCall, true);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() {
        try {
            return internalUpdate(1, getAndIncCall, false);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) {
        A.ensure(l > 0, " Parameter mustn't be less then 1: " + l);

        try {
            return internalUpdate(l, null, true);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) {
        A.ensure(l > 0, " Parameter mustn't be less then 1: " + l);

        try {
            return internalUpdate(l, null, false);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Synchronous sequence update operation. Will add given amount to the sequence value.
     *
     * @param l Increment amount.
     * @param updateCall Cache call that will update sequence reservation count in accordance with l.
     * @param updated If {@code true}, will return sequence value after update, otherwise will return sequence value
     *      prior to update.
     * @return Sequence value.
     * @throws IgniteCheckedException If update failed.
     */
    @SuppressWarnings("SignalWithoutCorrespondingAwait")
    private long internalUpdate(long l, @Nullable Callable<Long> updateCall, boolean updated) throws IgniteCheckedException {
        checkRemoved();

        assert l > 0;

        lock.lock();

        try {
            // If reserved range isn't exhausted.
            if (locVal + l <= upBound) {
                long curVal = locVal;

                locVal += l;

                return updated ? locVal : curVal;
            }
        }
        finally {
            lock.unlock();
        }

        if (updateCall == null)
            updateCall = internalUpdate(l, updated);

        while (true) {
            if (updateGuard.compareAndSet(false, true)) {
                try {
                    // This call must be outside lock.
                    return CU.outTx(updateCall, ctx);
                }
                finally {
                    lock.lock();

                    try {
                        updateGuard.set(false);

                        cond.signalAll();
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
            else {
                lock.lock();

                try {
                    while (locVal >= upBound && updateGuard.get())
                        U.await(cond, 500, MILLISECONDS);

                    checkRemoved();

                    // If reserved range isn't exhausted.
                    if (locVal + l <= upBound) {
                        long curVal = locVal;

                        locVal += l;

                        return updated ? locVal : curVal;
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * Asynchronous sequence update operation. Will add given amount to the sequence value.
     *
     * @param l Increment amount.
     * @param updateCall Cache call that will update sequence reservation count in accordance with l.
     * @param updated If {@code true}, will return sequence value after update, otherwise will return sequence value
     *      prior to update.
     * @return Future indicating sequence value.
     * @throws IgniteCheckedException If update failed.
     */
    @SuppressWarnings("SignalWithoutCorrespondingAwait")
    private IgniteInternalFuture<Long> internalUpdateAsync(long l, @Nullable Callable<Long> updateCall, boolean updated)
        throws IgniteCheckedException {
        checkRemoved();

        A.ensure(l > 0, " Parameter mustn't be less then 1: " + l);

        lock.lock();

        try {
            // If reserved range isn't exhausted.
            if (locVal + l <= upBound) {
                long curVal = locVal;

                locVal += l;

                return new GridFinishedFuture<>(updated ? locVal : curVal);
            }
        }
        finally {
            lock.unlock();
        }

        if (updateCall == null)
            updateCall = internalUpdate(l, updated);

        while (true) {
            if (updateGuard.compareAndSet(false, true)) {
                try {
                    // This call must be outside lock.
                    return ctx.closures().callLocalSafe(updateCall, true);
                }
                finally {
                    lock.lock();

                    try {
                        updateGuard.set(false);

                        cond.signalAll();
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
            else {
                lock.lock();

                try {
                    while (locVal >= upBound && updateGuard.get())
                        U.await(cond, 500, MILLISECONDS);

                    checkRemoved();

                    // If reserved range isn't exhausted.
                    if (locVal + l <= upBound) {
                        long curVal = locVal;

                        locVal += l;

                        return new GridFinishedFuture<>(updated ? locVal : curVal);
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }

    /** Get local batch size for this sequences.
     *
     * @return Sequence batch size.
     */
    @Override public int batchSize() {
        return batchSize;
    }

    /**
     * Set local batch size for this sequences.
     *
     * @param size Sequence batch size. Must be more then 0.
     */
    @Override public void batchSize(int size) {
        A.ensure(size > 0, " Batch size can't be less then 0: " + size);

        lock.lock();

        try {
            batchSize = size;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Check removed status.
     *
     * @throws IllegalStateException If removed.
     */
    private void checkRemoved() throws IllegalStateException {
        if (rmvd)
            throw removedError();

        if (rmvCheck) {
            try {
                rmvd = seqView.get(key) == null;
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            rmvCheck = false;

            if (rmvd) {
                ctx.kernalContext().dataStructures().onRemoved(key, this);

                throw removedError();
            }
        }
    }

    /**
     * @return Error.
     */
    private IllegalStateException removedError() {
        return new IllegalStateException("Sequence was removed from cache: " + name);
    }

    /** {@inheritDoc} */
    @Override public boolean onRemoved() {
        return rmvd = true;
    }

    /** {@inheritDoc} */
    @Override public void needCheckNotRemoved() {
        rmvCheck = true;
    }

    /** {@inheritDoc} */
    @Override public GridCacheInternalKey key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return rmvd;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            if (rmvd)
                return;

            ctx.kernalContext().dataStructures().removeSequence(name);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Method returns callable for execution all update operations in async and sync mode.
     *
     * @param l Value will be added to sequence.
     * @param updated If {@code true}, will return updated value, if {@code false}, will return previous value.
     * @return Callable for execution in async and sync mode.
     */
    @SuppressWarnings("TooBroadScope")
    private Callable<Long> internalUpdate(final long l, final boolean updated) {
        return retryTopologySafe(new Callable<Long>() {
            @Override public Long call() throws Exception {
                try (IgniteInternalTx tx = CU.txStartInternal(ctx, seqView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicSequenceValue seq = seqView.get(key);

                    checkRemoved();

                    assert seq != null;

                    long curLocVal;

                    long newUpBound;

                    lock.lock();

                    try {
                        curLocVal = locVal;

                        // If local range was already reserved in another thread.
                        if (locVal + l <= upBound) {
                            long retVal = locVal;

                            locVal += l;

                            return updated ? locVal : retVal;
                        }

                        long curGlobalVal = seq.get();

                        long newLocVal;

                        /* We should use offset because we already reserved left side of range.*/
                        long off = batchSize > 1 ? batchSize - 1 : 1;

                        // Calculate new values for local counter, global counter and upper bound.
                        if (curLocVal + l >= curGlobalVal) {
                            newLocVal = curLocVal + l;

                            newUpBound = newLocVal + off;
                        }
                        else {
                            newLocVal = curGlobalVal;

                            newUpBound = newLocVal + off;
                        }

                        locVal = newLocVal;
                        upBound = newUpBound;

                        if (updated)
                            curLocVal = newLocVal;
                    }
                    finally {
                        lock.unlock();
                    }

                    // Global counter must be more than reserved upper bound.
                    seq.set(newUpBound + 1);

                    seqView.put(key, seq);

                    tx.commit();

                    return curLocVal;
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to get and add: " + this, e);

                    throw e;
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx.kernalContext());
        out.writeUTF(name);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<GridKernalContext, String> t = stash.get();

        t.set1((GridKernalContext)in.readObject());
        t.set2(in.readUTF());
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<GridKernalContext, String> t = stash.get();

            return t.get1().dataStructures().sequence(t.get2(), 0L, false);
        }
        catch (IgniteCheckedException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAtomicSequenceImpl.class, this);
    }
}
