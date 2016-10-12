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

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.util.typedef.internal.CU.retryTopologySafe;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

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
                return new IgniteBiTuple<>();
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

    /** Upper bound of local counter. */
    private long upBound;

    /** Reserved bottom bound of local counter (included). */
    private long reservedBottomBound;

    /** Reserved upper bound of local counter (not included). */
    private long reservedUpBound;

    /** A limit after which a new reservation should be done. */
    private long newReservationLine;

    /** Whether reserveFuture already processed or not. */
    private boolean isReserveFutResultsProcessed = true;

    /** */
    private volatile int reservePercentage;

    /** Sequence batch size */
    private volatile int batchSize;

    /** Synchronization lock. */
    private final Lock lock = new ReentrantLock();

    /** Reservation future. */
    private IgniteInternalFuture<?> reservationFut = new GridFinishedFuture<>();

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
     * @param reservePercentage Reserve percentage.
     * @param locVal Local counter.
     * @param upBound Upper bound.
     */
    public GridCacheAtomicSequenceImpl(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicSequenceValue> seqView,
        GridCacheContext ctx,
        int batchSize,
        int reservePercentage,
        long locVal,
        long upBound) {
        assert key != null;
        assert seqView != null;
        assert ctx != null;
        assert batchSize > 0 : batchSize;
        assert locVal <= upBound;
        assert reservePercentage >= 0 && reservePercentage <= 100 : reservePercentage;

        this.batchSize = batchSize;
        this.ctx = ctx;
        this.key = key;
        this.seqView = seqView;
        this.upBound = upBound;
        this.locVal = locVal;
        this.name = name;
        this.reservePercentage = reservePercentage;

        newReservationLine = locVal + (batchSize * reservePercentage / 100);

        log = ctx.logger(getClass());
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
            return internalUpdate(1, true);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() {
        try {
            return internalUpdate(1, false);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) {
        A.ensure(l > 0, " Parameter mustn't be less then 1: " + l);

        try {
            return internalUpdate(l, true);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) {
        A.ensure(l > 0, " Parameter mustn't be less then 1: " + l);

        try {
            return internalUpdate(l, false);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Synchronous sequence update operation. Will add given amount to the sequence value.
     *
     * @param l Increment amount.
     * @param updated If {@code true}, will return sequence value after update, otherwise will return sequence value
     *      prior to update.
     * @return Sequence value.
     * @throws IgniteCheckedException If update failed.
     */
    @SuppressWarnings("SignalWithoutCorrespondingAwait")
    private long internalUpdate(final long l, final boolean updated) throws IgniteCheckedException {
        assert l > 0 : l;

        while (true) {
            checkRemoved();

            lock.lock();

            try {
                if (locVal + l >= newReservationLine && isReserveFutResultsProcessed && reservationFut.isDone())
                    reservationFut = runAsyncReservation(0);

                // If reserved range isn't exhausted.
                if (locVal + l < upBound) {
                    long curVal = locVal;

                    locVal += l;

                    return updated ? locVal : curVal;
                }

                if (!isReserveFutResultsProcessed && reservationFut.isDone()) {
                    isReserveFutResultsProcessed = true;

                    if (locVal + l < reservedUpBound) {
                        long curVal = locVal;

                        locVal = (locVal + l < reservedBottomBound) ? reservedBottomBound : locVal + l;

                        upBound = reservedUpBound;

                        return updated ? locVal : curVal;
                    }
                    else {
                        long diff = locVal + l - reservedUpBound;

                        long off = (diff / batchSize) * batchSize;

                        reservationFut = runAsyncReservation(off);
                    }
                }
            }
            finally {
                lock.unlock();
            }

            // If reserved range is exhausted.
            reservationFut.get();
        }
    }

    /**
     * Runs async reservation of new range for current node.
     *
     * @param off Offset.
     * @return Future.
     */
    private IgniteInternalFuture<?> runAsyncReservation(final long off) {
        assert off >= 0 : off;

        return ctx.kernalContext().closure().runLocalSafe(new Runnable() {
            @Override public void run() {
                Callable<Void> reserveCall = retryTopologySafe(new Callable<Void>() {
                    @Override public Void call() throws Exception {

                        // in some cases transaction can fail need restore previous state
                        long oldReservedBottomBound = reservedBottomBound;

                        long oldReservedUpBound = reservedUpBound;

                        long oldReservationLine = newReservationLine;

                        try (IgniteInternalTx tx = CU.txStartInternal(ctx, seqView, PESSIMISTIC, REPEATABLE_READ)) {
                            GridCacheAtomicSequenceValue seq = seqView.get(key);

                            checkRemoved();

                            assert seq != null;

                            lock.lock();

                            try {
                                assert isReserveFutResultsProcessed;

                                isReserveFutResultsProcessed = false;

                                long curGlobalVal = seq.get();

                                reservedBottomBound = curGlobalVal + off;

                                reservedUpBound = reservedBottomBound + batchSize;

                                newReservationLine = reservedBottomBound + (batchSize * reservePercentage / 100);
                            } finally {
                                lock.unlock();
                            }

                            seq.set(reservedUpBound);

                            seqView.put(key, seq);

                            tx.commit();
                        } catch (Error | Exception e) {
                            U.error(log, "Failed to get and add: " + this, e);

                            reservedBottomBound = oldReservedBottomBound;

                            reservedUpBound = oldReservedUpBound;

                            newReservationLine = oldReservationLine;

                            isReserveFutResultsProcessed = true;

                            throw e;

                        }


                        return null;
                    }
                });

                try {
                    CU.outTx(reserveCall, ctx);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }, /*sys pool*/ false);
    }

    /**
     * Get local batch size for this sequences.
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

    /** {@inheritDoc} */
    @Override public int reservePercentage() {
        return reservePercentage;
    }

    /** {@inheritDoc} */
    @Override public void reservePercentage(int percentage) {
        A.ensure(percentage >= 0 && percentage <= 100, "Invalid reserve percentage: " + percentage);

        lock.lock();

        try {
            this.reservePercentage = percentage;
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
