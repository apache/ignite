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

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache sequence implementation.
 */
public final class GridCacheAtomicSequenceImpl extends AtomicDataStructureProxy<GridCacheAtomicSequenceValue>
    implements GridCacheAtomicSequenceEx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** De-serialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return new IgniteBiTuple<>();
            }
        };

    /** Local value of sequence. */
    @GridToStringInclude(sensitive = true)
    private volatile long locVal;

    /** Upper bound of local counter. */
    private long upBound;

    /** Sequence batch size */
    private volatile int batchSize;

    /** Reservation percentage. */
    private volatile int reservePercentage;

    /** Reserved bottom bound of local counter (included). */
    private volatile long reservedBottomBound;

    /** Reserved upper bound of local counter (not included). */
    private volatile long reservedUpBound;

    /** A limit after which a new reservation should be done. */
    private volatile long newReservationLine;

    /** Reservation future. */
    private volatile IgniteInternalFuture<?> reservationFut;

    /** Reservation pool. */
    private final byte poolPlc = GridIoPolicy.SYSTEM_POOL;

    /** Synchronization lock for local value updates. */
    private final Lock localUpdateLock = new ReentrantLock();

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
     * @param batchSize Sequence batch size.
     * @param reservePercentage Reservation percentage.
     * @param locVal Local counter.
     * @param upBound Upper bound.
     */
    public GridCacheAtomicSequenceImpl(
        String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicSequenceValue> seqView,
        int batchSize,
        int reservePercentage,
        long locVal,
        long upBound
    ) {
        super(name, key, seqView);

        assert locVal <= upBound;

        this.batchSize = batchSize;
        this.reservePercentage = reservePercentage;
        this.upBound = upBound;
        this.locVal = locVal;

        reservedBottomBound = locVal;
        reservedUpBound = upBound;
        // Calculate next reservation bound.
        newReservationLine = calculateNewReservationLine(locVal);
    }

    /** {@inheritDoc} */
    @Override public long get() {
        checkRemoved();

        return locVal;
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
    private long internalUpdate(long l, boolean updated) throws IgniteCheckedException {
        assert l > 0;

        while (true){
            checkRemoved();

            localUpdateLock.lock();

            IgniteInternalFuture<?> reservation = reservationFut;

            try {
                boolean reservationInProgress = reservation != null;

                long newLocalVal = locVal + l;

                // Reserve new interval if operation is not in progress.
                if (newLocalVal >= newReservationLine && newLocalVal <= reservedUpBound && !reservationInProgress){
                    reservationFut = runAsyncReservation(0);
                }

                long locVal0 = locVal;

                if (newLocalVal <= upBound) {
                    locVal = newLocalVal;

                    return updated ? newLocalVal : locVal0;
                }

                // Await complete previous reservation.
                if (reservationInProgress){
                    reservation.get();

                    reservationFut = null;

                    // Retry check bounds.
                    continue;
                }

                // Still in reserved interval.
                if (newLocalVal < reservedUpBound) {
                    long curVal = locVal;

                    if (newLocalVal < reservedBottomBound)
                        locVal = reservedBottomBound;
                    else
                        locVal += l;

                    upBound = reservedUpBound;

                    return updated ? locVal : curVal;
                }
                // Switched to the next interval. New value more that upper reserved bound.
                else if (!reservationInProgress) {
                    long diff = newLocalVal - reservedUpBound;

                    // Calculate how many batch size included in l.
                    // It will our offset for global seq counter.
                    long off = (diff / batchSize) * batchSize;

                    reservationFut = runAsyncReservation(off);

                    // Can not wait async, should wait under lock until new interval reserved.
                    reservationFut.get();

                    reservationFut = null;
                }
            }
            finally {
                localUpdateLock.unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public int batchSize() {
        return batchSize;
    }

    /** {@inheritDoc} */
    @Override public void batchSize(int size) {
        A.ensure(size > 0, " Batch size can't be less then 0: " + size);

        localUpdateLock.lock();

        try {
            batchSize = size;
        }
        finally {
            localUpdateLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public int reservePercentage() {
        return reservePercentage;
    }

    /** {@inheritDoc} */
    @Override public void reservePercentage(int percentage) {
        A.ensure(percentage >= 0 && percentage <= 100, "Invalid reserve percentage: " + percentage);

        localUpdateLock.lock();

        try {
            reservePercentage = percentage;
        }
        finally {
            localUpdateLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override protected void invalidateLocalState() {
        locVal = 0;
        upBound = -1;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            if (rmvd)
                return;

            ctx.kernalContext().dataStructures().removeSequence(name, ctx.group().name());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
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

        GridFutureAdapter<?> resFut = new GridFutureAdapter<>();

        resFut.listen(f -> {
            if (f.error() == null)
                reservationFut = null;
        });

        ctx.kernalContext().closure().runLocalSafe(() -> {
            Callable<Void> reserveCall = reserveCallable(off);

            try {
                CU.retryTopologySafe(reserveCall);

                resFut.onDone();
            }
            catch (Throwable h) {
                resFut.onDone(h);
            }
        }, poolPlc);

        return resFut;
    }

    /**
     * @param off Reservation offset.
     * @return Callable for reserved new interval.
     */
    private Callable<Void> reserveCallable(long off){
       return new Callable<Void>() {
            @Override public Void call() throws Exception {
                try (GridNearTxLocal tx = CU.txStartInternal(ctx, cacheView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicSequenceValue seq = cacheView.get(key);

                    checkRemoved();

                    assert seq != null;

                    long curGlobalVal = seq.get();

                    reservedBottomBound = curGlobalVal + off;

                    reservedUpBound = reservedBottomBound + batchSize;

                    newReservationLine = calculateNewReservationLine(reservedBottomBound);

                    seq.set(reservedUpBound);

                    cacheView.put(key, seq);

                    tx.commit();
                }
                catch (Error | Exception e) {
                    if(!X.hasCause(e, ClusterTopologyCheckedException.class))
                        U.error(log, "Failed to get and add: " + this, e);

                    throw e;
                }

                return null;
            }
        };
    }

    /**
     * @return New reservation line.
     */
    private long calculateNewReservationLine(long initialValue) {
        return initialValue + (batchSize * reservePercentage / 100);
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

            return t.get1().dataStructures().sequence(t.get2(), null, 0L, false);
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
