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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CLUSTER_READ_ONLY_MODE_ERROR_MSG_FORMAT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache sequence implementation.
 */
public final class GridCacheAtomicSequenceImpl extends AtomicDataStructureProxy<GridCacheAtomicSequenceValue>
    implements GridCacheAtomicSequenceEx, IgniteChangeGlobalStateSupport, Externalizable {
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

    /**  Upper bound of local counter. */
    private long upBound;

    /**  Sequence batch size */
    private volatile int batchSize;

    /** Synchronization lock for local value updates. */
    private final Lock localUpdate = new ReentrantLock();

    /** Synchronization for distributed sequence update. Acquired by threads with free topology (not in TX). */
    private final ReentrantLock distUpdateFreeTop = new ReentrantLock();

    /** Synchronization for distributed sequence update. Acquired by threads with locked topology (inside TX). */
    private final ReentrantLock distUpdateLockedTop = new ReentrantLock();

    /** Callable for execution {@link #incrementAndGet} operation in async and sync mode.  */
    private final Callable<Long> incAndGetCall = internalUpdate(1, true);

    /** Callable for execution {@link #getAndIncrement} operation in async and sync mode.  */
    private final Callable<Long> getAndIncCall = internalUpdate(1, false);

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
     * @param locVal Local counter.
     * @param upBound Upper bound.
     */
    public GridCacheAtomicSequenceImpl(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicSequenceValue> seqView,
        int batchSize,
        long locVal,
        long upBound)
    {
        super(name, key, seqView);

        assert locVal <= upBound;

        this.batchSize = batchSize;
        this.upBound = upBound;
        this.locVal = locVal;
    }

    /** {@inheritDoc} */
    @Override public long get() {
        checkRemoved();

        return locVal;
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
    private long internalUpdate(long l, @Nullable Callable<Long> updateCall, boolean updated) throws IgniteCheckedException {
        checkRemoved();

        assert l > 0;

        if (ctx.shared().readOnlyMode()) {
            throw new CacheInvalidStateException(new IgniteClusterReadOnlyException(
                String.format(CLUSTER_READ_ONLY_MODE_ERROR_MSG_FORMAT, "sequence", ctx.group().name(), ctx.name())
            ));
        }

        localUpdate.lock();

        try {
            // If reserved range isn't exhausted.
            long locVal0 = locVal;

            if (locVal0 + l <= upBound) {
                locVal = locVal0 + l;

                return updated ? locVal0 + l : locVal0;
            }
        }
        finally {
            localUpdate.unlock();
        }

        AffinityTopologyVersion lockedVer = ctx.shared().lockedTopologyVersion(null);

        // We need two separate locks here because two independent thread may attempt to update the sequence
        // simultaneously, one thread with locked topology and other with unlocked.
        // We cannot use the same lock for both cases because it leads to a deadlock when free-topology thread
        // waits for topology change, and locked topology thread waits to acquire the lock.
        // If a thread has locked topology, it must bypass sync with non-locked threads, but at the same time
        // we do not want multiple threads to attempt to run identical cache updates.
        ReentrantLock distLock = lockedVer == null ? distUpdateFreeTop : distUpdateLockedTop;

        distLock.lock();

        try {
            if (updateCall == null)
                updateCall = internalUpdate(l, updated);

            try {
                return CU.retryTopologySafe(updateCall);
            }
            catch (IgniteCheckedException | IgniteException | IllegalStateException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }
        }
        finally {
            distLock.unlock();
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

        localUpdate.lock();

        try {
            batchSize = size;
        }
        finally {
            localUpdate.unlock();
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
     * Method returns callable for execution all update operations in async and sync mode.
     *
     * @param l Value will be added to sequence.
     * @param updated If {@code true}, will return updated value, if {@code false}, will return previous value.
     * @return Callable for execution in async and sync mode.
     */
    @SuppressWarnings("TooBroadScope")
    private Callable<Long> internalUpdate(final long l, final boolean updated) {
        return new Callable<Long>() {
            @Override public Long call() throws Exception {
                assert distUpdateFreeTop.isHeldByCurrentThread() || distUpdateLockedTop.isHeldByCurrentThread();

                try (GridNearTxLocal tx = CU.txStartInternal(ctx, cacheView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicSequenceValue seq = cacheView.get(key);

                    checkRemoved();

                    assert seq != null;

                    long curLocVal;

                    long newUpBound;

                    // Even though we hold a transaction lock here, we must hold the local update lock here as well
                    // because we mutate multipe variables (locVal and upBound).
                    localUpdate.lock();

                    try {
                        curLocVal = locVal;

                        // If local range was already reserved in another thread.
                        if (curLocVal + l <= upBound) {
                            locVal = curLocVal + l;

                            return updated ? curLocVal + l : curLocVal;
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
                        localUpdate.unlock();
                    }

                    // Global counter must be more than reserved upper bound.
                    seq.set(newUpBound + 1);

                    cacheView.put(key, seq);

                    tx.commit();

                    return curLocVal;
                }
                catch (Error | Exception e) {
                    if (!X.hasCause(e, ClusterTopologyCheckedException.class))
                        U.error(log, "Failed to get and add: " + this, e);

                    throw e;
                }
            }
        };
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
