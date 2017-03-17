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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.internal.util.typedef.internal.CU.retryTopologySafe;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache atomic stamped implementation.
 */
public final class GridCacheAtomicStampedImpl<T, S> implements GridCacheAtomicStampedEx<T, S>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return new IgniteBiTuple<>();
            }
        };

    /** Logger. */
    private IgniteLogger log;

    /** Atomic stamped name. */
    private String name;

    /** Removed flag.*/
    private volatile boolean rmvd;

    /** Check removed flag. */
    private boolean rmvCheck;

    /** Atomic stamped key. */
    private GridCacheInternalKey key;

    /** Atomic stamped projection. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicStampedValue<T, S>> atomicView;

    /** Cache context. */
    private GridCacheContext ctx;

    /** Callable for {@link #get()} operation */
    private final Callable<IgniteBiTuple<T, S>> getCall = retryTopologySafe(new Callable<IgniteBiTuple<T, S>>() {
        @Override public IgniteBiTuple<T, S> call() throws Exception {
            GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

            if (stmp == null)
                throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

            return stmp.get();
        }
    });

    /** Callable for {@link #value()} operation */
    private final Callable<T> valCall = retryTopologySafe(new Callable<T>() {
        @Override public T call() throws Exception {
            GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

            if (stmp == null)
                throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

            return stmp.value();
        }
    });

    /** Callable for {@link #stamp()} operation */
    private final Callable<S> stampCall = retryTopologySafe(new Callable<S>() {
        @Override public S call() throws Exception {
            GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

            if (stmp == null)
                throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

            return stmp.stamp();
        }
    });

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheAtomicStampedImpl() {
        // No-op.
    }

    /**
     * Default constructor.
     *
     * @param name Atomic stamped name.
     * @param key Atomic stamped key.
     * @param atomicView Atomic projection.
     * @param ctx Cache context.
     */
    public GridCacheAtomicStampedImpl(String name, GridCacheInternalKey key, IgniteInternalCache<GridCacheInternalKey,
            GridCacheAtomicStampedValue<T, S>> atomicView, GridCacheContext ctx) {
        assert key != null;
        assert atomicView != null;
        assert ctx != null;
        assert name != null;

        this.ctx = ctx;
        this.key = key;
        this.atomicView = atomicView;
        this.name = name;

        log = ctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<T, S> get() {
        checkRemoved();

        try {
            return CU.outTx(getCall, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void set(T val, S stamp) {
        checkRemoved();

        try {
            CU.outTx(internalSet(val, stamp), ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(T expVal, T newVal, S expStamp, S newStamp) {
        checkRemoved();

        try {
            return CU.outTx(internalCompareAndSet(F0.equalTo(expVal), wrapperClosure(newVal),
                F0.equalTo(expStamp), wrapperClosure(newStamp)), ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public S stamp() {
        checkRemoved();

        try {
            return CU.outTx(stampCall, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public T value() {
        checkRemoved();

        try {
            return CU.outTx(valCall, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
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
        if (rmvd)
            return;

        try {
            ctx.kernalContext().dataStructures().removeAtomicStamped(name);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Method make wrapper closure for existing value.
     *
     * @param val Value.
     * @return Closure.
     */
    private <N> IgniteClosure<N, N> wrapperClosure(final N val) {
        return new IgniteClosure<N, N>() {
            @Override public N apply(N e) {
                return val;
            }
        };
    }

    /**
     * Method returns callable for execution {@link #set(Object,Object)}} operation in async and sync mode.
     *
     * @param val Value will be set in the atomic stamped.
     * @param stamp Stamp will be set in the atomic stamped.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Boolean> internalSet(final T val, final S stamp) {
        return retryTopologySafe(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                try (GridNearTxLocal tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

                    if (stmp == null)
                        throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

                    stmp.set(val, stamp);

                    atomicView.put(key, stmp);

                    tx.commit();

                    return true;
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to set [val=" + val + ", stamp=" + stamp + ", atomicStamped=" + this + ']', e);

                    throw e;
                }
            }
        });
    }

    /**
     * Conditionally asynchronously sets the new value and new stamp. They will be set if
     * {@code expValPred} and {@code expStampPred} both evaluate to {@code true}.
     *
     * @param expValPred Predicate which should evaluate to {@code true} for value to be set
     * @param newValClos Closure generates new value.
     * @param expStampPred Predicate which should evaluate to {@code true} for value to be set
     * @param newStampClos Closure generates new stamp value.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Boolean> internalCompareAndSet(final IgnitePredicate<T> expValPred,
        final IgniteClosure<T, T> newValClos, final IgnitePredicate<S> expStampPred,
        final IgniteClosure<S, S> newStampClos) {
        return retryTopologySafe(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                try (GridNearTxLocal tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

                    if (stmp == null)
                        throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

                    if (!(expValPred.apply(stmp.value()) && expStampPred.apply(stmp.stamp()))) {
                        tx.setRollbackOnly();

                        return false;
                    }
                    else {
                        stmp.set(newValClos.apply(stmp.value()), newStampClos.apply(stmp.stamp()));

                        atomicView.getAndPut(key, stmp);

                        tx.commit();

                        return true;
                    }
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to compare and set [expValPred=" + expValPred + ", newValClos=" +
                        newValClos + ", expStampPred=" + expStampPred + ", newStampClos=" + newStampClos +
                        ", atomicStamped=" + this + ']', e);

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
    @SuppressWarnings("unchecked")
    private Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<GridKernalContext, String> t = stash.get();

            return t.get1().dataStructures().atomicStamped(t.get2(), null, null, false);
        }
        catch (IgniteCheckedException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
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
                rmvd = atomicView.get(key) == null;
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
        return new IllegalStateException("Atomic stamped was removed from cache: " + name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridCacheAtomicStampedImpl.class, this);
    }
}
