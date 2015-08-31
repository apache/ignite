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
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache atomic reference implementation.
 */
public final class GridCacheAtomicReferenceImpl<T> implements GridCacheAtomicReferenceEx<T>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return F.t2();
            }
        };

    /** Logger. */
    private IgniteLogger log;

    /** Atomic reference name. */
    private String name;

    /** Status.*/
    private volatile boolean rmvd;

    /** Check removed flag. */
    private boolean rmvCheck;

    /** Atomic reference key. */
    private GridCacheInternalKey key;

    /** Atomic reference projection. */
    private IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicReferenceValue<T>> atomicView;

    /** Cache context. */
    private GridCacheContext ctx;

    /** Callable for {@link #get} operation */
    private final Callable<T> getCall = new Callable<T>() {
        @Override public T call() throws Exception {
            GridCacheAtomicReferenceValue<T> ref = atomicView.get(key);

            if (ref == null)
                throw new IgniteCheckedException("Failed to find atomic reference with given name: " + name);

            return ref.get();
        }
    };

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridCacheAtomicReferenceImpl() {
        // No-op.
    }

    /**
     * Default constructor.
     *
     * @param name Atomic reference name.
     * @param key Atomic reference key.
     * @param atomicView Atomic projection.
     * @param ctx Cache context.
     */
    public GridCacheAtomicReferenceImpl(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicReferenceValue<T>> atomicView,
        GridCacheContext ctx) {
        assert key != null;
        assert atomicView != null;
        assert ctx != null;
        assert name != null;

        this.ctx = ctx;
        this.key = key;
        this.atomicView = atomicView;
        this.name = name;

        log = ctx.gridConfig().getGridLogger().getLogger(getClass());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public T get() {
        checkRemoved();

        try {
            return CU.outTx(getCall, ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void set(T val) {
        checkRemoved();

        try {
            CU.outTx(internalSet(val), ctx);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(T expVal, T newVal) {
        checkRemoved();

        try {
            return CU.outTx(internalCompareAndSet(wrapperPredicate(expVal), wrapperClosure(newVal)), ctx);
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
            ctx.kernalContext().dataStructures().removeAtomicReference(name);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Method make wrapper predicate for existing value.
     *
     * @param val Value.
     * @return Predicate.
     */
    private IgnitePredicate<T> wrapperPredicate(final T val) {
        return new IgnitePredicate<T>() {
            @Override public boolean apply(T e) {
                return val != null && val.equals(e);
            }
        };
    }

    /**
     * Method make wrapper closure for existing value.
     *
     * @param val Value.
     * @return Closure.
     */
    private IgniteClosure<T, T> wrapperClosure(final T val) {
        return new IgniteClosure<T, T>() {
            @Override public T apply(T e) {
                return val;
            }
        };
    }

    /**
     * Method returns callable for execution {@link #set(Object)} operation in async and sync mode.
     *
     * @param val Value will be set in reference .
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Boolean> internalSet(final T val) {
        return new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicReferenceValue<T> ref = atomicView.get(key);

                    if (ref == null)
                        throw new IgniteCheckedException("Failed to find atomic reference with given name: " + name);

                    ref.set(val);

                    atomicView.put(key, ref);

                    tx.commit();

                    return true;
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to set value [val=" + val + ", atomicReference=" + this + ']', e);

                    throw e;
                }
            }
        };
    }

    /**
     * Conditionally sets the new value. It will be set if {@code expValPred} is
     * evaluate to {@code true}.
     *
     * @param expValPred Predicate which should evaluate to {@code true} for value to be set.
     * @param newValClos Closure which generates new value.
     * @return Callable for execution in async and sync mode.
     */
    private Callable<Boolean> internalCompareAndSet(final IgnitePredicate<T> expValPred,
        final IgniteClosure<T, T> newValClos) {
        return new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                try (IgniteInternalTx tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                    GridCacheAtomicReferenceValue<T> ref = atomicView.get(key);

                    if (ref == null)
                        throw new IgniteCheckedException("Failed to find atomic reference with given name: " + name);

                    if (!expValPred.apply(ref.get())) {
                        tx.setRollbackOnly();

                        return false;
                    }
                    else {
                        ref.set(newValClos.apply(ref.get()));

                        atomicView.getAndPut(key, ref);

                        tx.commit();

                        return true;
                    }
                }
                catch (Error | Exception e) {
                    U.error(log, "Failed to compare and value [expValPred=" + expValPred + ", newValClos" +
                        newValClos + ", atomicReference" + this + ']', e);

                    throw e;
                }
            }
        };
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
        return new IllegalStateException("Atomic reference was removed from cache: " + name);
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

            return t.get1().dataStructures().atomicReference(t.get2(), null, false);
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
        return S.toString(GridCacheAtomicReferenceImpl.class, this);
    }
}