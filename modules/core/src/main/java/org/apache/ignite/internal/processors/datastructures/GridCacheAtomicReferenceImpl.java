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
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache atomic reference implementation.
 */
public final class GridCacheAtomicReferenceImpl<T> implements GridCacheAtomicReferenceEx<T>,
    IgniteChangeGlobalStateSupport, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return new IgniteBiTuple<>();
            }
        };

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
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public T get() {
        checkRemoved();

        try {
            GridCacheAtomicReferenceValue<T> ref = atomicView.get(key);

            if (ref == null)
                throw new IgniteCheckedException("Failed to find atomic reference with given name: " + name);

            return ref.get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void set(final T val) {
        checkRemoved();

        try {
            if (ctx.dataStructures().knownType(val))
                atomicView.invoke(key, new ReferenceSetEntryProcessor<>(val));
            else {
                CU.retryTopologySafe(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        try (GridNearTxLocal tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                            GridCacheAtomicReferenceValue<T> ref = atomicView.get(key);

                            if (ref == null)
                                throw new IgniteException("Failed to find atomic reference with given name: " + name);

                            atomicView.put(key, new GridCacheAtomicReferenceValue<>(val));

                            tx.commit();
                        }

                        return null;
                    }
                });
            }
        }
        catch (EntryProcessorException e) {
            throw new IgniteException(e.getMessage(), e);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(final T expVal, final T newVal) {
        checkRemoved();

        try {
            if (ctx.dataStructures().knownType(expVal) && ctx.dataStructures().knownType(newVal)) {
                EntryProcessorResult<Boolean> res =
                    atomicView.invoke(key, new ReferenceCompareAndSetEntryProcessor<>(expVal, newVal));

                assert res != null && res.get() != null : res;

                return res.get();
            }
            else {
                return CU.retryTopologySafe(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        try (GridNearTxLocal tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                            GridCacheAtomicReferenceValue<T> ref = atomicView.get(key);

                            if (ref == null)
                                throw new IgniteException("Failed to find atomic reference with given name: " + name);

                            T curVal = ref.get();

                            if (!F.eq(expVal, curVal))
                                return false;
                            else {
                                atomicView.put(key, new GridCacheAtomicReferenceValue<>(newVal));

                                tx.commit();

                                return true;
                            }
                        }
                    }
                });
            }
        }
        catch (EntryProcessorException e) {
            throw new IgniteException(e.getMessage(), e);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Compares current value with specified value for equality and, if they are equal, replaces current value.
     *
     * @param newVal New value to set.
     * @param expVal Expected value.
     * @return Original value.
     */
    public T compareAndSetAndGet(final T newVal, final T expVal) {
        checkRemoved();

        try {
            if (ctx.dataStructures().knownType(expVal) && ctx.dataStructures().knownType(newVal)) {
                EntryProcessorResult<T> res =
                    atomicView.invoke(key, new ReferenceCompareAndSetAndGetEntryProcessor<T>(expVal, newVal));

                assert res != null;

                return res.get();
            }
            else {
                return CU.retryTopologySafe(new Callable<T>() {
                    @Override public T call() throws Exception {
                        try (GridNearTxLocal tx = CU.txStartInternal(ctx, atomicView, PESSIMISTIC, REPEATABLE_READ)) {
                            GridCacheAtomicReferenceValue<T> ref = atomicView.get(key);

                            if (ref == null)
                                throw new IgniteException("Failed to find atomic reference with given name: " + name);

                            T curVal = ref.get();

                            if (!F.eq(expVal, curVal))
                                return curVal;
                            else {
                                atomicView.put(key, new GridCacheAtomicReferenceValue<>(newVal));

                                tx.commit();

                                return expVal;
                            }
                        }
                    }
                });
            }
        }
        catch (EntryProcessorException e) {
            throw new IgniteException(e.getMessage(), e);
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

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        this.atomicView = kctx.cache().atomicsCache();
        this.ctx = atomicView.context();
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) throws IgniteCheckedException {
        // No-op.
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

    /**
     *
     */
    static class ReferenceSetEntryProcessor<T> implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicReferenceValue<T>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final T newVal;

        /**
         * @param newVal New value.
         */
        ReferenceSetEntryProcessor(T newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<GridCacheInternalKey, GridCacheAtomicReferenceValue<T>> e,
            Object... args) {
            GridCacheAtomicReferenceValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic reference with given name: " + e.getKey().name());

            e.setValue(new GridCacheAtomicReferenceValue<>(newVal));

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReferenceSetEntryProcessor.class, this);
        }
    }

    /**
     *
     */
    static class ReferenceCompareAndSetEntryProcessor<T> implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicReferenceValue<T>, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final T expVal;

        /** */
        private final T newVal;

        /**
         * @param expVal Expected value.
         * @param newVal New value.
         */
        ReferenceCompareAndSetEntryProcessor(T expVal, T newVal) {
            this.expVal = expVal;
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<GridCacheInternalKey, GridCacheAtomicReferenceValue<T>> e,
            Object... args) {
            GridCacheAtomicReferenceValue<T> val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic reference with given name: " + e.getKey().name());

            T curVal = val.get();

            if (F.eq(expVal, curVal)) {
                e.setValue(new GridCacheAtomicReferenceValue<T>(newVal));

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReferenceCompareAndSetEntryProcessor.class, this);
        }
    }

    /**
     *
     */
    static class ReferenceCompareAndSetAndGetEntryProcessor<T> implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicReferenceValue<T>, T> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final T expVal;

        /** */
        private final T newVal;

        /**
         * @param expVal Expected value.
         * @param newVal New value.
         */
        ReferenceCompareAndSetAndGetEntryProcessor(T expVal, T newVal) {
            this.expVal = expVal;
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public T process(MutableEntry<GridCacheInternalKey, GridCacheAtomicReferenceValue<T>> e,
            Object... args) {
            GridCacheAtomicReferenceValue<T> val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic reference with given name: " + e.getKey().name());

            T curVal = val.get();

            if (F.eq(expVal, curVal))
                e.setValue(new GridCacheAtomicReferenceValue<T>(newVal));

            return curVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReferenceCompareAndSetAndGetEntryProcessor.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAtomicReferenceImpl.class, this);
    }
}
