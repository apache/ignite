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
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache atomic stamped implementation.
 */
public final class GridCacheAtomicStampedImpl<T, S> extends AtomicDataStructureProxy<GridCacheAtomicStampedValue<T, S>>
    implements GridCacheAtomicStampedEx<T, S>, IgniteChangeGlobalStateSupport, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return new IgniteBiTuple<>();
            }
        };

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
     */
    public GridCacheAtomicStampedImpl(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicStampedValue<T, S>> atomicView) {
        super(name, key, atomicView);
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<T, S> get() {
        checkRemoved();

        try {
            GridCacheAtomicStampedValue<T, S> stmp = cacheView.get(key);

            if (stmp == null)
                throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

            return stmp.get();
        }
        catch (IgniteException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void set(final T val, final S stamp) {
        checkRemoved();

        try {
            if (ctx.dataStructures().knownType(val) && ctx.dataStructures().knownType(stamp)) {
                EntryProcessorResult res = cacheView.invoke(key, new StampedSetEntryProcessor<>(val, stamp));

                assert res != null;

                res.get();
            }
            else {
                CU.retryTopologySafe(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        try (GridNearTxLocal tx = CU.txStartInternal(ctx, cacheView, PESSIMISTIC, REPEATABLE_READ)) {
                            GridCacheAtomicStampedValue<T, S> ref = cacheView.get(key);

                            if (ref == null)
                                throw new IgniteException("Failed to find atomic stamped with given name: " + name);

                            cacheView.put(key, new GridCacheAtomicStampedValue<>(val, stamp));

                            tx.commit();
                        }

                        return null;
                    }
                });
            }
        }
        catch (EntryProcessorException | IgniteException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(final T expVal, final T newVal, final S expStamp, final S newStamp) {
        checkRemoved();

        try {
            if (ctx.dataStructures().knownType(expVal) &&
                ctx.dataStructures().knownType(newVal) &&
                ctx.dataStructures().knownType(expStamp) &&
                ctx.dataStructures().knownType(newStamp)) {
                EntryProcessorResult<Boolean> res =
                    cacheView.invoke(key, new StampedCompareAndSetEntryProcessor<>(expVal, expStamp, newVal, newStamp));

                assert res != null && res.get() != null : res;

                return res.get();
            }
            else {
                return CU.retryTopologySafe(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        try (GridNearTxLocal tx = CU.txStartInternal(ctx, cacheView, PESSIMISTIC, REPEATABLE_READ)) {
                            GridCacheAtomicStampedValue<T, S> val = cacheView.get(key);

                            if (val == null)
                                throw new IgniteException("Failed to find atomic stamped with given name: " + name);

                            if (F.eq(expVal, val.value()) && F.eq(expStamp, val.stamp())) {
                                cacheView.put(key, new GridCacheAtomicStampedValue<>(newVal, newStamp));

                                tx.commit();

                                return true;
                            }

                            return false;
                        }
                    }
                });
            }
        }
        catch (EntryProcessorException | IgniteException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public S stamp() {
        checkRemoved();

        try {
            GridCacheAtomicStampedValue<T, S> stmp = cacheView.get(key);

            if (stmp == null)
                throw new IgniteException("Failed to find atomic stamped with given name: " + name);

            return stmp.stamp();
        }
        catch (IgniteException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public T value() {
        checkRemoved();

        try {
            GridCacheAtomicStampedValue<T, S> stmp = cacheView.get(key);

            if (stmp == null)
                throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

            return stmp.value();
        }
        catch (IgniteException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (rmvd)
            return;

        try {
            ctx.kernalContext().dataStructures().removeAtomicStamped(name, ctx.group().name());
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

            return t.get1().dataStructures().atomicStamped(t.get2(), null, null, null, false);
        }
        catch (IgniteCheckedException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /**
     * @return Error.
     */
    private IllegalStateException removedError() {
        return new IllegalStateException("Atomic stamped was removed from cache: " + name);
    }

    /**
     *
     */
    static class StampedSetEntryProcessor<T, S> implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicStampedValue<T, S>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final T newVal;

        /** */
        private final S newStamp;

        /**
         * @param newVal New value.
         * @param newStamp New stamp value.
         */
        StampedSetEntryProcessor(T newVal, S newStamp) {
            this.newVal = newVal;
            this.newStamp = newStamp;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<GridCacheInternalKey, GridCacheAtomicStampedValue<T, S>> e,
            Object... args) {
            GridCacheAtomicStampedValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic stamped with given name: " + e.getKey().name());

            e.setValue(new GridCacheAtomicStampedValue<>(newVal, newStamp));

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return GridToStringBuilder.toString(StampedSetEntryProcessor.class, this);
        }
    }

    /**
     *
     */
    static class StampedCompareAndSetEntryProcessor<T, S> implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicStampedValue<T, S>, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final T expVal;

        /** */
        private final S expStamp;

        /** */
        private final T newVal;

        /** */
        private final S newStamp;

        /**
         * @param expVal Expected value.
         * @param expStamp Expected stamp.
         * @param newVal New value.
         * @param newStamp New stamp value.
         */
        StampedCompareAndSetEntryProcessor(T expVal, S expStamp, T newVal, S newStamp) {
            this.expVal = expVal;
            this.expStamp = expStamp;
            this.newVal = newVal;
            this.newStamp = newStamp;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<GridCacheInternalKey, GridCacheAtomicStampedValue<T, S>> e,
            Object... args) {
            GridCacheAtomicStampedValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic stamped with given name: " + e.getKey().name());

            if (F.eq(expVal, val.value()) && F.eq(expStamp, val.stamp())) {
                e.setValue(new GridCacheAtomicStampedValue<>(newVal, newStamp));

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return GridToStringBuilder.toString(StampedCompareAndSetEntryProcessor.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridCacheAtomicStampedImpl.class, this);
    }
}
