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
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Cache atomic stamped implementation.
 */
public final class GridCacheAtomicStampedImpl<T, S> implements GridCacheAtomicStampedEx<T, S>, IgniteChangeGlobalStateSupport, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<GridKernalContext, String>> stash =
        new ThreadLocal<IgniteBiTuple<GridKernalContext, String>>() {
            @Override protected IgniteBiTuple<GridKernalContext, String> initialValue() {
                return new IgniteBiTuple<>();
            }
        };

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
    public GridCacheAtomicStampedImpl(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicStampedValue<T, S>> atomicView,
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
    @Override public IgniteBiTuple<T, S> get() {
        checkRemoved();

        try {
            GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

            if (stmp == null)
                throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

            return stmp.get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void set(T val, S stamp) {
        checkRemoved();

        try {
            atomicView.invoke(key, new StampedSetEntryProcessor<>(val, stamp));
        }
        catch (EntryProcessorException e) {
            throw new IgniteException(e.getMessage(), e);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(T expVal, T newVal, S expStamp, S newStamp) {
        checkRemoved();

        try {
            EntryProcessorResult<Boolean> res =
                atomicView.invoke(key, new StampedCompareAndSetEntryProcessor<>(expVal, expStamp, newVal, newStamp));

            assert res != null && res.get() != null : res;

            return res.get();
        }
        catch (EntryProcessorException e) {
            throw new IgniteException(e.getMessage(), e);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public S stamp() {
        checkRemoved();

        try {
            GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

            if (stmp == null)
                throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

            return stmp.stamp();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public T value() {
        checkRemoved();

        try {
            GridCacheAtomicStampedValue<T, S> stmp = atomicView.get(key);

            if (stmp == null)
                throw new IgniteCheckedException("Failed to find atomic stamped with given name: " + name);

            return stmp.value();
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

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        this.atomicView = kctx.cache().atomicsCache();
        this.ctx = atomicView.context();
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) throws IgniteCheckedException {

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
