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
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Cache atomic long implementation.
 */
public final class GridCacheAtomicLongImpl extends AtomicDataStructureProxy<GridCacheAtomicLongValue>
    implements GridCacheAtomicLongEx, IgniteChangeGlobalStateSupport, Externalizable {
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
    public GridCacheAtomicLongImpl() {
        // No-op.
    }

    /**
     * Default constructor.
     *
     * @param name Atomic long name.
     * @param key Atomic long key.
     * @param atomicView Atomic projection.
     */
    public GridCacheAtomicLongImpl(String name,
        GridCacheInternalKey key,
        IgniteInternalCache<GridCacheInternalKey, GridCacheAtomicLongValue> atomicView) {
        super(name, key, atomicView);
    }

    /** {@inheritDoc} */
    @Override public long get() {
        checkRemoved();

        try {
            GridCacheAtomicLongValue val = cacheView.get(key);

            if (val == null)
                throw new IgniteException("Failed to find atomic long: " + name);

            return val.get();
        }
        catch (IgniteException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long incrementAndGet() {
        checkRemoved();

        try {
            EntryProcessorResult<Long> res = cacheView.invoke(key, IncrementAndGetProcessor.INSTANCE);

            assert res != null && res.get() != null : res;

            return res.get();
        }
        catch (EntryProcessorException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() {
        checkRemoved();

        try {
            EntryProcessorResult<Long> res = cacheView.invoke(key, GetAndIncrementProcessor.INSTANCE);

            assert res != null && res.get() != null : res;

            return res.get();
        }
        catch (EntryProcessorException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) {
        checkRemoved();

        try {
            EntryProcessorResult<Long> res = cacheView.invoke(key, new AddAndGetProcessor(l));

            assert res != null && res.get() != null : res;

            return res.get();
        }
        catch (EntryProcessorException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) {
        checkRemoved();

        try {
            EntryProcessorResult<Long> res = cacheView.invoke(key, new GetAndAddProcessor(l));

            assert res != null && res.get() != null : res;

            return res.get();
        }
        catch (EntryProcessorException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long decrementAndGet() {
        checkRemoved();

        try {
            EntryProcessorResult<Long> res = cacheView.invoke(key, DecrementAndGetProcessor.INSTANCE);

            assert res != null && res.get() != null : res;

            return res.get();
        }
        catch (EntryProcessorException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndDecrement() {
        checkRemoved();

        try {
            EntryProcessorResult<Long> res = cacheView.invoke(key, GetAndDecrementProcessor.INSTANCE);

            assert res != null && res.get() != null : res;

            return res.get();
        }
        catch (EntryProcessorException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndSet(long l) {
        checkRemoved();

        try {
            EntryProcessorResult<Long> res = cacheView.invoke(key, new GetAndSetProcessor(l));

            assert res != null && res.get() != null : res;

            return res.get();
        }
        catch (EntryProcessorException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(long expVal, long newVal) {
        checkRemoved();

        try {
            EntryProcessorResult<Long> res = cacheView.invoke(key, new CompareAndSetProcessor(expVal, newVal));

            assert res != null && res.get() != null : res;

            return res.get() == expVal;
        }
        catch (EntryProcessorException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /**
     * @param expVal Expected value.
     * @param newVal New value.
     * @return Old value.
     */
    public long compareAndSetAndGet(long expVal, long newVal) {
        checkRemoved();

        try {
            EntryProcessorResult<Long> res = cacheView.invoke(key, new CompareAndSetProcessor(expVal, newVal));

            assert res != null && res.get() != null : res;

            return res.get();
        }
        catch (EntryProcessorException | IgniteCheckedException e) {
            throw checkRemovedAfterFail(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (rmvd)
            return;

        try {
            ctx.kernalContext().dataStructures().removeAtomicLong(name, ctx.group().name());
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

            return t.get1().dataStructures().atomicLong(t.get2(), null, 0L, false);
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
        return S.toString(GridCacheAtomicLongImpl.class, this);
    }

    /**
     *
     */
    static class GetAndSetProcessor implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicLongValue, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final long newVal;

        /**
         * @param newVal New value.
         */
        GetAndSetProcessor(long newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<GridCacheInternalKey, GridCacheAtomicLongValue> e, Object... args) {
            GridCacheAtomicLongValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic long: " + e.getKey().name());

            long curVal = val.get();

            e.setValue(new GridCacheAtomicLongValue(newVal));

            return curVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GetAndSetProcessor.class, this);
        }
    }

    /**
     *
     */
    static class GetAndAddProcessor implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicLongValue, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final long delta;

        /**
         * @param delta Delta.
         */
        GetAndAddProcessor(long delta) {
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<GridCacheInternalKey, GridCacheAtomicLongValue> e, Object... args) {
            GridCacheAtomicLongValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic long: " + e.getKey().name());

            long curVal = val.get();

            e.setValue(new GridCacheAtomicLongValue(curVal + delta));

            return curVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GetAndAddProcessor.class, this);
        }
    }

    /**
     *
     */
    static class AddAndGetProcessor implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicLongValue, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final long delta;

        /**
         * @param delta Delta.
         */
        AddAndGetProcessor(long delta) {
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<GridCacheInternalKey, GridCacheAtomicLongValue> e, Object... args) {
            GridCacheAtomicLongValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic long: " + e.getKey().name());

            long newVal = val.get() + delta;

            e.setValue(new GridCacheAtomicLongValue(newVal));

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AddAndGetProcessor.class, this);
        }
    }

    /**
     *
     */
    static class CompareAndSetProcessor implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicLongValue, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final long expVal;

        /** */
        private final long newVal;

        /**
         * @param expVal Expected value.
         * @param newVal New value.
         */
        CompareAndSetProcessor(long expVal, long newVal) {
            this.expVal = expVal;
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<GridCacheInternalKey, GridCacheAtomicLongValue> e, Object... args) {
            GridCacheAtomicLongValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic long: " + e.getKey().name());

            long curVal = val.get();

            if (curVal == expVal)
                e.setValue(new GridCacheAtomicLongValue(newVal));

            return curVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CompareAndSetProcessor.class, this);
        }
    }

    /**
     *
     */
    static class GetAndIncrementProcessor implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicLongValue, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final GetAndIncrementProcessor INSTANCE = new GetAndIncrementProcessor();

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<GridCacheInternalKey, GridCacheAtomicLongValue> e, Object... args) {
            GridCacheAtomicLongValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic long: " + e.getKey().name());

            long ret = val.get();

            e.setValue(new GridCacheAtomicLongValue(ret + 1));

            return ret;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GetAndIncrementProcessor.class, this);
        }
    }

    /**
     *
     */
    static class IncrementAndGetProcessor implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicLongValue, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final IncrementAndGetProcessor INSTANCE = new IncrementAndGetProcessor();

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<GridCacheInternalKey, GridCacheAtomicLongValue> e, Object... args) {
            GridCacheAtomicLongValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic long: " + e.getKey().name());

            long newVal = val.get() + 1;

            e.setValue(new GridCacheAtomicLongValue(newVal));

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IncrementAndGetProcessor.class, this);
        }
    }

    /**
     *
     */
    static class GetAndDecrementProcessor implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicLongValue, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final GetAndDecrementProcessor INSTANCE = new GetAndDecrementProcessor();

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<GridCacheInternalKey, GridCacheAtomicLongValue> e, Object... args) {
            GridCacheAtomicLongValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic long: " + e.getKey().name());

            long ret = val.get();

            e.setValue(new GridCacheAtomicLongValue(ret - 1));

            return ret;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GetAndDecrementProcessor.class, this);
        }
    }

    /**
     *
     */
    static class DecrementAndGetProcessor implements
        CacheEntryProcessor<GridCacheInternalKey, GridCacheAtomicLongValue, Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final DecrementAndGetProcessor INSTANCE = new DecrementAndGetProcessor();

        /** {@inheritDoc} */
        @Override public Long process(MutableEntry<GridCacheInternalKey, GridCacheAtomicLongValue> e, Object... args) {
            GridCacheAtomicLongValue val = e.getValue();

            if (val == null)
                throw new EntryProcessorException("Failed to find atomic long: " + e.getKey().name());

            long newVal = val.get() - 1;

            e.setValue(new GridCacheAtomicLongValue(newVal));

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DecrementAndGetProcessor.class, this);
        }
    }
}
