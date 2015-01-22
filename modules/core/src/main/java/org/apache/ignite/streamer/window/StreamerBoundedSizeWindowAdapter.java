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

package org.apache.ignite.streamer.window;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.gridgain.grid.kernal.processors.streamer.*;
import org.apache.ignite.internal.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Abstract non-public class for size-bound windows. Support reset.
 */
abstract class StreamerBoundedSizeWindowAdapter<E, T> extends StreamerWindowAdapter<E> {
    /** Reference. */
    private AtomicReference<WindowHolder> ref = new AtomicReference<>();

    /** If true, only unique elements will be accepted. */
    private boolean unique;

    /** Window maximum size. */
    protected int maxSize;

    /**
     * Gets window maximum size.
     *
     * @return Maximum size.
     */
    public int getMaximumSize() {
        return maxSize;
    }

    /**
     * Sets window maximum size.
     *
     * @param maxSize Maximum size.
     */
    public void setMaximumSize(int maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * @return True if only unique elements will be accepted.
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * @param unique If true, only unique elements will be accepted.
     */
    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    /** {@inheritDoc} */
    @Override public void checkConfiguration() throws IgniteCheckedException {
        if (maxSize < 0)
            throw new IgniteCheckedException("Failed to initialize window (maximumSize cannot be negative) " +
                "[windowClass=" + getClass().getSimpleName() +
                ", maxSize=" + maxSize +
                ", unique=" + unique + ']');
    }

    /** {@inheritDoc} */
    @Override protected void stop0() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int size() {
        int size = ref.get().size().get();

        return size > 0 ? size : 0;
    }

    /** {@inheritDoc} */
    @Override public int evictionQueueSize() {
        int evictSize = size() - maxSize;

        return evictSize > 0 ? evictSize : 0;
    }

    /** {@inheritDoc} */
    @Override protected boolean enqueue0(E evt) {
        add(evt);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> pollEvicted0(int cnt) {
        Collection<E> res = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++) {
            E evicted = pollEvictedInternal();

            if (evicted == null)
                return res;

            res.add(evicted);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> pollEvictedBatch0() {
        E res = pollEvictedInternal();

        if (res == null)
            return Collections.emptyList();

        return Collections.singleton(res);
    }

    /**
     * Poll evicted internal implementation.
     *
     * @return Evicted element.
     */
    @Nullable private E pollEvictedInternal() {
        WindowHolder tup = ref.get();

        AtomicInteger size = tup.size();

        while (true) {
            int curSize = size.get();

            if (curSize > maxSize) {
                if (size.compareAndSet(curSize, curSize - 1)) {
                    E evt = pollInternal(tup.collection(), tup.set());

                    if (evt != null)
                        return evt;
                    else {
                        // No actual events in queue, it means that other thread is just adding event.
                        // return null as it is a concurrent add call.
                        size.incrementAndGet();

                        return null;
                    }
                }
            }
            else
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> dequeue0(int cnt) {
        WindowHolder tup = ref.get();

        AtomicInteger size = tup.size();
        Collection<T> evts = tup.collection();

        Collection<E> resCol = new ArrayList<>(cnt);

        while (true) {
            int curSize = size.get();

            if (curSize > 0) {
                if (size.compareAndSet(curSize, curSize - 1)) {
                    E res = pollInternal(evts, tup.set());

                    if (res != null) {
                        resCol.add(res);

                        if (resCol.size() >= cnt)
                            return resCol;
                    }
                    else {
                        size.incrementAndGet();

                        return resCol;
                    }
                }
            }
            else
                return resCol;
        }
    }

    /** {@inheritDoc} */
    @Override protected GridStreamerWindowIterator<E> iterator0() {
        WindowHolder win = ref.get();

        return iteratorInternal(win.collection(), win.set(), win.size());
    }

    /** {@inheritDoc} */
    @Override protected void reset0() {
        ref.set(new WindowHolder(newCollection(),
            unique ? new GridConcurrentHashSet<E>() : null,
            new AtomicInteger()));
    }

    /**
     * @param evt Event to add.
     */
    private void add(E evt) {
        WindowHolder tup = ref.get();

        if (addInternal(evt, tup.collection(), tup.set()))
            tup.size().incrementAndGet();
    }

    /**
     * @param evts Events to add.
     */
    private void addAll(Collection<E> evts) {
        WindowHolder tup = ref.get();

        int cnt = addAllInternal(evts, tup.collection(), tup.set());

        tup.size().addAndGet(cnt);
    }

    /**
     * Checks window consistency. Used for testing.
     */
    void consistencyCheck() {
        WindowHolder win = ref.get();

        consistencyCheck(win.collection(), win.set(), win.size());
    }

    /**
     * Get underlying collection.
     *
     * @return Collection.
     */
    @SuppressWarnings("ConstantConditions")
    protected Collection<T> collection() {
        return ref.get().get1();
    }

    /**
     * Creates new collection specific for window implementation. This collection will be subsequently passed
     * to addInternal(...) and pollInternal() methods.
     *
     * @return Collection - holder.
     */
    protected abstract Collection<T> newCollection();

    /**
     * Adds event to queue implementation.
     *
     * @param evt Event to add.
     * @param col Collection to add to.
     * @param set Set to check.
     * @return {@code True} if event was added.
     */
    protected abstract boolean addInternal(E evt, Collection<T> col, @Nullable Set<E> set);

    /**
     * Adds all events to queue implementation.
     *
     * @param evts Events to add.
     * @param col Collection to add to.
     * @param set Set to check.
     * @return Added events number.
     */
    protected abstract int addAllInternal(Collection<E> evts, Collection<T> col, @Nullable Set<E> set);

    /**
     * @param col Collection to add to.
     * @param set Set to check.
     * @return Polled object.
     */
    @Nullable protected abstract E pollInternal(Collection<T> col, @Nullable Set<E> set);

    /**
     * Creates iterator based on implementation collection type.
     *
     * @param col Collection.
     * @param set Set to check.
     * @param size Size.
     * @return Iterator.
     */
    protected abstract GridStreamerWindowIterator<E> iteratorInternal(Collection<T> col, @Nullable Set<E> set,
        AtomicInteger size);

    /**
     * Checks consistency. Used in tests.
     *
     * @param col Collection.
     * @param set Set if unique.
     * @param size Size holder.
     */
    protected abstract void consistencyCheck(Collection<T> col, Set<E> set, AtomicInteger size);

    /**
     * Window holder.
     */
    @SuppressWarnings("ConstantConditions")
    private class WindowHolder extends GridTuple3<Collection<T>, Set<E>, AtomicInteger> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public WindowHolder() {
            // No-op.
        }

        /**
         * @param col Collection.
         * @param set Set if unique.
         * @param size Window size counter.
         */
        WindowHolder(@Nullable Collection<T> col, @Nullable Set<E> set, @Nullable AtomicInteger size) {
            super(col, set, size);
        }

        /**
         * @return Collection.
         */
        public Collection<T> collection() {
            return get1();
        }

        /**
         * @return Set.
         */
        public Set<E> set() {
            return get2();
        }

        /**
         * @return Size.
         */
        public AtomicInteger size() {
            return get3();
        }
    }
}
