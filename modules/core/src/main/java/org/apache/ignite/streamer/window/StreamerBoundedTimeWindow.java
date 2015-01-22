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
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Window which is bounded by size and time interval.
 */
public class StreamerBoundedTimeWindow<E> extends StreamerWindowAdapter<E> {
    /** Window structures holder. */
    private AtomicReference<WindowHolder> ref = new AtomicReference<>();

    /** Time interval. */
    private long timeInterval;

    /** Window maximum size. */
    private int maxSize;

    /** Unique flag. */
    private boolean unique;

    /** Event order counter. */
    private AtomicLong orderCnt = new AtomicLong();

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
     * @param maxSize Max size.
     */
    public void setMaximumSize(int maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * Gets window time interval.
     *
     * @return Time interval.
     */
    public long getTimeInterval() {
        return timeInterval;
    }

    /**
     * Sets window time interval.
     *
     * @param timeInterval Time interval.
     */
    public void setTimeInterval(long timeInterval) {
        this.timeInterval = timeInterval;
    }

    /**
     * Gets window unique flag.
     *
     * @return {@code True} if only unique events should be added to window.
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Sets window unique flag.
     *
     * @param unique {@code True} if only unique events should be added to window.
     */
    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    /** {@inheritDoc} */
    @Override public void checkConfiguration() throws IgniteCheckedException {
        if (timeInterval <= 0)
            throw new IgniteCheckedException("Failed to initialize window (timeInterval must be positive): [windowClass=" +
                getClass().getSimpleName() + ", maxSize=" + maxSize + ", timeInterval=" + timeInterval + ", unique=" +
                unique + ']');

        if (maxSize < 0)
            throw new IgniteCheckedException("Failed to initialize window (maximumSize cannot be negative): [windowClass=" +
                getClass().getSimpleName() + ", maxSize=" + maxSize + ", timeInterval=" + timeInterval + ", unique=" +
                unique + ']');
    }

    /** {@inheritDoc} */
    @Override protected void stop0() {
        // No-op.
    }

    /** {@inheritDoc} */
    @SuppressWarnings("RedundantCast")
    @Override protected void reset0() {
        ref.set(new WindowHolder(newQueue(), unique ? (Set<Object>)new GridConcurrentHashSet<>() : null,
            new AtomicInteger()));
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return ref.get().size().get();
    }

    /** {@inheritDoc} */
    @Override public int evictionQueueSize() {
        // Get estimate for eviction queue size.
        WindowHolder tup = ref.get();

        GridConcurrentSkipListSet<Holder<E>> evtsQueue = tup.collection();

        boolean sizeCheck = maxSize != 0;

        int overflow = tup.size().get() - maxSize;

        long timeBound = U.currentTimeMillis() - timeInterval;

        int idx = 0;
        int cnt = 0;

        for (Holder holder : evtsQueue) {
            if ((idx < overflow && sizeCheck) || holder.ts < timeBound)
                cnt++;
            else if ((idx >= overflow && sizeCheck) && holder.ts >= timeBound)
                break;
            else if (!sizeCheck && holder.ts >= timeBound)
                break;

            idx++;
        }

        return cnt;
    }

    /** {@inheritDoc} */
    @Override protected boolean enqueue0(E evt) {
        add(evt, U.currentTimeMillis());

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

    /** {@inheritDoc} */
    @Nullable private <T> T pollEvictedInternal() {
        WindowHolder tup = ref.get();

        AtomicInteger size = tup.size();

        GridConcurrentSkipListSet<Holder<E>> evtsQueue = tup.collection();

        long now = U.currentTimeMillis();

        while (true) {
            int curSize = size.get();

            if (maxSize > 0 && curSize > maxSize) {
                if (size.compareAndSet(curSize, curSize - 1)) {
                    Holder hldr = evtsQueue.pollFirst();

                    if (hldr != null) {
                        if (unique)
                            tup.set().remove(hldr.val);

                        return (T)hldr.val;
                    }
                    else {
                        // No actual events in queue, it means that other thread is just adding event.
                        // return null as it is a concurrent add call.
                        size.incrementAndGet();

                        return null;
                    }
                }
            }
            else {
                // Check if first entry qualifies for eviction.
                Holder first = evtsQueue.firstx();

                if (first != null && first.ts < now - timeInterval) {
                    if (evtsQueue.remove(first)) {
                        if (unique)
                            tup.set().remove(first.val);

                        size.decrementAndGet();

                        return (T)first.val;
                    }
                }
                else
                    return null;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected Collection<E> dequeue0(int cnt) {
        WindowHolder tup = ref.get();

        AtomicInteger size = tup.size();
        GridConcurrentSkipListSet<Holder<E>> evtsQueue = tup.collection();

        Collection<E> resCol = new ArrayList<>(cnt);

        while (true) {
            int curSize = size.get();

            if (curSize > 0) {
                if (size.compareAndSet(curSize, curSize - 1)) {
                    Holder<E> h = evtsQueue.pollLast();

                    if (h != null) {
                        resCol.add(h.val);

                        if (unique)
                            tup.set().remove(h.val);

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
        final WindowHolder win = ref.get();

        final GridConcurrentSkipListSet<Holder<E>> col = win.collection();
        final Set<Object> set = win.set();

        final Iterator<Holder<E>> it = col.iterator();

        return new GridStreamerWindowIterator<E>() {
            private Holder<E> lastRet;

            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public E next() {
                lastRet = it.next();

                return lastRet.val;
            }

            @Override public E removex() {
                if (lastRet == null)
                    throw new IllegalStateException();

                if (col.remove(lastRet)) {
                    if (set != null)
                        set.remove(lastRet.val);

                    win.size().decrementAndGet();

                    return lastRet.val;
                }
                else
                    return null;
            }
        };
    }

    /**
     * Checks queue consistency. Used in tests.
     */
    void consistencyCheck() {
        WindowHolder win = ref.get();

        assert win.collection().size() == win.size().get();

        if (win.set() != null) {
            // Check no duplicates in collection.

            Collection<Object> vals = new HashSet<>();

            for (Object evt : win.collection())
                assert vals.add(((Holder)evt).val);
        }
    }

    /**
     * @return New queue.
     */
    private GridConcurrentSkipListSet<Holder<E>> newQueue() {
        return new GridConcurrentSkipListSet<>(new Comparator<Holder>() {
            @Override public int compare(Holder h1, Holder h2) {
                if (h1 == h2)
                    return 0;

                if (h1.ts != h2.ts)
                    return h1.ts < h2.ts ? -1 : 1;

                return h1.order < h2.order ? -1 : 1;
            }
        });
    }

    /**
     * @param evt Event to add.
     * @param ts Event timestamp.
     */
    private void add(E evt, long ts) {
        WindowHolder tup = ref.get();

        if (!unique) {
            tup.collection().add(new Holder<>(evt, ts, orderCnt.incrementAndGet()));

            tup.size().incrementAndGet();
        }
        else {
            if (tup.set().add(evt)) {
                tup.collection().add(new Holder<>(evt, ts, orderCnt.incrementAndGet()));

                tup.size().incrementAndGet();
            }
        }
    }

    /**
     * @param evts Events to add.
     * @param ts Timestamp for added events.
     */
    private void addAll(Iterable<E> evts, long ts) {
        for (E evt : evts)
            add(evt, ts);
    }

    /**
     * Holder.
     */
    private static class Holder<E> {
        /** Value. */
        private E val;

        /** Event timestamp. */
        private long ts;

        /** Event order. */
        private long order;

        /**
         * @param val Event.
         * @param ts Timestamp.
         * @param order Order.
         */
        private Holder(E val, long ts, long order) {
            this.val = val;
            this.ts = ts;
            this.order = order;
        }
    }

    /**
     * Window holder.
     */
    @SuppressWarnings("ConstantConditions")
    private class WindowHolder extends GridTuple3<GridConcurrentSkipListSet<Holder<E>>, Set<Object>, AtomicInteger> {
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
         * @param size Size.
         */
        private WindowHolder(@Nullable GridConcurrentSkipListSet<Holder<E>> col,
            @Nullable Set<Object> set, @Nullable AtomicInteger size) {
            super(col, set, size);
        }

        /**
         * @return Holders collection.
         */
        public GridConcurrentSkipListSet<Holder<E>> collection() {
            return get1();
        }

        /**
         * @return Uniqueness set.
         */
        public Set<Object> set() {
            return get2();
        }

        /**
         * @return Size counter.
         */
        public AtomicInteger size() {
            return get3();
        }
    }
}
