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

import org.gridgain.grid.kernal.processors.streamer.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Size-bounded sorted window. Unlike {@link StreamerBoundedSizeWindow}, which limits
 * window only on size, this window also provides events in sorted order.
 */
public class StreamerBoundedSizeSortedWindow<E>
    extends StreamerBoundedSizeWindowAdapter<E, StreamerBoundedSizeSortedWindow.Holder<E>> {
    /** Comparator. */
    private Comparator<E> comp;

    /** Order counter. */
    private AtomicLong orderCnt = new AtomicLong();

    /**
     * Gets event comparator.
     *
     * @return Event comparator.
     */
    public Comparator<E> getComparator() {
        return comp;
    }

    /**
     * Sets event comparator.
     *
     * @param comp Comparator.
     */
    public void setComparator(Comparator<E> comp) {
        this.comp = comp;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected Collection<Holder<E>> newCollection() {
        final Comparator<E> comp0 = comp;

        Collection<Holder<E>> col = new GridConcurrentSkipListSet<>(new Comparator<Holder<E>>() {
            @Override public int compare(Holder<E> h1, Holder<E> h2) {
                if (h1 == h2)
                    return 0;

                int diff = comp0 == null ?
                    ((Comparable<E>)h1.val).compareTo(h2.val) : comp0.compare(h1.val, h2.val);

                if (diff != 0)
                    return diff;
                else {
                    assert h1.order != h2.order;

                    return h1.order < h2.order ? -1 : 1;
                }
            }
        });

        return (Collection)col;
    }

    /** {@inheritDoc} */
    @Override protected boolean addInternal(E evt, Collection<Holder<E>> col, @Nullable Set<E> set) {
        if (comp == null) {
            if (!(evt instanceof Comparable))
                throw new IllegalArgumentException("Failed to add object to window (object is not comparable and no " +
                    "comparator is specified: " + evt);
        }

        if (set != null) {
            if (set.add(evt)) {
                col.add(new Holder<>(evt, orderCnt.getAndIncrement()));

                return true;
            }

            return false;
        }
        else {
            col.add(new Holder<>(evt, orderCnt.getAndIncrement()));

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override protected int addAllInternal(Collection<E> evts, Collection<Holder<E>> col, @Nullable Set<E> set) {
        int cnt = 0;

        for (E evt : evts) {
            if (addInternal(evt, col, set))
                cnt++;
        }

        return cnt;
    }

    /** {@inheritDoc} */
    @Override protected E pollInternal(Collection<Holder<E>> col, Set<E> set) {
        Holder<E> h = (Holder<E>)((NavigableSet<E>)col).pollLast();

        if (set != null && h != null)
            set.remove(h.val);

        return h == null ? null : h.val;
    }

    /** {@inheritDoc} */
    @Override protected GridStreamerWindowIterator<E> iteratorInternal(final Collection<Holder<E>> col,
        final Set<E> set, final AtomicInteger size) {
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

                    size.decrementAndGet();

                    return lastRet.val;
                }
                else
                    return null;
            }
        };
    }

    /**
     * Value wrapper.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class Holder<E> {
        /** Value. */
        private E val;

        /** Order to distinguish between objects for which comparator returns 0. */
        private long order;

        /**
         * @param val Value to hold.
         * @param order Adding order.
         */
        private Holder(E val, long order) {
            this.val = val;
            this.order = order;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (this == obj)
                return false;

            if (!(obj instanceof Holder))
                return false;

            Holder h = (Holder)obj;

            return F.eq(val, h.val) && order == h.order;
        }
    }

    /** {@inheritDoc} */
    @Override protected void consistencyCheck(Collection<Holder<E>> col, Set<E> set, AtomicInteger size) {
        assert col.size() == size.get();

        if (set != null) {
            // Check no duplicates in collection.

            Collection<Object> vals = new HashSet<>();

            for (Object evt : col)
                assert vals.add(((Holder)evt).val);
        }
    }
}
