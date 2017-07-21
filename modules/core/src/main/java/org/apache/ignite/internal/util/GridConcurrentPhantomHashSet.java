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

package org.apache.ignite.internal.util;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Concurrent phantom hash set implementation.
 */
public class GridConcurrentPhantomHashSet<E> implements Set<E> {
    /** Empty array. */
    private static final Object[] EMPTY_ARR = new Object[0];

    /** Reference store. */
    @GridToStringInclude
    private GridConcurrentHashSet<PhantomReferenceElement<E>> store;

    /** Reference queue. */
    @GridToStringExclude
    private final ReferenceQueue<E> refQ = new ReferenceQueue<>();

    /** Reference factory. */
    private final IgniteClosure<E, PhantomReferenceElement<E>> fact = new IgniteClosure<E, PhantomReferenceElement<E>>() {
        @Override public PhantomReferenceElement<E> apply(E e) {
            assert e != null;

            return new PhantomReferenceElement<>(e, refQ);
        }
    };

    /**
     * Creates a new, empty set with a default initial capacity,
     * load factor, and concurrencyLevel.
     */
    public GridConcurrentPhantomHashSet() {
        store = new GridConcurrentHashSet<>();
    }

    /**
     * Creates a new, empty set with the specified initial
     * capacity, and with default load factor and concurrencyLevel.
     *
     * @param initCap The initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative.
     */
    public GridConcurrentPhantomHashSet(int initCap) {
        store = new GridConcurrentHashSet<>(initCap);
    }

    /**
     * Creates a new, empty set with the specified initial
     * capacity, load factor, and concurrency level.
     *
     * @param initCap The initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @param loadFactor The load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @param conLevel The estimated number of concurrently
     *      updating threads. The implementation performs internal sizing
     *      to try to accommodate this many threads.
     * @throws IllegalArgumentException if the initial capacity is
     *      negative or the load factor or concurrency level are
     *      non-positive.
     */
    public GridConcurrentPhantomHashSet(int initCap, float loadFactor, int conLevel) {
        store = new GridConcurrentHashSet<>(initCap, loadFactor, conLevel);
    }

    /**
     * Constructs a new set containing the elements in the specified
     * collection, with default load factor and an initial
     * capacity sufficient to contain the elements in the specified collection.
     *
     * @param c Collection to add.
     */
    public GridConcurrentPhantomHashSet(Collection<E> c) {
        this(c.size());

        addAll(c);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SimplifiableIfStatement"})
    @Override public boolean add(E e) {
        A.notNull(e, "e");

        removeStale();

        if (!contains(e))
            return store.add(fact.apply(e));

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(@Nullable Collection<? extends E> c) {
        boolean res = false;

        if (!F.isEmpty(c)) {
            assert c != null;

            for (E e : c) {
                res |= add(e);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(@Nullable Collection<?> c) {
        removeStale();

        boolean res = false;

        if (!F.isEmpty(c)) {
            assert c != null;

            Iterator<PhantomReferenceElement<E>> iter = store.iterator();

            while (iter.hasNext()) {
                if (!c.contains(iter.next().get())) {
                    iter.remove();

                    res = true;
                }
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        removeStale();

        return store.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        removeStale();

        return store.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean contains(@Nullable Object o) {
        removeStale();

        if (!store.isEmpty() && o != null) {
            for (PhantomReferenceElement ref : store) {
                Object reft = ref.get();

                if (reft != null && reft.equals(o))
                    return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(@Nullable Collection<?> c) {
        if (F.isEmpty(c))
            return false;

        assert c != null;

        for (Object o : c) {
            if (!contains(o))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ToArrayCallWithZeroLengthArrayArgument"})
    @Override public Object[] toArray() {
        return toArray(EMPTY_ARR);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SuspiciousToArrayCall"})
    @Override public <T> T[] toArray(T[] a) {
        removeStale();

        Collection<E> elems = new LinkedList<>();

        for (PhantomReferenceElement<E> ref : store) {
            E e = ref.get();

            if (e != null)
                elems.add(e);
        }

        return elems.toArray(a);
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        removeStale();

        return new Iterator<E>() {
            /** Storage iterator. */
            private Iterator<PhantomReferenceElement<E>> iter = store.iterator();

            /** Current element. */
            private E elem;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                if (elem == null) {
                    while (iter.hasNext()) {
                        PhantomReferenceElement<E> ref = iter.next();

                        E e;

                        if (ref != null && (e = ref.get()) != null) {
                            elem = e;

                            break;
                        }
                        else
                            removeStale();
                    }
                }

                return elem != null;
            }

            /** {@inheritDoc} */
            @SuppressWarnings({"IteratorNextCanNotThrowNoSuchElementException"})
            @Override public E next() {
                if (elem == null) {
                    if (!hasNext())
                        throw new NoSuchElementException();
                }

                E res = elem;

                elem = null;

                return res;
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                iter.remove();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        store.clear();
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@Nullable Object o) {
        removeStale();

        if (o != null) {
            for (Iterator<PhantomReferenceElement<E>> iter = store.iterator(); iter.hasNext();) {
                Object reft = iter.next().get();

                if (reft != null && reft.equals(o)) {
                    iter.remove();

                    return true;
                }
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(@Nullable Collection<?> c) {
        boolean res = false;

        if (!F.isEmpty(c)) {
            assert c != null;

            for (Object o : c) {
                res |= remove(o);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(@Nullable Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridConcurrentPhantomHashSet))
            return false;

        GridConcurrentPhantomHashSet that = (GridConcurrentPhantomHashSet)o;

        return store.equals(that.store);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return store.hashCode();
    }

    /**
     * Removes stale references.
     */
    private void removeStale() {
        PhantomReferenceElement<E> ref;

        while ((ref = (PhantomReferenceElement<E>) refQ.poll()) != null) {
            store.remove(ref);

            onGc(ref.get());
        }
    }

    /**
     * This method is called on every element when it gets GC-ed.
     *
     * @param e Element that is about to get GC-ed.
     */
    protected void onGc(E e) {
        // No-op.
    }

    /**
     * Phantom reference implementation for this set.
     */
    private static class PhantomReferenceElement<E> extends PhantomReference<E> {
        /** Element hash code. */
        private int hashCode;

        /**
         * Creates weak reference element.
         *
         * @param ref Referent.
         * @param refQ Reference queue.
         */
        private PhantomReferenceElement(E ref, ReferenceQueue<? super E> refQ) {
            super(ref, refQ);

            hashCode = ref != null ? ref.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof PhantomReferenceElement))
                return false;

            E thisRef = get();

            Object thatRef = ((Reference)o).get();

            return thisRef != null ? thisRef.equals(thatRef) : thatRef == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hashCode;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridConcurrentPhantomHashSet.class, this);
    }
}