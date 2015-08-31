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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.jetbrains.annotations.Nullable;

import static java.lang.Boolean.TRUE;

/**
 * This class provided the same logic as {@link ConcurrentSkipListSet}. It adds
 * methods {@link #firstx()} and {@link #lastx()}, which unlike {@link #first()}
 * and {@link #last()} methods return {@code null} for empty sets instead of
 * throwing {@link NoSuchElementException}.
 */
public class GridConcurrentSkipListSet<E> extends GridSerializableSet<E> implements NavigableSet<E>, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * The underlying map. Uses Boolean.TRUE as value for each
     * element.
     */
    private ConcurrentNavigableMap<E, Object> m;

    /**
     * Constructs a new, empty set that orders its elements according to
     * their {@linkplain Comparable natural ordering}.
     */
    public GridConcurrentSkipListSet() {
        m = new ConcurrentSkipListMap<>();
    }

    /**
     * Constructs a new, empty set that orders its elements according to
     * the specified comparator.
     *
     * @param comp the comparator that will be used to order this set.
     *        If <tt>null</tt>, the {@linkplain Comparable natural
     *        ordering} of the elements will be used.
     */
    public GridConcurrentSkipListSet(Comparator<? super E> comp) {
        m = new ConcurrentSkipListMap<>(comp);
    }

    /**
     * Constructs a new set containing the elements in the specified
     * collection, that orders its elements according to their
     * {@linkplain Comparable natural ordering}.
     *
     * @param c The elements that will comprise the new set
     * @throws ClassCastException if the elements in <tt>c</tt> are
     *         not {@link Comparable}, or are not mutually comparable
     * @throws NullPointerException if the specified collection or any
     *         of its elements are null
     */
    public GridConcurrentSkipListSet(Collection<? extends E> c) {
        m = new ConcurrentSkipListMap<>();
        addAll(c);
    }

    /**
     * Constructs a new set containing the same elements and using the
     * same ordering as the specified sorted set.
     *
     * @param s sorted set whose elements will comprise the new set
     * @throws NullPointerException if the specified sorted set or any
     *         of its elements are null
     */
    public GridConcurrentSkipListSet(SortedSet<E> s) {
        m = new ConcurrentSkipListMap<>(s.comparator());

        addAll(s);
    }

    /**
     * For use by submaps.
     *
     * @param m Base map.
     */
    private GridConcurrentSkipListSet(ConcurrentNavigableMap<E, Object> m) {
        this.m = m;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "OverriddenMethodCallDuringObjectConstruction", "CloneCallsConstructors", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public GridConcurrentSkipListSet<E> clone() {
        try {
            GridConcurrentSkipListSet<E> clone = (GridConcurrentSkipListSet<E>)super.clone();

            clone.m = new ConcurrentSkipListMap<>(m);

            return clone;
        }
        catch (CloneNotSupportedException ignored) {
            throw new Error("Clone should be supported on GridConcurrentSkipListSet class: " + this);
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return m.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return m.isEmpty();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SuspiciousMethodCalls"})
    @Override public boolean contains(Object o) {
        return m.containsKey(o);
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        return m.putIfAbsent(e, TRUE) == null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        return m.remove(o, TRUE);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        m.clear();
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        return m.navigableKeySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> descendingIterator() {
        return m.descendingKeySet().iterator();
    }

    /** {@inheritDoc} */
    public boolean equals(Object o) {
        // Override AbstractSet version to avoid calling size()
        if (o == this)
            return true;

        if (!(o instanceof Set))
            return false;

        Collection<?> c = (Collection<?>)o;

        try {
            return containsAll(c) && c.containsAll(this);
        }
        catch (ClassCastException ignored) {
            return false;
        }
        catch (NullPointerException ignored) {
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return super.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> c) {
        // Override AbstractSet version to avoid unnecessary call to size()
        boolean modified = false;

        for (Object o : c)
            if (remove(o))
                modified = true;

        return modified;
    }

    /** {@inheritDoc} */
    @Override public E lower(E e) {
        return m.lowerKey(e);
    }

    /** {@inheritDoc} */
    @Override public E floor(E e) {
        return m.floorKey(e);
    }

    /** {@inheritDoc} */
    @Override public E ceiling(E e) {
        return m.ceilingKey(e);
    }

    /** {@inheritDoc} */
    @Override public E higher(E e) {
        return m.higherKey(e);
    }

    /** {@inheritDoc} */
    @Nullable @Override public E pollFirst() {
        Map.Entry<E, Object> e = m.pollFirstEntry();

        return e == null ? null : e.getKey();
    }

    /** {@inheritDoc} */
    @Nullable @Override public E pollLast() {
        Map.Entry<E, Object> e = m.pollLastEntry();

        return e == null ? null : e.getKey();
    }

    /** {@inheritDoc} */
    @Override public Comparator<? super E> comparator() {
        return m.comparator();
    }

    /** {@inheritDoc} */
    @Override public E first() {
        return m.firstKey();
    }

    /**
     * Same as {@link #first()}, but returns {@code null} if set is empty.
     *
     * @return First entry or {@code null} if set is empty.
     */
    @Nullable public E firstx() {
        Map.Entry<E, Object> e = m.firstEntry();

        return e == null ? null : e.getKey();
    }

    /** {@inheritDoc} */
    @Override public E last() {
        return m.lastKey();
    }

    /**
     * Same as {@link #last()}, but returns {@code null} if set is empty.
     *
     * @return Last entry or {@code null} if set is empty.
     */
    @Nullable public E lastx() {
        Map.Entry<E, Object> e = m.lastEntry();

        return e == null ? null : e.getKey();
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
        return new GridConcurrentSkipListSet<>(m.subMap(fromElement, fromInclusive, toElement, toInclusive));
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<E> headSet(E toElement, boolean inclusive) {
        return new GridConcurrentSkipListSet<>(m.headMap(toElement, inclusive));
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
        return new GridConcurrentSkipListSet<>(m.tailMap(fromElement, inclusive));
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<E> subSet(E fromElement, E toElement) {
        return subSet(fromElement, true, toElement, false);
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<E> headSet(E toElement) {
        return headSet(toElement, false);
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<E> tailSet(E fromElement) {
        return tailSet(fromElement, true);
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<E> descendingSet() {
        return new GridConcurrentSkipListSet<>(m.descendingMap());
    }
}