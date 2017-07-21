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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link Set} based on internal list rather than hash table.
 * The order of the internal list is either insertion order or defined by
 * optionally provided {@link Comparator}.
 * <p>
 * Note that if {@code 'strict'} flag is false, then this set allows inconsistencies
 * between {@link Object#equals(Object)} and {@link Comparator#compare(Object, Object)}
 * methods. The {@code equals(Object)} method will always be used to determine
 * final equality. This allows to have multiple elements for which {@code compare(Object)}
 * method returns {@code 0}, but {@code equals(Object)} method returns {@code false}. The
 * reverse is also true, where {@code equals(Object)} may return true, but
 * {@code compare(Object)} method may return non-zero values.
 * <p>
 * In {@code strict} mode, which is default, {@link Object#equals(Object)} and
 * {@link Comparator#compare(Object, Object)} methods must be absolutely consistent with each other.
 * <p>
 * This set does not allow {@code null} values.
 */
public class GridListSet<V> extends GridSerializableSet<V> implements Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Internal list. */
    @GridToStringInclude
    private LinkedList<V> vals = new LinkedList<>();

    /** Comparator. */
    @GridToStringInclude
    private Comparator<V> comp;

    /** Strict flag. */
    private boolean strict;

    /**
     * Creates unsorted list set. Values will be ordered in insertion order.
     */
    public GridListSet() {
        comp = null;
        strict = true;
    }

    /**
     * If comparator is not {@code null}, then sorted list set will be created.
     * Values will be sorted according to provided comparator.
     *
     * @param comp Optional comparator to sort values.
     */
    public GridListSet(@Nullable Comparator<V> comp) {
        this.comp = comp;

        strict = true;
    }

    /**
     * If comparator is not {@code null}, then sorted list set will be created.
     * Values will be sorted according to provided comparator.
     *
     * @param comp Optional comparator to sort values.
     * @param strict Strict flag.
     */
    public GridListSet(@Nullable Comparator<V> comp, boolean strict) {
        this.comp = comp;
        this.strict = strict;
    }

    /**
     * Copy constructor.
     *
     * @param copy Set to copy from.
     */
    public GridListSet(GridListSet<V> copy) {
        assert copy != null;

        comp = copy.comp;
        strict = copy.strict;

        addAll(copy);
    }

    /**
     * Gets value of {@code strict} flag for this set.
     *
     * @return Value of {@code strict} flag for this set.
     */
    public boolean strict() {
        return strict;
    }

    /**
     * Gets optional comparator for this set.
     *
     * @return Optional comparator for this set.
     */
    @Nullable public Comparator<V> comparator() {
        return comp;
    }

    /** {@inheritDoc} */
    @Override public boolean add(V val) {
        return addx(val) == null;
    }

    /**
     * Either adds a value to set or does nothing if value is already present.
     *
     * @param val Value to add.
     * @return The instance of value from this set or {@code null} if value was added.
     */
    @Nullable public V addx(V val) {
        A.notNull(val, "val");

        if (comp == null) {
            for (V v : vals)
                if (v.equals(val))
                    return v;

            vals.add(val);

            return null;
        }

        if (strict) {
            for (ListIterator<V> it = vals.listIterator(); it.hasNext();) {
                V v = it.next();

                // Prefer equals to comparator.
                if (v.equals(val))
                    return v;

                int c = comp.compare(v, val);

                if (c == 0)
                    throw new IllegalStateException("Inconsistent equals and compare methods.");

                if (c > 0) {
                    // Back up.
                    it.previous();

                    it.add(val);

                    return null;
                }
            }

            vals.add(val);

            return null;
        }

        // Full scan first.
        for (V v : vals)
            if (v.equals(val))
                return v;

        for (ListIterator<V> it = vals.listIterator(); it.hasNext();) {
            V v = it.next();

            if (comp.compare(v, val) > 0) {
                do {
                    // Back up.
                    v = it.previous();
                }
                while (comp.compare(v, val) == 0);

                it.add(val);

                return null;
            }
        }

        vals.add(val);

        return null;
    }

    /**
     * Removes the first element of this list.
     *
     * @return Removed element or {@code null} if list is empty.
     */
    @Nullable public V removeFirst() {
        return vals.isEmpty() ? null : vals.removeFirst();
    }

    /**
     * Removes the last element of this list.
     *
     * @return Removed element or {@code null} if list is empty.
     */
    @Nullable public V removeLast() {
        return vals.isEmpty() ? null : vals.removeLast();
    }

    /**
     * Gets first element of this list.
     *
     * @return First element or {@code null} if list is empty.
     */
    @Nullable public V first() {
        return vals.isEmpty() ? null : vals.getFirst();
    }

    /**
     * Gets last element of this list.
     *
     * @return Last element or {@code null} if list is empty.
     */
    @Nullable public V last() {
        return vals.isEmpty() ? null : vals.getLast();
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    @Override public boolean remove(Object val) {
        A.notNull(val, "val");

        return removex((V)val) != null;
    }

    /**
     * Removes given value from the set and returns the instance stored in the set
     * or {@code null} if value was not found.
     *
     * @param val Value to remove.
     * @return The instance that was stored in the set or {@code null}.
     */
    @Nullable public V removex(V val) {
        A.notNull(val, "val");

        if (comp == null || !strict) {
            for (Iterator<V> it = vals.iterator(); it.hasNext();) {
                V v = it.next();

                if (v.equals(val)) {
                    it.remove();

                    return v;
                }
            }

            return null;
        }

        assert comp != null && strict;

        for (Iterator<V> it = vals.iterator(); it.hasNext();) {
            V v = it.next();

            // Prefer equals to comparator.
            if (v.equals(val)) {
                it.remove();

                return v;
            }

            if (comp.compare(v, val) > 0)
                break;
        }

        return null;
    }

    /**
     * Gets instance {@code e} stored in this set for which {@code e.equals(val)}
     * returns {@code true}.
     *
     * @param val Value to check for equality.
     * @return Instance stored in this set for which {@code e.equals(val)}
     *      returns {@code true}.
     */
    @Nullable public V get(V val) {
        A.notNull(val, "val");

        if (comp == null || !strict) {
            for (V v : vals)
                if (v.equals(val))
                    return v;

            return null;
        }

        assert comp != null && strict;

        for (V v : vals) {
            // Prefer equals to comparator.
            if (v.equals(val))
                return v;

            if (comp.compare(v, val) > 0)
                break;
        }

        return null;
    }

    /**
     * Gets value at given index within internal list. Note that this method will iterate through
     * the list to get a value at the specified index.
     *
     * @param idx Index to get value at (must be non-negative and less than {@link #size()}).
     * @return Value at give index.
     */
    public V get(int idx) {
        A.ensure(idx >= 0 && idx < size(), "idx >= 0 && idx < size()");

        return vals.get(idx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    @Override public boolean contains(Object val) {
        A.notNull(val, "val");

        if (comp != null && strict) {
            for (V v : vals) {
                // Prefer equals to comparator.
                if (v.equals(val))
                    return true;

                if (comp.compare(v, (V)val) > 0)
                    break;
            }

            return false;
        }

        return vals.contains(val);
    }

    /**
     * Creates a copy of this set.
     *
     * @return Copy of this set.
     */
    public GridListSet<V> copy() {
        return new GridListSet<>(this);
    }

    /** {@inheritDoc} */
    @Override public Iterator<V> iterator() {
        return vals.iterator();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return vals.size();
    }

    /**
     * Gets a copy of the internal list.
     *
     * @return Copy of the internal list.
     */
    public List<V> values() {
        return new ArrayList<>(vals);
    }

    /**
     * Clones this set.
     *
     * @return Clone of this set.
     * @throws CloneNotSupportedException
     */
    @SuppressWarnings( {"unchecked", "OverriddenMethodCallDuringObjectConstruction"})
    @Override protected Object clone() throws CloneNotSupportedException {
        GridListSet<V> clone = (GridListSet<V>)super.clone();

        clone.vals = (LinkedList<V>)vals.clone();
        clone.comp = comp;
        clone.strict = strict;

        return clone;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridListSet.class, this);
    }

    /**
     * Creates a synchronized instance of this set.
     *
     * @return Synchronized instance of this set.
     */
    public GridListSet<V> toSynchronized() {
        final GridListSet<V> set = this;

        return new GridListSet<V>() {
            @Override public synchronized boolean add(V val) {
                return set.add(val);
            }

            @Override public synchronized V addx(V val) {
                return set.addx(val);
            }

            @SuppressWarnings( {"CloneDoesntCallSuperClone"})
            @Override public synchronized Object clone() throws CloneNotSupportedException {
                return set.clone();
            }

            @Override public synchronized boolean contains(Object val) {
                return set.contains(val);
            }

            @Override public synchronized GridListSet<V> copy() {
                return set.copy();
            }

            @Override public synchronized V get(int idx) {
                return set.get(idx);
            }

            @Override public synchronized V get(V val) {
                return set.get(val);
            }

            @Override public synchronized Iterator<V> iterator() {
                return set.iterator();
            }

            @Override public synchronized boolean remove(Object val) {
                return set.remove(val);
            }

            @Override public synchronized V removex(V val) {
                return set.removex(val);
            }

            @Override public synchronized int size() {
                return set.size();
            }

            @Override public synchronized String toString() {
                return set.toString();
            }

            @Override public synchronized GridListSet<V> toSynchronized() {
                return set.toSynchronized();
            }

            @Override public synchronized boolean equals(Object o) {
                return set.equals(o);
            }

            @Override public synchronized int hashCode() {
                return set.hashCode();
            }

            @Override public synchronized boolean removeAll(Collection<?> c) {
                return set.removeAll(c);
            }

            @Override public synchronized boolean addAll(Collection<? extends V> c) {
                return set.addAll(c);
            }

            @Override public synchronized void clear() {
                set.clear();
            }

            @Override public synchronized boolean containsAll(Collection<?> c) {
                return set.containsAll(c);
            }

            @Override public synchronized boolean isEmpty() {
                return set.isEmpty();
            }

            @Override public synchronized boolean retainAll(Collection<?> c) {
                return set.retainAll(c);
            }

            @Override public synchronized Object[] toArray() {
                return set.toArray();
            }

            @SuppressWarnings( {"SuspiciousToArrayCall"})
            @Override public synchronized <T> T[] toArray(T[] a) {
                return set.toArray(a);
            }

            @Override public synchronized V first() {
                return set.first();
            }

            @Override public synchronized V removeFirst() {
                return set.removeFirst();
            }

            @Override public synchronized List<V> values() {
                return set.values();
            }

            @Override public V last() {
                return set.last();
            }

            @Override public V removeLast() {
                return set.removeLast();
            }
        };
    }
}