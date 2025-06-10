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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Set implementation without extra overhead related to internal hash buckets or Entry objects.
 * Backed by an underlying array. Allows duplicates for fast sequential collection, no uniqueness enforcement.
 * Note: For internal use only
 */
public class GridArraySet<E> implements Set<E>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int DEFAULT_CAPACITY = 10;

    /** */
    private static final float GROWTH_FACTOR = 1.5f;

    /** */
    private E[] elements;

    /** */
    private int size;

    /** */
    public GridArraySet() {
        this(DEFAULT_CAPACITY);
    }

    /** */
    @SuppressWarnings("unchecked")
    public GridArraySet(int capacity) {
        elements = (E[])new Object[capacity];
        size = 0;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return size == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        throw new UnsupportedOperationException("'contains' operation is not supported by GridArraySet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull Iterator<E> iterator() {
        return new Iterator<E>() {
            private int index = 0;

            @Override public boolean hasNext() {
                return index < size;
            }

            @Override public E next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return elements[index++];
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Object @NotNull [] toArray() {
        return Arrays.copyOf(elements, size);
    }

    /** {@inheritDoc} */
    @Override public <T> T @NotNull [] toArray(T[] a) {
        if (a.length < size)
            return (T[])Arrays.copyOf(elements, size, a.getClass());

        System.arraycopy(elements, 0, a, 0, size);

        if (a.length > size)
            a[size] = null;

        return a;
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        ensureCapacity();

        elements[size++] = e;

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        throw new UnsupportedOperationException("'remove' operation is not supported by GridArraySet.");
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("'containsAll' operation is not supported by GridArraySet.");
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(Collection<? extends E> c) {
        ensureCapacity(size + c.size());

        for (E e : c)
            add(e);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("'retainAll' operation is not supported by GridArraySet.");
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("'removeAll' operation is not supported by GridArraySet.");
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        Arrays.fill(elements, 0, size, null);

        size = 0;
    }

    /** */
    private void ensureCapacity() {
        ensureCapacity(size + 1);
    }

    /** */
    private void ensureCapacity(int minCapacity) {
        if (minCapacity > elements.length) {
            int newCapacity = Math.max((int)(elements.length * GROWTH_FACTOR), minCapacity);
            elements = Arrays.copyOf(elements, newCapacity);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridArraySet.class, this);
    }
}

