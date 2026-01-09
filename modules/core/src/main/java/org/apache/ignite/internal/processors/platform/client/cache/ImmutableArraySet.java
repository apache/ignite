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
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * A lightweight, array-backed, immutable implementation of the {@link Set} interface.
 * <p>
 * This class provides a fixed set of elements, defined at construction time and backed
 * directly by the provided array. Most mutating and lookup operations are unsupported,
 * including {@code contains}, {@code toArray}, and all modification methods.
 * <p>
 * Intended for internal or performance-critical use cases where a lightweight,
 * iteration-only view of a set is sufficient.
 *
 * <p><strong>Important:</strong> This implementation does not enforce element uniqueness,
 * nor does it provide hash-based lookup or set operations. Use only when element identity,
 * equality, or duplication do not matter, or are guaranteed externally.
 *
 * @param <E> the type of elements maintained by this set
 */
public class ImmutableArraySet<E> implements Set<E>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Backing array for the elements of the set. */
    private final E[] elements;

    /**
     * Constructs an {@code ImmutableArraySet} backed by the given array.
     * The array must not be modified after passing it to the constructor.
     *
     * @param elements the backing array for this set
     */
    public ImmutableArraySet(E[] elements) {
        this.elements = elements;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return elements.length;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return elements.length == 0;
    }

    /**
     * Unsupported operation. This implementation does not support element lookup.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public boolean contains(Object o) {
        throw new UnsupportedOperationException("'contains' operation is not supported by ImmutableArraySet.");
    }

    /**
     * Returns an unmodifiable iterator over the elements in the set, in array order.
     *
     * @return an iterator over the elements of the set
     */
    @Override public @NotNull Iterator<E> iterator() {
        return new Iterator<E>() {
            private int idx = 0;

            @Override public boolean hasNext() {
                return idx < elements.length;
            }

            @Override public E next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return elements[idx++];
            }
        };
    }

    /**
     * Unsupported operation. Conversion to array is not supported.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public Object @NotNull [] toArray() {
        throw new UnsupportedOperationException("'toArray' operation is not supported by ImmutableArraySet.");
    }

    /**
     * Unsupported operation. Conversion to array is not supported.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public <T> T @NotNull [] toArray(T @NotNull [] a) {
        throw new UnsupportedOperationException("'toArray' operation is not supported by ImmutableArraySet.");
    }

    /**
     * Unsupported operation. This set is immutable.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public boolean add(E e) {
        throw new UnsupportedOperationException("'add' operation is not supported by ImmutableArraySet.");
    }

    /**
     * Unsupported operation. This set is immutable.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public boolean remove(Object o) {
        throw new UnsupportedOperationException("'remove' operation is not supported by ImmutableArraySet.");
    }

    /**
     * Unsupported operation. This implementation does not support bulk containment checks.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public boolean containsAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException("'containsAll' operation is not supported by ImmutableArraySet.");
    }

    /**
     * Unsupported operation. This set is immutable.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public boolean addAll(@NotNull Collection<? extends E> c) {
        throw new UnsupportedOperationException("'addAll' operation is not supported by ImmutableArraySet.");
    }

    /**
     * Unsupported operation. This set is immutable.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException("'retainAll' operation is not supported by ImmutableArraySet.");
    }

    /**
     * Unsupported operation. This set is immutable.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public boolean removeAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException("'removeAll' operation is not supported by ImmutableArraySet.");
    }

    /**
     * Unsupported operation. This set is immutable.
     *
     * @throws UnsupportedOperationException always
     */
    @Override public void clear() {
        throw new UnsupportedOperationException("'clear' operation is not supported by ImmutableArraySet.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ImmutableArraySet.class, this);
    }
}

