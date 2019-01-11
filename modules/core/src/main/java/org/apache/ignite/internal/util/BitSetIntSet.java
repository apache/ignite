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

import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable BitSet wrapper, which also implements Set of Integers interface.
 */
public class BitSetIntSet implements Set<Integer> {
    /**
     * Wrapped BitSet.
     */
    private final BitSet bitSet;

    /**
     * Size.
     */
    private int size;

    /**
     *
     */
    public BitSetIntSet() {
        bitSet = new BitSet();
    }

    /**
     *
     * @param initCap initial capacity.
     */
    public BitSetIntSet(int initCap) {
        bitSet = new BitSet(initCap);

        int idx = 0;
        int size0 = 0;
    }

    /**
     * @{inheritDoc}
     */
    @Override public int size() {
        return size;
    }

    /**
     * @{inheritDoc}
     */
    @Override public boolean isEmpty() {
        return size == 0;
    }

    /**
     * @{inheritDoc}
     */
    @Override public boolean contains(Object o) {
        if (o == null)
            throw new UnsupportedOperationException("Null values are not supported!");

        int val = (int)o;

        if (val < 0)
            throw new UnsupportedOperationException("Negative values are not supported!");

        return bitSet.get(val);
    }

    /**
     * @{inheritDoc}
     */
    @Override public @NotNull Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            private int next = -1;

            /** @{inheritDoc} */
            @Override public boolean hasNext() {
                int nextBit = bitSet.nextSetBit(next + 1);

                if(nextBit != -1) {
                    next = nextBit;

                    return true;
                } else
                    return false;
            }

            /** @{inheritDoc} */
            @Override public Integer next() {
                if (next == -1)
                    throw new NoSuchElementException();

                return next;
            }
        };
    }

    /**
     * @{inheritDoc}
     */
    @Override public @NotNull Object[] toArray() {
        Object[] arr = new Object[size];

        Iterator<Integer> iterator = iterator();

        int idx = 0;

        while (iterator.hasNext())
            arr[idx++] = iterator.next();

        return arr;
    }

    /** @{inheritDoc} */
    @Override public @NotNull <T> T[] toArray(@NotNull T[] a) {
        T[] r = a.length >= size
            ? a
            :(T[])java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

        Iterator it = iterator();

        for (int i = 0; i < r.length; i++)
            r[i] = it.hasNext() ? (T)it.next() : null;

        return r;
    }

    /**
     * Unsupported operation.
     */
    @Override public boolean add(Integer integer) {
        if (integer == null || integer < 0)
            throw new UnsupportedOperationException("Negative or null values are not supported!");

        boolean alreadySet = bitSet.get(integer);

        if (!alreadySet) {
            bitSet.set(integer);

            size++;
        }

        return !alreadySet;
    }

    /**
     * Unsupported operation.
     */
    @Override public boolean remove(Object o) {
        if (o == null)
            throw new UnsupportedOperationException("Null values are not supported!");

        int val = (int)o;

        if (val < 0)
            throw new UnsupportedOperationException("Negative values are not supported!");

        boolean alreadySet = bitSet.get(val);

        if (alreadySet) {
            bitSet.clear(val);

            size--;
        }

        return alreadySet;
    }

    /**
     * @{inheritDoc}
     */
    @Override public boolean containsAll(@NotNull Collection<?> c) {
        for(Object o : c) {
            if (!contains(o))
                return false;
        }

        return true;
    }

    /**
     * @{inheritDoc}
     */
    @Override public boolean addAll(@NotNull Collection<? extends Integer> c) {
        boolean atLeastOneAdded = false;

        for(Integer o : c) {
            if (add(o))
                atLeastOneAdded = true;
        }

        return atLeastOneAdded;
    }

    /**
     * Unsupported operation.
     */
    @Override public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * @{inheritDoc}
     */
    @Override public boolean removeAll(@NotNull Collection<?> c) {
        boolean atLeastOneRemoved = false;

        for(Object o : c) {
            if (remove(o))
                atLeastOneRemoved = true;
        }

        return atLeastOneRemoved;
    }

    /**
     * @{inheritDoc}
     */
    @Override public void clear() {
        bitSet.clear();

        size = 0;
    }
}
