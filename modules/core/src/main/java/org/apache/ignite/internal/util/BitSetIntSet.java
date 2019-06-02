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
 * Set of Integers implementation based on BitSet.
 *
 * Implementation doesn't support negative values and null, cause we can't distinct null from 0 bit in BitSet.
 */
public class BitSetIntSet extends GridSerializableCollection<Integer> implements Set<Integer> {
    /** */
    private static final long serialVersionUID = 0L;

    /** BitSet. */
    private final BitSet bitSet;

    /** Calculated size. */
    private int size;

    /** */
    public BitSetIntSet() {
        bitSet = new BitSet();
    }

    /**
     *
     * @param initCap initial capacity.
     */
    public BitSetIntSet(int initCap) {
        bitSet = new BitSet(initCap);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        if (o == null)
            throw new UnsupportedOperationException("Null values are not supported!");

        int val = (int)o;

        if (val < 0)
            throw new UnsupportedOperationException("Negative values are not supported!");

        return bitSet.get(val);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            private int next = -1;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                int nextBit = bitSet.nextSetBit(next + 1);

                if (nextBit != -1) {
                    next = nextBit;

                    return true;
                }
                else
                    return false;
            }

            /** {@inheritDoc} */
            @Override public Integer next() {
                if (next == -1)
                    throw new NoSuchElementException();

                return next;
            }
        };
    }

    /** Unsupported operation. */
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

    /** Unsupported operation. */
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

    /** {@inheritDoc} */
    @Override public boolean containsAll(@NotNull Collection<?> c) {
        for (Object o : c) {
            if (!contains(o))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(@NotNull Collection<? extends Integer> c) {
        boolean atLeastOneAdded = false;

        for (Integer o : c) {
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

    /** {@inheritDoc} */
    @Override public boolean removeAll(@NotNull Collection<?> c) {
        boolean atLeastOneRemoved = false;

        for (Object o : c) {
            if (remove(o))
                atLeastOneRemoved = true;
        }

        return atLeastOneRemoved;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        bitSet.clear();

        size = 0;
    }
}
