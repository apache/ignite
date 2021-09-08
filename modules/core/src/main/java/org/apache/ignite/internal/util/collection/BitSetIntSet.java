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
package org.apache.ignite.internal.util.collection;

import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.GridSerializableCollection;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.util.IgniteUtils.EMPTY_INTS;

/**
 * Set of Integers implementation based on BitSet.
 *
 * Implementation doesn't support negative values and null, cause we can't distinct null from 0 bit in BitSet.
 */
public class BitSetIntSet extends GridSerializableCollection<Integer> implements IntSet {
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
     * @param initCap initial capacity.
     */
    public BitSetIntSet(int initCap) {
        bitSet = new BitSet(initCap);
    }

    /**
     * @param initCap initial capacity.
     * @param coll initial collection.
     */
    public BitSetIntSet(int initCap, Collection<Integer> coll) {
        bitSet = new BitSet(initCap);
        addAll(coll);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        if (o == null)
            throw new NullPointerException("Null values are not supported!");

        return contains((int)o);
    }

    /** {@inheritDoc} */
    @Override public boolean contains(int element) {
        if (element < 0)
            return false;

        return bitSet.get(element);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            /** */
            private int idx = -1;

            /** */
            private int nextIdx = -1;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return nextIdx == idx ? (nextIdx = bitSet.nextSetBit(idx + 1)) != -1 : nextIdx != -1;
            }

            /** {@inheritDoc} */
            @Override public Integer next() {
                if (nextIdx == idx)
                    nextIdx = bitSet.nextSetBit(idx + 1);

                if (nextIdx == -1)
                    throw new NoSuchElementException();

                idx = nextIdx;

                return idx;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public boolean add(Integer integer) {
        if (integer == null)
            throw new IllegalArgumentException("Negative or null values are not supported!");

        return add((int)integer);
    }

    /** {@inheritDoc} */
    @Override public boolean add(int element) {
        if (element < 0)
            throw new IllegalArgumentException("Negative or null values are not supported!");

        boolean alreadySet = bitSet.get(element);

        if (!alreadySet) {
            bitSet.set(element);

            size++;

            return true;
        }
        else
            return false;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        if (o == null)
            return false;

        return remove((int)o);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(int element) {
        if (element < 0)
            return false;

        boolean alreadySet = bitSet.get(element);

        if (alreadySet) {
            bitSet.clear(element);

            size--;
        }

        return alreadySet;
    }

    /** {@inheritDoc} */
    @Override public int[] toIntArray() {
        if (size == 0)
            return EMPTY_INTS;

        int[] arr = new int[size];

        for (int i = 0, pos = -1; i < size; i++) {
            pos = bitSet.nextSetBit(pos + 1);
            arr[i] = pos;
        }

        return arr;
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
