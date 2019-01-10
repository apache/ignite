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
public class ImmutableBitSet implements Set<Integer> {
    /**
     * Wrapped BitSet.
     */
    private final BitSet bitSet;

    /**
     * Size.
     */
    private final int size;

    /**
     *
     * @param bitSet BitSet to wrap.
     */
    public ImmutableBitSet(BitSet bitSet) {
        this.bitSet = bitSet;

        int idx = 0;
        int size0 = 0;

        while ((idx = bitSet.nextSetBit(idx)) != -1) {
            size0++;

            idx++;
        }

        size = size0;
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
        return bitSet.get((int)o);
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
    @Override public @NotNull <I> I[] toArray(@NotNull I[] a) {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation.
     */
    @Override public boolean add(Integer integer) {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation.
     */
    @Override public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * @{inheritDoc}
     */
    @Override public boolean containsAll(@NotNull Collection<?> c) {
        return false;
    }

    /**
     * Unsupported operation.
     */
    @Override public boolean addAll(@NotNull Collection<? extends Integer> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation.
     */
    @Override public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation.
     */
    @Override public boolean removeAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation.
     */
    @Override public void clear() {
        throw new UnsupportedOperationException();
    }
}
