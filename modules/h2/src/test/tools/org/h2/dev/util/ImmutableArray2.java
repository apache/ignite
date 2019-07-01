/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.h2.mvstore.DataUtils;

/**
 * An immutable array.
 *
 * @param <K> the type
 */
public final class ImmutableArray2<K> implements Iterable<K> {

    private static final ImmutableArray2<?> EMPTY = new ImmutableArray2<>(
            new Object[0], 0);

    /**
     * The array.
     */
    private final K[] array;
    private final int length;
    private AtomicBoolean canExtend;

    private ImmutableArray2(K[] array, int len) {
        this.array = array;
        this.length = len;
    }

    private ImmutableArray2(K[] array, int len, boolean canExtend) {
        this.array = array;
        this.length = len;
        if (canExtend) {
            this.canExtend = new AtomicBoolean(true);
        }
    }

    /**
     * Get the entry at this index.
     *
     * @param index the index
     * @return the entry
     */
    public K get(int index) {
        if (index >= length) {
            throw new IndexOutOfBoundsException();
        }
        return array[index];
    }

    /**
     * Get the length.
     *
     * @return the length
     */
    public int length() {
        return length;
    }

    /**
     * Set the entry at this index.
     *
     * @param index the index
     * @param obj the object
     * @return the new immutable array
     */
    public ImmutableArray2<K> set(int index, K obj) {
        K[] a2 = Arrays.copyOf(array, length);
        a2[index] = obj;
        return new ImmutableArray2<>(a2, length);
    }

    /**
     * Insert an entry at this index.
     *
     * @param index the index
     * @param obj the object
     * @return the new immutable array
     */
    public ImmutableArray2<K> insert(int index, K obj) {
        int len = length + 1;
        int newLen = len;
        boolean extendable;
        if (index == len - 1) {
            AtomicBoolean x = canExtend;
            if (x != null) {
                // can set it to null early - we anyway
                // reset the flag, so it is no longer useful
                canExtend = null;
                if (array.length > index && x.getAndSet(false)) {
                    array[index] = obj;
                    return new ImmutableArray2<>(array, len, true);
                }
            }
            extendable = true;
            newLen = len + 4;
        } else {
            extendable = false;
        }
        @SuppressWarnings("unchecked")
        K[] a2 = (K[]) new Object[newLen];
        DataUtils.copyWithGap(array, a2, length, index);
        a2[index] = obj;
        return new ImmutableArray2<>(a2, len, extendable);
    }

    /**
     * Remove the entry at this index.
     *
     * @param index the index
     * @return the new immutable array
     */
    public ImmutableArray2<K> remove(int index) {
        int len = length - 1;
        if (index == len) {
            return new ImmutableArray2<>(array, len);
        }
        @SuppressWarnings("unchecked")
        K[] a2 = (K[]) new Object[len];
        DataUtils.copyExcept(array, a2, length, index);
        return new ImmutableArray2<>(a2, len);
    }

    /**
     * Get a sub-array.
     *
     * @param fromIndex the index of the first entry
     * @param toIndex the end index, plus one
     * @return the new immutable array
     */
    public ImmutableArray2<K> subArray(int fromIndex, int toIndex) {
        int len = toIndex - fromIndex;
        if (fromIndex == 0) {
            return new ImmutableArray2<>(array, len);
        }
        return new ImmutableArray2<>(Arrays.copyOfRange(array, fromIndex, toIndex), len);
    }

    /**
     * Create an immutable array.
     *
     * @param array the data
     * @return the new immutable array
     */
    @SafeVarargs
    public static <K> ImmutableArray2<K> create(K... array) {
        return new ImmutableArray2<>(array, array.length);
    }

    /**
     * Get the data.
     *
     * @return the data
     */
    public K[] array() {
        return array;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        for (K obj : this) {
            buff.append(' ').append(obj);
        }
        return buff.toString();
    }

    /**
     * Get an empty immutable array.
     *
     * @param <K> the key type
     * @return the array
     */
    @SuppressWarnings("unchecked")
    public static <K> ImmutableArray2<K> empty() {
        return (ImmutableArray2<K>) EMPTY;
    }

    /**
     * Get an iterator over all entries.
     *
     * @return the iterator
     */
    @Override
    public Iterator<K> iterator() {
        return new Iterator<K>() {

            ImmutableArray2<K> a = ImmutableArray2.this;
            int index;

            @Override
            public boolean hasNext() {
                return index < a.length();
            }

            @Override
            public K next() {
                return a.get(index++);
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }

}

