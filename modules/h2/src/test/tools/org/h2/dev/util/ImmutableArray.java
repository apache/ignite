/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.util.Arrays;
import java.util.Iterator;
import org.h2.mvstore.DataUtils;

/**
 * An immutable array.
 *
 * @param <K> the type
 */
public final class ImmutableArray<K> implements Iterable<K> {

    private static final ImmutableArray<?> EMPTY = new ImmutableArray<>(
            new Object[0]);

    /**
     * The array.
     */
    private final K[] array;

    private ImmutableArray(K[] array) {
        this.array = array;
    }

    /**
     * Get the entry at this index.
     *
     * @param index the index
     * @return the entry
     */
    public K get(int index) {
        return array[index];
    }

    /**
     * Get the length.
     *
     * @return the length
     */
    public int length() {
        return array.length;
    }

    /**
     * Set the entry at this index.
     *
     * @param index the index
     * @param obj the object
     * @return the new immutable array
     */
    public ImmutableArray<K> set(int index, K obj) {
        K[] array = this.array.clone();
        array[index] = obj;
        return new ImmutableArray<>(array);
    }

    /**
     * Insert an entry at this index.
     *
     * @param index the index
     * @param obj the object
     * @return the new immutable array
     */
    public ImmutableArray<K> insert(int index, K obj) {
        int len = array.length + 1;
        @SuppressWarnings("unchecked")
        K[] array = (K[]) new Object[len];
        DataUtils.copyWithGap(this.array, array, this.array.length, index);
        array[index] = obj;
        return new ImmutableArray<>(array);
    }

    /**
     * Remove the entry at this index.
     *
     * @param index the index
     * @return the new immutable array
     */
    public ImmutableArray<K> remove(int index) {
        int len = array.length - 1;
        @SuppressWarnings("unchecked")
        K[] array = (K[]) new Object[len];
        DataUtils.copyExcept(this.array, array, this.array.length, index);
        return new ImmutableArray<>(array);
    }

    /**
     * Get a sub-array.
     *
     * @param fromIndex the index of the first entry
     * @param toIndex the end index, plus one
     * @return the new immutable array
     */
    public ImmutableArray<K> subArray(int fromIndex, int toIndex) {
        return new ImmutableArray<>(Arrays.copyOfRange(array, fromIndex, toIndex));
    }

    /**
     * Create an immutable array.
     *
     * @param array the data
     * @return the new immutable array
     */
    @SafeVarargs
    public static <K> ImmutableArray<K> create(K... array) {
        return new ImmutableArray<>(array);
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
    public static <K> ImmutableArray<K> empty() {
        return (ImmutableArray<K>) EMPTY;
    }

    /**
     * Get an iterator over all entries.
     *
     * @return the iterator
     */
    @Override
    public Iterator<K> iterator() {
        return new Iterator<K>() {

            ImmutableArray<K> a = ImmutableArray.this;
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



