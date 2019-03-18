/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Arrays;

import org.h2.engine.SysProperties;

/**
 * An array with integer element.
 */
public class IntArray {

    private int[] data;
    private int size;
    private int hash;

    /**
     * Create an int array with the default initial capacity.
     */
    public IntArray() {
        this(10);
    }

    /**
     * Create an int array with specified initial capacity.
     *
     * @param capacity the initial capacity
     */
    public IntArray(int capacity) {
        data = new int[capacity];
    }

    /**
     * Create an int array with the given values and size.
     *
     * @param data the int array
     */
    public IntArray(int[] data) {
        this.data = data;
        size = data.length;
    }

    /**
     * Append a value.
     *
     * @param value the value to append
     */
    public void add(int value) {
        if (size >= data.length) {
            ensureCapacity(size + size);
        }
        data[size++] = value;
    }

    /**
     * Get the value at the given index.
     *
     * @param index the index
     * @return the value
     */
    public int get(int index) {
        if (SysProperties.CHECK) {
            if (index >= size) {
                throw new ArrayIndexOutOfBoundsException("i=" + index + " size=" + size);
            }
        }
        return data[index];
    }

    /**
     * Remove the value at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        if (SysProperties.CHECK) {
            if (index >= size) {
                throw new ArrayIndexOutOfBoundsException("i=" + index + " size=" + size);
            }
        }
        System.arraycopy(data, index + 1, data, index, size - index - 1);
        size--;
    }

    /**
     * Ensure the the underlying array is large enough for the given number of
     * entries.
     *
     * @param minCapacity the minimum capacity
     */
    public void ensureCapacity(int minCapacity) {
        minCapacity = Math.max(4, minCapacity);
        if (minCapacity >= data.length) {
            data = Arrays.copyOf(data, minCapacity);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IntArray)) {
            return false;
        }
        IntArray other = (IntArray) obj;
        if (hashCode() != other.hashCode() || size != other.size) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (data[i] != other.data[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        int h = size + 1;
        for (int i = 0; i < size; i++) {
            h = h * 31 + data[i];
        }
        hash = h;
        return h;
    }

    /**
     * Get the size of the list.
     *
     * @return the size
     */
    public int size() {
        return size;
    }

    /**
     * Convert this list to an array. The target array must be big enough.
     *
     * @param array the target array
     */
    public void toArray(int[] array) {
        System.arraycopy(data, 0, array, 0, size);
    }

    @Override
    public String toString() {
        StatementBuilder buff = new StatementBuilder("{");
        for (int i = 0; i < size; i++) {
            buff.appendExceptFirst(", ");
            buff.append(data[i]);
        }
        return buff.append('}').toString();
    }

    /**
     * Remove a number of elements.
     *
     * @param fromIndex the index of the first item to remove
     * @param toIndex upper bound (exclusive)
     */
    public void removeRange(int fromIndex, int toIndex) {
        if (SysProperties.CHECK) {
            if (fromIndex > toIndex || toIndex > size) {
                throw new ArrayIndexOutOfBoundsException("from=" + fromIndex +
                        " to=" + toIndex + " size=" + size);
            }
        }
        System.arraycopy(data, toIndex, data, fromIndex, size - toIndex);
        size -= toIndex - fromIndex;
    }

}
