/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.Arrays;
import java.util.Iterator;

/**
 * A very simple array list that supports concurrent access.
 * Internally, it uses immutable objects.
 *
 * @param <K> the key type
 */
public class ConcurrentArrayList<K> {

    /**
     * The array.
     */
    @SuppressWarnings("unchecked")
    K[] array = (K[]) new Object[0];

    /**
     * Get the first element, or null if none.
     *
     * @return the first element
     */
    public K peekFirst() {
        K[] a = array;
        return a.length == 0 ? null : a[0];
    }

    /**
     * Get the last element, or null if none.
     *
     * @return the last element
     */
    public K peekLast() {
        K[] a = array;
        int len = a.length;
        return len == 0 ? null : a[len - 1];
    }

    /**
     * Add an element at the end.
     *
     * @param obj the element
     */
    public synchronized void add(K obj) {
        if (obj == null) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "adding null value to list");
        }
        int len = array.length;
        array = Arrays.copyOf(array, len + 1);
        array[len] = obj;
    }

    /**
     * Remove the first element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public synchronized boolean removeFirst(K obj) {
        if (peekFirst() != obj) {
            return false;
        }
        int len = array.length;
        @SuppressWarnings("unchecked")
        K[] a = (K[]) new Object[len - 1];
        System.arraycopy(array, 1, a, 0, len - 1);
        array = a;
        return true;
    }

    /**
     * Remove the last element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public synchronized boolean removeLast(K obj) {
        if (peekLast() != obj) {
            return false;
        }
        array = Arrays.copyOf(array, array.length - 1);
        return true;
    }

    /**
     * Get an iterator over all entries.
     *
     * @return the iterator
     */
    public Iterator<K> iterator() {
        return new Iterator<K>() {

            K[] a = array;
            int index;

            @Override
            public boolean hasNext() {
                return index < a.length;
            }

            @Override
            public K next() {
                return a[index++];
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }

}
