/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.util.Iterator;

import org.h2.mvstore.DataUtils;

/**
 * A ring buffer that supports concurrent access.
 *
 * @param <K> the key type
 */
public class ConcurrentRing<K> {

    /**
     * The ring buffer.
     */
    K[] buffer;

    /**
     * The read position.
     */
    volatile int readPos;

    /**
     * The write position.
     */
    volatile int writePos;

    @SuppressWarnings("unchecked")
    public ConcurrentRing() {
        buffer = (K[]) new Object[4];
    }

    /**
     * Get the first element, or null if none.
     *
     * @return the first element
     */
    public K peekFirst() {
        return buffer[getIndex(readPos)];
    }

    /**
     * Get the last element, or null if none.
     *
     * @return the last element
     */
    public K peekLast() {
        return buffer[getIndex(writePos - 1)];
    }

    /**
     * Add an element at the end.
     *
     * @param obj the element
     */
    public void add(K obj) {
        buffer[getIndex(writePos)] = obj;
        writePos++;
        if (writePos - readPos >= buffer.length) {
            // double the capacity
            @SuppressWarnings("unchecked")
            K[] b2 = (K[]) new Object[buffer.length * 2];
            for (int i = readPos; i < writePos; i++) {
                K x = buffer[getIndex(i)];
                int i2 = i & b2.length - 1;
                b2[i2] = x;
            }
            buffer = b2;
        }
    }

    /**
     * Remove the first element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public boolean removeFirst(K obj) {
        int p = readPos;
        int idx = getIndex(p);
        if (buffer[idx] != obj) {
            return false;
        }
        buffer[idx] = null;
        readPos = p + 1;
        return true;
    }

    /**
     * Remove the last element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public boolean removeLast(K obj) {
        int p = writePos;
        int idx = getIndex(p - 1);
        if (buffer[idx] != obj) {
            return false;
        }
        buffer[idx] = null;
        writePos = p - 1;
        return true;
    }

    /**
     * Get the index in the array of the given position.
     *
     * @param pos the position
     * @return the index
     */
    int getIndex(int pos) {
        return pos & (buffer.length - 1);
    }

    /**
     * Get an iterator over all entries.
     *
     * @return the iterator
     */
    public Iterator<K> iterator() {
        return new Iterator<K>() {

            int offset;

            @Override
            public boolean hasNext() {
                return readPos + offset < writePos;
            }

            @Override
            public K next() {
                if (buffer[getIndex(readPos + offset)] == null) {
                    System.out.println("" + readPos);
                    System.out.println("" + getIndex(readPos + offset));
                    System.out.println("null?");
                }
                return buffer[getIndex(readPos + offset++)];
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }

}
