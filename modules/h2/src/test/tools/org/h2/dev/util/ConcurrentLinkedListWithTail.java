/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.util.Iterator;

import org.h2.mvstore.DataUtils;

/**
 * A very simple linked list that supports concurrent access.
 *
 * @param <K> the key type
 */
public class ConcurrentLinkedListWithTail<K> {

    /**
     * The first entry (if any).
     */
    volatile Entry<K> head;

    /**
     * The last entry (if any).
     */
    private volatile Entry<K> tail;

    /**
     * Get the first element, or null if none.
     *
     * @return the first element
     */
    public K peekFirst() {
        Entry<K> x = head;
        return x == null ? null : x.obj;
    }

    /**
     * Get the last element, or null if none.
     *
     * @return the last element
     */
    public K peekLast() {
        Entry<K> x = tail;
        return x == null ? null : x.obj;
    }

    /**
     * Add an element at the end.
     *
     * @param obj the element
     */
    public void add(K obj) {
        Entry<K> x = new Entry<>(obj);
        Entry<K> t = tail;
        if (t != null) {
            t.next = x;
        }
        tail = x;
        if (head == null) {
            head = x;
        }
    }

    /**
     * Remove the first element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public boolean removeFirst(K obj) {
        Entry<K> x = head;
        if (x == null || x.obj != obj) {
            return false;
        }
        if (head == tail) {
            tail = x.next;
        }
        head = x.next;
        return true;
    }

    /**
     * Remove the last element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public boolean removeLast(K obj) {
        Entry<K> x = head;
        if (x == null) {
            return false;
        }
        Entry<K> prev = null;
        while (x.next != null) {
            prev = x;
            x = x.next;
        }
        if (x.obj != obj) {
            return false;
        }
        if (prev != null) {
            prev.next = null;
        }
        if (head == tail) {
            head = prev;
        }
        tail = prev;
        return true;
    }

    /**
     * Get an iterator over all entries.
     *
     * @return the iterator
     */
    public Iterator<K> iterator() {
        return new Iterator<K>() {

            Entry<K> current = head;

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public K next() {
                K x = current.obj;
                current = current.next;
                return x;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }

    /**
     * An entry in the linked list.
     */
    private static class Entry<K> {
        final K obj;
        Entry<K> next;

        Entry(K obj) {
            this.obj = obj;
        }
    }

}
