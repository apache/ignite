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

import java.util.AbstractCollection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Queue which supports addition at tail and removing at head. This
 * queue also exposes its internal linked list nodes and allows for
 * constant time removal from the middle of the queue.
 * <p>
 * This queue is not thread-safe.
 */
public class GridQueue<E> extends AbstractCollection<E> implements Queue<E> {
    /** Queue size. */
    private int size;

    /** Modification count. */
    private int modCnt;

    /** Queue header. */
    private Node<E> hdr = new Node<>(null, null, null);

    /**
     * Creates empty queue.
     */
    public GridQueue() {
        hdr.next = hdr.prev = hdr;
    }

    /**
     * Handles modification count check.
     *
     * @param match Modification count to match.
     */
    private void checkModCount(int match) {
        if (modCnt != match)
            throw new ConcurrentModificationException("Mod count mismatch [expected=" + match +
                ", actual=" + modCnt + ']');

        modCnt++;
    }

    /**
     * Adds element before node.
     *
     * @param e Element to add.
     * @param n Node.
     * @return New node.
     */
    private Node<E> addBefore(E e, Node<E> n) {
        A.notNull(e, "e");

        assert n != null;

        int match = modCnt;

        Node<E> newNode = new Node<>(e, n, n.prev);

        // Link.
        newNode.prev.next = newNode;
        newNode.next.prev = newNode;

        size++;

        checkModCount(match);

        return newNode;
    }

    /**
     * Removes node.
     *
     * @param n Node to remove.
     * @return Removed value.
     */
    private E remove(Node<E> n) {
        assert n != null;

        if (n == hdr)
            throw new NoSuchElementException();

        assert !n.unlinked();

        int match = modCnt;

        E res = n.item;

        // Relink.
        n.prev.next = n.next;
        n.next.prev = n.prev;

        // GC.
        n.next = n.prev = null;
        n.item = null;

        size--;

        checkModCount(match);

        n.unlink();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        offer(e);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        A.notNull(o, "o");

        for (Node<E> n = hdr.next; n != hdr; n = n.next) {
            if (o.equals(n.item)) {
                remove(n);

                return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean offer(E e) {
        addBefore(e, hdr);

        return true;
    }

    /**
     * Same as {@link #offer(Object)}, but returns created node.
     *
     * @param e Element to add.
     * @return New node.
     */
    public Node<E> offerx(E e) {
        return addBefore(e, hdr);
    }

    /**
     * Polls element from head of the queue.
     *
     * @return Polled element.
     */
    @Nullable @Override public E poll() {
        if (size == 0)
            return null;

        return remove(hdr.next);
    }

    /** {@inheritDoc} */
    @Override public E element() {
        Node<E> n = hdr.next;

        if (n == null)
            throw new NoSuchElementException();

        return n.item;
    }

    /** {@inheritDoc} */
    @Override public E remove() {
        E item = poll();

        if (item == null)
            throw new NoSuchElementException();

        return item;
    }

    /** {@inheritDoc} */
    @Nullable @Override public E peek() {
        return hdr.next.item;
    }

    /**
     * @return Peeks at first node in the queue.
     */
    public Node<E> peekx() {
        return hdr.next == hdr ? null : hdr.next;
    }

    /**
     * Unlinks node from the queue.
     *
     * @param n Node to unlink.
     */
    public void unlink(Node<E> n) {
        A.notNull(n, "n");

        remove(n);
    }

    /**
     * Gets queue size.
     *
     * @return Queue size.
     */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        return new QueueIterator();
    }

    /**
     * Node for internal linked list.
     *
     * @param <E> Queue element.
     */
    @SuppressWarnings( {"PublicInnerClass"})
    public static class Node<E> {
        /** Item. */
        private E item;

        /** Next. */
        @GridToStringExclude
        private Node<E> next;

        /** Previous. */
        @GridToStringExclude
        private Node<E> prev;

        /** Unlinked flag. */
        private boolean unlinked;

        /**
         * @param item Item.
         * @param next Next link.
         * @param prev Previous link.
         */
        private Node(E item, Node<E> next, Node<E> prev) {
            this.item = item;
            this.next = next;
            this.prev = prev;
        }

        /**
         * Gets this node's item.
         *
         * @return This node's item.
         */
        public E item() {
            return item;
        }

        /**
         * Sets unlinked flag.
         */
        void unlink() {
            assert !unlinked;

            unlinked = true;
        }

        /**
         * Checks if node is unlinked.
         *
         * @return {@code True} if node is unlinked.
         */
        public boolean unlinked() {
            return unlinked;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Node.class, this);
        }
    }

    /**
     * Iterator.
     */
    private class QueueIterator implements Iterator<E> {
        /** Next element. */
        private Node<E> next;

        /** Expected modification count. */
        private int expModCnt = modCnt;

        /**
         *
         */
        QueueIterator() {
            next = hdr.next;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != hdr;
        }

        /** {@inheritDoc} */
        @Override public E next() {
            checkModCount();

            if (next == null)
                throw new NoSuchElementException();

            E ret = next.item;

            next = next.next;

            return ret;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * Checks modification count.
         */
        private void checkModCount() {
            if (modCnt != expModCnt)
                throw new ConcurrentModificationException("Mod count mismatch [expected=" + expModCnt +
                    ", actual=" + modCnt + ']');
        }
    }
}