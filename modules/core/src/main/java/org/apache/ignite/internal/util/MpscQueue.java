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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * MP-SC concurrent linked queue implementation based on Dmitry Vyukov's <a href="http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue">
 * Non-intrusive MPSC node-based queue</a>.
 */
@SuppressWarnings({"WeakerAccess", "PackageVisibleField", "unused"})
public final class MpscQueue<E> extends Head<E> {
    /** Padding. */
    long p00, p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16, p17;

    public MpscQueue() {
        Node node = new Node(null);

        tail = node;
        head = node;
    }
}

/**
 * Head of {@link MpscQueue}.
 */
@SuppressWarnings({"WeakerAccess", "AtomicFieldUpdaterIssues", "ClassNameDiffersFromFileName"})
abstract class Head<E> extends PaddingL1<E> {
    /** Head field updater. */
    private static final AtomicReferenceFieldUpdater<Head, Node> updater =
        AtomicReferenceFieldUpdater.newUpdater(Head.class, Node.class, "head");

    /** Head. */
    protected volatile Node head;

    /**
     * Poll element.
     *
     * @return Element.
     */
    @SuppressWarnings("unchecked")
    public E poll() {
        Node node = peekNode();

        if (node != null) {
            Object val = node.value();

            node.value(null);

            updater.lazySet(this, node);

            return (E)val;
        }
        else
            return null;
    }


    /**
     * @return queue size.
     */
    public int size() {
        Node node = peekNode();

        int size = 0;

        for (;;) {
            if (node == null || node.value() == null)
                break;

            Node next = node.next();

            if (node == next)
                break;

            node = next;

            if (++size == Integer.MAX_VALUE)
                break;
        }

        return size;
    }

    /** {@inheritDoc} */
    public String toString() {
        Node node = peekNode();

        StringBuilder sb = new StringBuilder().append('[');

        for (;;) {
            if (node == null)
                break;

            Object value = node.value();

            if (value == null)
                break;

            if(sb.length() > 1)
                sb.append(',').append(' ');

            sb.append(value);

            Node next = node.next();

            if (node == next)
                break;

            node = next;
        }

        return sb.append(']').toString();
    }

    /**
     * @return The node after the head of the queue (the first element in the queue).
     */
    private Node peekNode() {
        Node head = this.head;
        Node next = head.next();

        if (next == null && head != tail) {
            do {
                next = head.next();
            } while (next == null);
        }
        return next;
    }
}

/**
 * Padding.
 */
@SuppressWarnings({"PackageVisibleField", "ClassNameDiffersFromFileName", "unused"})
abstract class PaddingL1<E> extends Tail<E> {
    /** Padding. */
    long p00, p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16, p17;
}

/**
 * Tail of {@link MpscQueue}.
 */
@SuppressWarnings({"ClassNameDiffersFromFileName", "AtomicFieldUpdaterIssues"})
abstract class Tail<E> extends PaddingL0 {
    /** Tail field updater. */
    private static final AtomicReferenceFieldUpdater<Tail, Node> updater =
        AtomicReferenceFieldUpdater.newUpdater(Tail.class, Node.class, "tail");

    /** Tail. */
    protected volatile Node tail;

    /**
     * Offer element.
     *
     * @param e Element.
     */
    public void offer(final E e) {
        if (e == null)
            throw new IllegalArgumentException("Null are not allowed.");

        Node newTail = new Node(e);

        Node prevTail = updater.getAndSet(this, newTail);

        prevTail.next(newTail);
    }
}

/**
 * Padding.
 */
@SuppressWarnings({"PackageVisibleField", "ClassNameDiffersFromFileName", "unused"})
abstract class PaddingL0 {
    /** Padding. */
    long p00, p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16, p17;
}

@SuppressWarnings({"UnusedDeclaration", "ClassNameDiffersFromFileName"})
final class Node {
    /** Next field updater. */
    private static final AtomicReferenceFieldUpdater<Node, Node> updater =
        AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");

    /** Value. */
    private Object val;

    /** Next node. */
    private volatile Node next;

    /**
     * Constructor.
     *
     * @param val Value.
     */
    Node(Object val) {
        this.val = val;
    }

    /**
     * Set next node.
     *
     * @param next Next node.
     */
    void next(Node next) {
        updater.lazySet(this, next);
    }

    /** Value. */
    Object value() {
        return val;
    }

    void value(Object val) {
        this.val = val;
    }

    /** Next node. */
    Node next() {
        return next;
    }
}