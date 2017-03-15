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

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * @param <E>
 */
public final class MPSCQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {

    /** */
    static final int INITIAL_ARRAY_SIZE = 512;
    /** */
    static final Node BLOCKED = new Node();

    /** */
    final AtomicReference<Node> putStack = new AtomicReference<>();
    /** */
    private final AtomicInteger takeStackSize = new AtomicInteger();

    /** */
    private Thread consumerThread;
    /** */
    private Object[] takeStack = new Object[INITIAL_ARRAY_SIZE];
    /** */
    private int takeStackIndex = -1;

    /**
     */
    public MPSCQueue(Thread consumerThread) {
        assert consumerThread != null;
        this.consumerThread = consumerThread;
    }

    /**
     */
    public MPSCQueue() {
    }

    /**
     * Sets the consumer thread.
     *
     * The consumer thread is needed for blocking, so that an offering known which thread
     * to wakeup. There can only be a single consumerThread and this method should be called
     * before the queue is safely published. It will not provide a happens before relation on
     * its own.
     *
     * @param consumerThread the consumer thread.
     * @throws NullPointerException when consumerThread null.
     */
    public void setConsumerThread(Thread consumerThread) {
        assert consumerThread != null;
        this.consumerThread = consumerThread;
    }

    /**
     * {@inheritDoc}.
     *
     * This call is threadsafe; but it will only remove the items that are on the put-stack.
     */
    @Override public void clear() {
        putStack.set(BLOCKED);
    }

    /** {@inheritDoc}. */
    @Override public boolean offer(E item) {
        assert item != null : "item can't be null";

        AtomicReference<Node> putStack = this.putStack;
        Node newHead = new Node();
        newHead.item = item;

        for (; ; ) {
            Node oldHead = putStack.get();
            if (oldHead == null || oldHead == BLOCKED) {
                newHead.next = null;
                newHead.size = 1;
            }
            else {
                newHead.next = oldHead;
                newHead.size = oldHead.size + 1;
            }

            if (!putStack.compareAndSet(oldHead, newHead))
                continue;

            if (oldHead == BLOCKED)
                unpark(consumerThread);

            return true;
        }
    }

    /** {@inheritDoc}. */
    @Override public E peek() {
        E item = peekNext();
        if (item != null)
            return item;

        if (!drainPutStack())
            return null;

        return peekNext();
    }

    /** {@inheritDoc}. */
    @Override public E take() throws InterruptedException {
        E item = next();

        if (item != null)
            return item;

        takeAll();
        assert takeStackIndex == 0;
        assert takeStack[takeStackIndex] != null;

        return next();
    }

    /** {@inheritDoc}. */
    @Override public E poll() {
        E item = next();

        if (item != null)
            return item;

        if (!drainPutStack())
            return null;

        return next();
    }

    /** */
    private E next() {
        E item = peekNext();

        if (item != null)
            dequeue();

        return item;
    }

    /** */
    private E peekNext() {
        if (takeStackIndex == -1)
            return null;

        if (takeStackIndex == takeStack.length) {
            takeStackIndex = -1;
            return null;
        }

        E item = (E)takeStack[takeStackIndex];

        if (item == null) {
            takeStackIndex = -1;
            return null;
        }

        return item;
    }

    /** */
    private void dequeue() {
        takeStack[takeStackIndex] = null;
        takeStackIndex++;
        takeStackSize.lazySet(takeStackSize.get() - 1);
    }

    /** */
    private void takeAll() throws InterruptedException {
        long iteration = 0;
        AtomicReference<Node> putStack = this.putStack;

        for (; ; ) {
            if (consumerThread != null && consumerThread.isInterrupted()) {
                putStack.compareAndSet(BLOCKED, null);
                throw new InterruptedException();
            }

            Node currentPutStackHead = putStack.get();

            if (currentPutStackHead == null) {
                // there is nothing to be take, so lets block.
                if (!putStack.compareAndSet(null, BLOCKED)) {
                    // we are lucky, something is available
                    continue;
                }

                // lets block for real.
                park();
            }
            else if (currentPutStackHead == BLOCKED)
                park();

            else {
                if (!putStack.compareAndSet(currentPutStackHead, null))
                    continue;

                copyIntoTakeStack(currentPutStackHead);
                break;
            }
            iteration++;
        }
    }

    /** */
    private boolean drainPutStack() {
        for (; ; ) {
            Node head = putStack.get();

            if (head == null)
                return false;

            if (putStack.compareAndSet(head, null)) {
                copyIntoTakeStack(head);
                return true;
            }
        }
    }

    /** */
    private void copyIntoTakeStack(Node putStackHead) {
        int putStackSize = putStackHead.size;

        takeStackSize.lazySet(putStackSize);

        if (putStackSize > takeStack.length)
            takeStack = new Object[nextPowerOfTwo(putStackHead.size)];

        for (int i = putStackSize - 1; i >= 0; i--) {
            takeStack[i] = putStackHead.item;
            putStackHead = putStackHead.next;
        }

        takeStackIndex = 0;
        assert takeStack[0] != null;
    }

    /**
     * {@inheritDoc}.
     *
     * Best effort implementation.
     */
    @Override public int size() {
        Node h = putStack.get();
        int putStackSize = h == null ? 0 : h.size;
        return putStackSize + takeStackSize.get();
    }

    /** {@inheritDoc}. */
    @Override public boolean isEmpty() {
        return size() == 0;
    }

    /** {@inheritDoc}. */
    @Override public void put(E e) throws InterruptedException {
        offer(e);
    }

    /** {@inheritDoc}. */
    @Override public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        add(e);
        return true;
    }

    /** {@inheritDoc}. */
    @Override public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc}. */
    @Override public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    /** {@inheritDoc}. */
    @Override public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc}. */
    @Override public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc}. */
    @Override public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    /** */
    private static int nextPowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    /** */
    private static final class Node<E> {
        /** */
        Node next;
        /** */
        E item;
        /** */
        int size;
    }
}
