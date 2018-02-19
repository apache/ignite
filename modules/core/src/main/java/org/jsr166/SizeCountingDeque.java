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

package org.jsr166;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.jetbrains.annotations.NotNull;

/**
 * {@link java.util.Deque} decorator maintaining element counter on mutative operations.
 * <p />
 * Implementation is thread-safe if both {@link Counter} and underlying {@link Deque} are thread-safe.
 *
 * @param <E> Deque element type
 * @param <N> Number type that is used to count.
 */
public class SizeCountingDeque<E, N extends Number> implements Deque<E> {

    /** */
    public interface Counter<N> {
        /** */
        void inc();

        /** */
        void dec();

        /** */
        void add(int n);

        /** */
        N get();
    }

    /** */
    private class Iter implements Iterator<E> {
        /** */
        private final Iterator<E> iter;

        /** */
        private Iter(Iterator<E> iter) {
            this.iter = iter;
        }

        /** */
        @Override public boolean hasNext() {
            return iter.hasNext();
        }

        /** */
        @Override public E next() {
            return iter.next();
        }

        /** */
        @Override public void remove() {
            iter.remove();

            cntr.dec();
        }

        /** */
        @Override public void forEachRemaining(Consumer<? super E> consumer) {
            iter.forEachRemaining(consumer);
        }
    }

    /** */
    private final Deque<E> deque;

    /** */
    private final Counter<N> cntr;

    /** Creates a decorator.
     *
     * @param deque Deque being decorated.
     */
    public SizeCountingDeque(Deque<E> deque, Counter<N> cntr) {
        this.deque = Objects.requireNonNull(deque);
        this.cntr = Objects.requireNonNull(cntr);
    }

    /** {@inheritDoc} */
    @Override public void addFirst(E e) {
        deque.addFirst(e);

        cntr.inc();
    }

    /** {@inheritDoc} */
    @Override public void addLast(E e) {
        deque.addLast(e);

        cntr.inc();
    }

    /** {@inheritDoc} */
    @Override public boolean offerFirst(E e) {
        boolean res = deque.offerFirst(e);

        if (res)
            cntr.inc();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean offerLast(E e) {
        boolean res = deque.offerLast(e);

        if (res)
            cntr.inc();

        return res;
    }

    /** {@inheritDoc} */
    @Override public E removeFirst() {
        E res = deque.removeFirst();

        cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public E removeLast() {
        E res = deque.removeLast();

        cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public E pollFirst() {
        E res = deque.pollFirst();

        if (res != null)
            cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public E pollLast() {
        E res = deque.pollFirst();

        if (res != null)
            cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public E getFirst() {
        return deque.getFirst();
    }

    /** {@inheritDoc} */
    @Override public E getLast() {
        return deque.getLast();
    }

    /** {@inheritDoc} */
    @Override public E peekFirst() {
        return deque.peekFirst();
    }

    /** {@inheritDoc} */
    @Override public E peekLast() {
        return deque.peekLast();
    }

    /** {@inheritDoc} */
    @Override public boolean removeFirstOccurrence(Object o) {
        boolean res = deque.removeFirstOccurrence(o);

        if (res)
            cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean removeLastOccurrence(Object o) {
        boolean res = deque.removeLastOccurrence(o);

        if (res)
            cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        boolean alwaysTrue = deque.add(e);

        cntr.inc();

        return alwaysTrue;
    }

    /** {@inheritDoc} */
    @Override public boolean offer(E e) {
        boolean res = deque.offer(e);

        if (res)
            cntr.inc();

        return res;
    }

    /** {@inheritDoc} */
    @Override public E remove() {
        E res = deque.remove();

        cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public E poll() {
        E res = deque.poll();

        if (res != null)
            cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public E element() {
        return deque.element();
    }

    /** {@inheritDoc} */
    @Override public E peek() {
        return deque.peek();
    }

    /** {@inheritDoc} */
    @Override public void push(E e) {
        deque.push(e);

        cntr.inc();
    }

    /** {@inheritDoc} */
    @Override public E pop() {
        E res = deque.pop();

        cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        boolean res = deque.remove(o);

        if (res)
            cntr.dec();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(@NotNull Collection<?> col) {
        return deque.containsAll(col);
    }

    /**
     * Adds all of the elements in the specified collection at the end of this deque.
     * <p />
     * Note: If collection being added can mutate concurrently, or underlying deque implementation allows partial
     * insertion, then subsequent calls to {@link #size()} can report incorrect value.
     *
     * @param col The elements to be inserted into this deque.
     * @return {@code true} if this deque changed as a result of the call.
     */
    @Override public boolean addAll(@NotNull Collection<? extends E> col) {
        int colSize = col.size();

        boolean res = deque.addAll(col);

        if (res)
            cntr.add(colSize);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(@NotNull Collection<?> col) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean removeIf(Predicate<? super E> pred) {
        // Default implementation in Collection works through iterator, hence the adder is kept consistent.
        // But Deque implementations can override default behavior, so we'd better to prohibit the operation.
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(@NotNull Collection<?> col) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        while (pollFirst() != null) {
            // No-op.
        }
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        return deque.contains(o);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return cntr.get().intValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return cntr.get().intValue() == 0;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<E> iterator() {
        return new Iter(deque.iterator());
    }

    /** {@inheritDoc} */
    @NotNull @Override public Object[] toArray() {
        return deque.toArray();
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> T[] toArray(@NotNull T[] ts) {
        return deque.toArray(ts);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<E> descendingIterator() {
        return new Iter(deque.descendingIterator());
    }
}
