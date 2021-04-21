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


package org.apache.ignite.internal.util.collection;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/** */
public class ImmutableIntSet implements IntSet {
    /** */
    private static final ImmutableIntSet EMPTY_SET = new ImmutableIntSet(new BitSetIntSet(1));

    /** Delegate. */
    private final Set<Integer> delegate;

    /**
     * @param delegate Delegate.
     */
    public static ImmutableIntSet wrap(Set<Integer> delegate) {
        return delegate instanceof ImmutableIntSet ? (ImmutableIntSet)delegate : new ImmutableIntSet(delegate);
    }

    /** */
    public static ImmutableIntSet emptySet() {
        return EMPTY_SET;
    }

    /**
     * @param delegate Delegate.
     */
    public ImmutableIntSet(Set<Integer> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(int element) {
        if (delegate instanceof IntSet)
            return ((IntSet)delegate).contains(element);
        else
            return delegate.contains(element);
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(Collection<?> coll) {
        return delegate.containsAll(coll);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return delegate.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        return delegate.contains(o);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Object[] toArray() {
        return delegate.toArray();
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> T[] toArray(@NotNull T[] a) {
        return delegate.toArray(a);
    }

    /** {@inheritDoc} */
    @Override public int[] toIntArray() {
        if (delegate instanceof IntSet)
            return ((IntSet)delegate).toIntArray();
        else
            return U.toIntArray(delegate);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return delegate.toString();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Integer> iterator() {
        return new Iterator<Integer>() {
            /** */
            private final Iterator<? extends Integer> iter = delegate.iterator();

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return iter.hasNext();
            }

            /** */
            @Override public Integer next() {
                return iter.next();
            }

            /** */
            @Override public void remove() {
                throw new UnsupportedOperationException();
            }

            /** */
            @Override public void forEachRemaining(Consumer<? super Integer> act) {
                iter.forEachRemaining(act);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public boolean add(int element) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean remove(int element) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean add(Integer integer) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(@NotNull Collection<? extends Integer> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void forEach(Consumer<? super Integer> act) {
        delegate.forEach(act);
    }

    /** {@inheritDoc} */
    @Override public boolean removeIf(Predicate<? super Integer> filter) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Integer> spliterator() {
        return delegate.spliterator();
    }

    /** {@inheritDoc} */
    @Override public Stream<Integer> stream() {
        return delegate.stream();
    }

    /** {@inheritDoc} */
    @Override public Stream<Integer> parallelStream() {
        return delegate.parallelStream();
    }
}
