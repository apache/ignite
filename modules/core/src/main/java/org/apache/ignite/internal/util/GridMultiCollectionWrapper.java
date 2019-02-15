/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;

/**
 * Wrapper around several collection, don't allow adding new elements.
 * @param <E>
 */
public class GridMultiCollectionWrapper<E> implements Collection<E>{
    /** Collections. */
    public final Collection<E>[] collections;

    /**
     * @param collections Collections.
     */
    public GridMultiCollectionWrapper(Collection<E> ... collections) {
        this.collections = collections;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        int size = 0;

        for (Collection<E> collection : collections)
            size += collection.size();

        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        for (Collection<E> collection : collections) {
            if (!collection.isEmpty())
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        for (Collection<E> collection : collections) {
            if (collection.contains(o))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<E> iterator() {
        if (collections.length == 0)
            return Collections.emptyIterator();

        return new Iterator<E>() {
            int currCol = 0;

            Iterator<E> currIter = collections[0].iterator();

            @Override public boolean hasNext() {
                if (currIter.hasNext())
                    return true;

                while (true) {
                    currCol++;

                    if (currCol < collections.length)
                        currIter = collections[currCol].iterator();
                    else
                        return false;

                    if (currIter.hasNext())
                        return true;
                }
            }

            @Override public E next() {
                if (hasNext())
                    return currIter.next();

                throw new NoSuchElementException();
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** {@inheritDoc} */
    @NotNull @Override public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> T[] toArray(@NotNull T[] a) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        boolean rmv = false;

        for (Collection<E> collection : collections)
            rmv |= collection.remove(o);

        return rmv;
    }

    /** {@inheritDoc} */
    @Override public boolean containsAll(@NotNull Collection<?> c) {
        for (Object o : c) {
            if (!contains(o))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean removeAll(@NotNull Collection<?> c) {
        boolean rmv = false;

        for (Collection<E> collection : collections)
            rmv |= collection.removeAll(c);

        return rmv;
    }

    /** {@inheritDoc} */
    @Override public boolean retainAll(@NotNull Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        for (Collection<E> collection : collections)
            collection.clear();
    }

    /**
     * @return Number of inner collections.
     */
    public int collectionsSize() {
        return collections.length;
    }

    /**
     * @param idx Inner collection index.
     * @return Collection.
     */
    public Collection<E> innerCollection(int idx) {
        return collections[idx];
    }
}