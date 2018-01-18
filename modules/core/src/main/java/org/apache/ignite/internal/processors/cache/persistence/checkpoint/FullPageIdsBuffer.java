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
package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.pagemem.FullPageId;

/**
 * Full Pages IDs buffer. Wraps array of pages and start-end position. <br> Usage of this class allows to save arrays
 * allocation. <br> Several page IDs buffers can share same array but with different offset and length.
 */
public class FullPageIdsBuffer {
    /** Source array. May be shared between different buffers. */
    private final FullPageId[] arr;

    /** Start position. Index of first element, inclusive. */
    private final int position;

    /** Limit. Index of last element, exclusive. */
    private final int limit;

    /** Cached comparator used for sorting this buffer. */
    private Comparator<FullPageId> sortedUsingComparator;

    /**
     * @param arr Array.
     * @param position Position.
     * @param limit Limit.
     */
    public FullPageIdsBuffer(FullPageId[] arr, int position, int limit) {
        this.arr = arr;
        this.position = position;
        this.limit = limit;
    }

    /**
     * @return copy of buffer data as array. Causes new instance allocation.
     */
    public FullPageId[] toArray() {
        return Arrays.copyOfRange(arr, position, limit);
    }

    /**
     * @return Number of remaining (contained) page identifiers.
     */
    public int remaining() {
        return limit - position;
    }

    /**
     * Sorts underlying sub array with provided comparator.
     *
     * @param comp the comparator to determine the order of elements in the buffer.
     */
    public void sort(Comparator<FullPageId> comp) {
        Arrays.sort(arr, position, limit, comp);

        setSortedUsingComparator(comp);
    }

    /**
     * @return shared array of pages. Operating on this array outside bounds may be unsafe.
     */
    public FullPageId[] internalArray() {
        return arr;
    }

    /**
     * @return Start position. Index of first element, inclusive.
     */
    public int position() {
        return position;
    }

    /**
     * @return Limit. Index of last element, exclusive.
     */
    public int limit() {
        return limit;
    }

    /**
     * Creates buffer from current buffer range.
     *
     * @param position required start position, absolute - counted from array start, inclusive. <br> May not be less
     * that current buffer {@link #position}.
     * @param limit required buffer limit, absolute - counted from internal array start position, exclusive. <br> May
     * not be greater that current buffer {@link #limit}.
     * @return buffer created. Shares array with initial buffer.
     */
    FullPageIdsBuffer bufferOfRange(int position, int limit) {
        assert position >= this.position;
        assert limit <= this.limit;

        return new FullPageIdsBuffer(arr, position, limit);
    }

    /**
     * Splits pages to {@code pagesSubArrays} sub-buffer. This buffer is to be used as one or several tasks in executor
     * service. If any thread will be faster, it will help slower threads.
     *
     * @param pagesSubArrays required sub arrays count.
     * @return full page arrays to be processed as standalone tasks.
     */
    public Collection<FullPageIdsBuffer> split(int pagesSubArrays) {
        assert pagesSubArrays > 0;

        if (pagesSubArrays == 1)
            return Collections.singletonList(this);

        Collection<FullPageIdsBuffer> res = new ArrayList<>(pagesSubArrays);

        int totalSize = remaining();

        for (int i = 0; i < pagesSubArrays; i++) {
            int from = totalSize * i / (pagesSubArrays);

            int to = totalSize * (i + 1) / (pagesSubArrays);

            res.add(bufferOfRange(position + from, position + to));
        }

        return res;
    }

    /**
     * Marks buffer elements as sorted using exact comparator instance.
     *
     * @param comp comparator used for sorting.
     */
    private FullPageIdsBuffer setSortedUsingComparator(Comparator<FullPageId> comp) {
        sortedUsingComparator = comp;

        return this;
    }

    /**
     * Checks if this buffer was already sorted by provided comparator instance.
     *
     * @param comp comparator probably used for sorting.
     * @return {@code True} if buffer elements are sorted using exactly the same comparator.
     */
    boolean isSortedUsingComparator(Comparator<FullPageId> comp) {
        return sortedUsingComparator != null && sortedUsingComparator == comp;
    }

    /**
     * @param multiColl Multiple collections to create buffer from. Collection elements may be removed during this
     * method operation.
     * @return buffer with element from all collections
     */
    static FullPageIdsBuffer createBufferFromMultiCollection(Iterable<? extends Collection<FullPageId>> multiColl) {
        int size = 0;

        for (Collection<FullPageId> next : multiColl) {
            size += next.size();
        }

        FullPageId[] pageIds = new FullPageId[size];
        int locIdx = 0;

        for (Collection<FullPageId> set : multiColl) {
            for (FullPageId pageId : set) {
                pageIds[locIdx] = pageId;
                locIdx++;
            }
        }

        //actual number of pages may be less then initial calculated size
        return new FullPageIdsBuffer(pageIds, 0, locIdx);
    }

    /**
     * @param multiColl multi collections, each collection is already ordered using comparator {@code comp}.
     * @param comp comparator used for ordering provided {@code multiColl}.
     * @param subBufSize size of elements to provide to subBufferConsumer
      *@param subBufConsumer handler for sub buffers collected (called earlier than all collection is merged). May be null.
     * @return buffer with all found collections elements.
     */
    static FullPageIdsBuffer createBufferFromSortedCollections(Iterable<? extends Collection<FullPageId>> multiColl,
        Comparator<FullPageId> comp,
        int subBufSize,
        BiConsumer<Boolean, FullPageIdsBuffer> subBufConsumer) {
        PriorityQueue<ComparableIterator<FullPageId>> queue = new PriorityQueue<>();
        int size = 0;

        for (Collection<FullPageId> list : multiColl) {
            int curListSize = list.size();

            size += curListSize;
            if (curListSize > 0)
                queue.add(new ComparableIterator<>(comp, list.iterator()));
        }

        FullPageId[] arr = new FullPageId[size];
        int locIdx = 0;
        int curStartPosition = 0;

        while (!queue.isEmpty()) {
            ComparableIterator<FullPageId> next = queue.remove();
            FullPageId element = next.next();

            if (element != null) { // here null is possible because of parallel removal from underlying collections
                arr[locIdx] = element;
                locIdx++;

                if (subBufSize > 0 && subBufConsumer != null && (locIdx - curStartPosition >= subBufSize)) {
                    FullPageIdsBuffer buf = new FullPageIdsBuffer(arr, curStartPosition, locIdx).setSortedUsingComparator(comp);

                    subBufConsumer.accept(locIdx < size, buf);

                    curStartPosition = locIdx;
                }
            }

            if (next.hasNext())
                queue.add(next);
        }

        if (subBufSize > 0 && subBufConsumer != null && locIdx > curStartPosition) {
            FullPageIdsBuffer buf = new FullPageIdsBuffer(arr, curStartPosition, locIdx).setSortedUsingComparator(comp);
            subBufConsumer.accept(false, buf);
        }

        //actual number of pages may be less then precalculated size
        return new FullPageIdsBuffer(arr, 0, locIdx).setSortedUsingComparator(comp);
    }

    /**
     * Comparable iterator, which uses peeked elements values for actual comparing iterators.
     *
     * @param <E> element for iterating
     */
    private static class ComparableIterator<E> implements Iterator<E>, Comparable<ComparableIterator<E>> {
        /** Peek element. */
        private E peekElem;

        /** Comparator for element. */
        private Comparator<E> comp;

        /** Iterator */
        private Iterator<? extends E> it;

        /**
         * @param comp compactor for elements.
         * @param it underlying iterator to obtain next elements.
         */
        ComparableIterator(Comparator<E> comp, Iterator<? extends E> it) {
            this.comp = comp;
            this.it = it;

            peekElem = it.hasNext() ? it.next() : null;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            return peekElem != null;
        }

        /** {@inheritDoc} */
        @Override
        public E next() {
            E ret = peekElem;

            peekElem = it.hasNext() ? it.next() : null;

            return ret;
        }

        /** {@inheritDoc} */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(ComparableIterator<E> o) {
            return peekElem == null ? 1 : comp.compare(peekElem, o.peekElem);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj.getClass() == ComparableIterator.class && compareTo((ComparableIterator)obj) == 0;
        }
    }
}
