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
    public FullPageIdsBuffer bufferOfRange(int position, int limit) {
        assert position >= this.position;
        assert limit <= this.limit;

        return new FullPageIdsBuffer(arr, position, limit);
    }

    /**
     * Splits pages to {@code pagesSubArrays} sub-buffer. If any thread will be faster, it will help slower threads.
     *
     * @param pagesSubArrays required subArraysCount.
     * @return full page arrays to be processed as standalone tasks.
     */
    public Collection<FullPageIdsBuffer> split(int pagesSubArrays) {
        assert pagesSubArrays > 0;

        if (pagesSubArrays == 1)
            return Collections.singletonList(this);

        final Collection<FullPageIdsBuffer> res = new ArrayList<>();

        final int totalSize = remaining();

        for (int i = 0; i < pagesSubArrays; i++) {
            int from = totalSize * i / (pagesSubArrays);

            int to = totalSize * (i + 1) / (pagesSubArrays);

            res.add(bufferOfRange(position + from, position + to));
        }

        return res;
    }
}
