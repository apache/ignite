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
 * Full Pages IDs buffer.
 */
public class FullPageIdsBuffer {
    /** Source array. May be shared between different buffers */
    private final FullPageId[] arr;

    /** Start position. Index of first element inclusive. */
    private final int position;

    /** Limit. Index of last element exclusive. */
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
     * @return
     */
    public FullPageId[] toArray() {
        return Arrays.copyOfRange(arr, position, limit);
    }

    /**
     * @return
     */
    public int remaining() {
        return limit - position;
    }

    /**
     * @param comp
     */
    public void sort(Comparator<FullPageId> comp) {
        Arrays.sort(arr, position, limit, comp);
    }

    /**
     * @return
     */
    public FullPageId[] internalArray() {
        return arr;
    }

    /**
     * @return
     */
    public int position() {
        return position;
    }

    /**
     * @return
     */
    public int limit() {
        return limit;
    }

    /**
     * @param position
     * @param limit
     * @return
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
