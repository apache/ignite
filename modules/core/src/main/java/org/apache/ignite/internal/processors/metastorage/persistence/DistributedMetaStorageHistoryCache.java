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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.util.Arrays;

/**
 * In-memory storage for the latest distributed metastorage updates.
 */
final class DistributedMetaStorageHistoryCache {
    /**
     * Version of the oldest history item in the cache. For empty history it is expected to be zero until the
     * first change.
     */
    private long startingVer;

    /**
     * Looped array to store history items. Must always have size that is the power of two, 16 by default.
     */
    private DistributedMetaStorageHistoryItem[] items;

    /**
     * Index of the first item. 0 if cache is empty.
     */
    private int from;

    /**
     * Index next to the last one. 0 if cache is empty.
     */
    private int to;

    /**
     * Approximate size of all cached history items.
     */
    private long sizeApproximation;

    /**
     * Default constructor.
     */
    public DistributedMetaStorageHistoryCache() {
        items = new DistributedMetaStorageHistoryItem[16];
    }

    /**
     * Get history item by the specific version.
     *
     * @param ver Specific version.
     * @return Requested hitsory item or {@code null} if it's not found.
     */
    public DistributedMetaStorageHistoryItem get(long ver) {
        int size = size();

        if (ver < startingVer || ver >= startingVer + size)
            return null;

        return items[(from + (int)(ver - startingVer)) & (items.length - 1)];
    }

    /**
     * Add history item to the cache. Works in "append-only" mode.
     *
     * @param ver Specific version. Must be either the last version in current cache or the next version after it.
     * @param item New history item. Expected to be the same as already stored one if {@code ver} is equal to the last
     *      version in the cache. Otherwise any non-null value is suitable.
     */
    public void put(long ver, DistributedMetaStorageHistoryItem item) {
        if (isEmpty()) {
            assert from == 0;
            assert to == 0;

            startingVer = ver;
        }

        if (ver == startingVer + size()) {
            items[to] = item;
            sizeApproximation += item.estimateSize();

            to = next(to);

            if (from == to)
                expandBuffer();
        }
        else {
            assert ver == startingVer + size() - 1 : startingVer + " " + from + " " + to + " " + ver;
            assert items[(to - 1) & (items.length - 1)].equals(item);
        }
    }

    /**
     * Remove oldest history item from the cache.
     *
     * @return Removed value.
     */
    public DistributedMetaStorageHistoryItem removeOldest() {
        DistributedMetaStorageHistoryItem oldItem = items[from];

        sizeApproximation -= oldItem.estimateSize();

        items[from] = null;

        from = next(from);

        ++startingVer;

        if (from == to) {
            from = to = 0;

            startingVer = 0L;

            assert sizeApproximation == 0L;
        }

        return oldItem;
    }

    /**
     * Number of elements in the cache.
     */
    public int size() {
        return (to - from) & (items.length - 1);
    }

    /**
     * @return {@code true} if cache is empty, {@code false} otherwise.
     */
    public boolean isEmpty() {
        return sizeApproximation == 0;
    }

    /**
     * Clear the history cache.
     */
    public void clear() {
        Arrays.fill(items, null);

        from = to = 0;

        sizeApproximation = startingVer = 0L;
    }

    /**
     * @return Approximate size of all cached history items.
     */
    public long sizeInBytes() {
        return sizeApproximation;
    }

    /**
     * @return All history items as array.
     */
    public DistributedMetaStorageHistoryItem[] toArray() {
        int size = size();

        DistributedMetaStorageHistoryItem[] arr = new DistributedMetaStorageHistoryItem[size];

        if (from <= to)
            System.arraycopy(items, from, arr, 0, size);
        else {
            System.arraycopy(items, from, arr, 0, items.length - from);
            System.arraycopy(items, 0, arr, items.length - from, to);
        }

        return arr;
    }

    /** */
    private int next(int i) {
        return (i + 1) & (items.length - 1);
    }

    /** */
    private void expandBuffer() {
        DistributedMetaStorageHistoryItem[] items = this.items;

        DistributedMetaStorageHistoryItem[] newItems = new DistributedMetaStorageHistoryItem[items.length * 2];

        System.arraycopy(items, from, newItems, 0, items.length - from);
        System.arraycopy(items, 0, newItems, items.length - from, to);

        from = 0;
        to = items.length;

        this.items = newItems;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        DistributedMetaStorageHistoryCache that = (DistributedMetaStorageHistoryCache)o;

        int size = size();

        if (size != that.size())
            return false;

        if (startingVer != that.startingVer)
            return false;

        for (long ver = startingVer; ver < startingVer + size; ver++) {
            if (!get(ver).equals(that.get(ver)))
                return false;
        }

        assert sizeInBytes() == that.sizeInBytes();

        return true;
    }
}
