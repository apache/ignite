/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.partstate;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Information structure with partitions state.
 * Page counts map.
 */
public class PartitionStatMap {
    /** Maps following pairs: (cacheId, partId) -> (lastAllocatedIndex, count) */
    private final NavigableMap<CachePartitionId, Value> map = new TreeMap<>(PartStatMapFullPageIdComparator.INSTANCE);

    public Value get(CachePartitionId key) {
        return map.get(key);
    }

    public Value get(FullPageId fullId) {
        return get(createKey(fullId));
    }

    @NotNull public static CachePartitionId createKey(@NotNull final FullPageId fullId) {
        return new CachePartitionId(fullId.cacheId(), PageIdUtils.partId(fullId.pageId()));
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public int size() {
        return map.size();
    }

    public Set<CachePartitionId> keySet() {
        return map.keySet();
    }

    public Iterable<Value> values() {
        return map.values();
    }

    public CachePartitionId firstKey() {
        return map.firstKey();
    }

    public SortedMap<CachePartitionId, Value> headMap(CachePartitionId key) {
        return map.headMap(key);
    }

    public SortedMap<CachePartitionId, Value> headMap(FullPageId fullId) {
        return headMap(createKey(fullId));
    }

    public SortedMap<CachePartitionId, Value> tailMap(CachePartitionId key, boolean inclusive) {
        return map.tailMap(key, inclusive);
    }

    public SortedMap<CachePartitionId, Value> tailMap(FullPageId fullId, boolean inclusive) {
        return tailMap(createKey(fullId), inclusive);
    }

    public Set<Map.Entry<CachePartitionId, Value>> entrySet() {
        return map.entrySet();
    }

    public boolean containsKey(FullPageId id) {
        return map.containsKey(createKey(id));
    }

    public Value put(CachePartitionId key, Value value) {
        return map.put(key, value);
    }

    public static class Value {
        /**
         * Last Allocated Page Index (count) from PageMetaIO.
         * Used to separate newly allocated pages with previously observed state
         */
        private final int lastAllocatedIdx;

        /**
         * Total number of pages allocated for partition <code>[partition, cacheId]</code>
         */
        private final int count;

        public Value(int lastAllocatedIdx, int count) {
            this.lastAllocatedIdx = lastAllocatedIdx;
            this.count = count;
        }

        public int getCount() {
            return count;
        }

        /** Tmp method for compatibility with tuple */
        public int get2() {
            return getCount();
        }

        public int getLastAllocatedIdx() {
            return lastAllocatedIdx;
        }

        /** Tmp method for compatibility with tuple */
        public int get1() {
            return getLastAllocatedIdx();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Value{" +
                "lastAllocatedIndex=" + lastAllocatedIdx +
                ", count=" + count +
                '}';
        }
    }
}
