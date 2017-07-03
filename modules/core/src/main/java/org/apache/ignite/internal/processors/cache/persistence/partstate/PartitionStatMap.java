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
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.NotNull;

/**
 * Information structure with partitions state.
 * Page counts map.
 * Maps following pairs: (cacheId, partId) -> (lastAllocatedIndex, count)
 */
public class PartitionStatMap {
    private final NavigableMap<Key, Value> map = new TreeMap<>(PartStatMapFullPageIdComparator.INSTANCE);

    public Value get(Key key) {
        return map.get(key);
    }

    public Value get(FullPageId fullId) {
        return get(createKey(fullId));
    }

    @NotNull public static Key createKey(@NotNull final FullPageId fullId) {
        return new Key(fullId.cacheId(), PageIdUtils.partId(fullId.pageId()));
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public int size() {
        return map.size();
    }

    public Set<Key> keySet() {
        return map.keySet();
    }

    public Iterable<Value> values() {
        return map.values();
    }

    public Key firstKey() {
        return map.firstKey();
    }

    public SortedMap<Key, Value> headMap(Key key) {
        return map.headMap(key);
    }

    public SortedMap<Key, Value> headMap(FullPageId fullId) {
        return headMap(createKey(fullId));
    }

    public SortedMap<Key, Value> tailMap(Key key, boolean inclusive) {
        return map.tailMap(key, inclusive);
    }

    public SortedMap<Key, Value> tailMap(FullPageId fullId, boolean inclusive) {
        return tailMap(createKey(fullId), inclusive);
    }

    public Set<Map.Entry<Key, Value>> entrySet() {
        return map.entrySet();
    }

    public boolean containsKey(FullPageId id) {
        return map.containsKey(createKey(id));
    }

    public Value put(Key key, Value value) {
        return map.put(key, value);
    }

    public static class Key implements Comparable<Key> {
        private final int cacheId;
        private final int partId;

        public Key(int cacheId, int partId) {
            this.cacheId = cacheId;
            this.partId = partId;
        }

        public Key(T2<Integer, Integer> pair) {
            this(pair.get1(), pair.get2());
        }

        public int getCacheId() {
            return cacheId;
        }

        public int getPartId() {
            return partId;
        }

        public int get1() {
            return getCacheId();
        }

        public int get2() {
            return getPartId();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Key{" +
                "cacheId=" + cacheId +
                ", partId=" + partId +
                '}';
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            if (cacheId != key.cacheId)
                return false;
            return partId == key.partId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = cacheId;
            result = 31 * result + partId;
            return result;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Key o) {
            if (getCacheId() < o.getCacheId())
                return -1;

            if (getCacheId() > o.getCacheId())
                return 1;

            if (getPartId() < o.getPartId())
                return -1;

            if (getPartId() > o.getPartId())
                return 1;
            return 0;
        }
    }

    public static class Value {
        /**
         * last Allocated Page Index (previousSnapshotPageCount
         */
        private final int lastAllocatedIndex;
        private final int count;

        public Value(int lastAllocatedIndex, int count) {
            this.lastAllocatedIndex = lastAllocatedIndex;
            this.count = count;
        }

        public Value(T2<Integer, Integer> pair) {
            this(pair.get1(), pair.get2());
        }

        public int getCount() {
            return count;
        }

        public int get2() {
            return getCount();
        }

        public int getLastAllocatedIndex() {
            return lastAllocatedIndex;
        }

        public int get1() {
            return getLastAllocatedIndex();
        }

        @Override public String toString() {
            return "Value{" +
                "lastAllocatedIndex=" + lastAllocatedIndex +
                ", count=" + count +
                '}';
        }
    }
}
