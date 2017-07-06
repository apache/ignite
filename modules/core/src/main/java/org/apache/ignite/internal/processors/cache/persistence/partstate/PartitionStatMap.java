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
    /** Maps following pairs: (cacheId, partId) -> (lastAllocatedCount, allocatedCount) */
    private final NavigableMap<CachePartitionId, PagesAllocationRange> map = new TreeMap<>(PartStatMapFullPageIdComparator.INSTANCE);

    public PagesAllocationRange get(CachePartitionId key) {
        return map.get(key);
    }

    public PagesAllocationRange get(FullPageId fullId) {
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

    public Iterable<PagesAllocationRange> values() {
        return map.values();
    }

    public CachePartitionId firstKey() {
        return map.firstKey();
    }

    public SortedMap<CachePartitionId, PagesAllocationRange> headMap(CachePartitionId key) {
        return map.headMap(key);
    }

    public SortedMap<CachePartitionId, PagesAllocationRange> headMap(FullPageId fullId) {
        return headMap(createKey(fullId));
    }

    public SortedMap<CachePartitionId, PagesAllocationRange> tailMap(CachePartitionId key, boolean inclusive) {
        return map.tailMap(key, inclusive);
    }

    public SortedMap<CachePartitionId, PagesAllocationRange> tailMap(FullPageId fullId, boolean inclusive) {
        return tailMap(createKey(fullId), inclusive);
    }

    public Set<Map.Entry<CachePartitionId, PagesAllocationRange>> entrySet() {
        return map.entrySet();
    }

    public boolean containsKey(FullPageId id) {
        return map.containsKey(createKey(id));
    }

    public PagesAllocationRange put(CachePartitionId key, PagesAllocationRange val) {
        return map.put(key, val);
    }

}
