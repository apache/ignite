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
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Information structure with partitions state.
 * Page counts map.
 */
public class PartitionAllocationMap {
    /** Maps following pairs: (groupId, partId) -> (lastAllocatedCount, allocatedCount) */
    @GridToStringInclude
    private final NavigableMap<GroupPartitionId, PagesAllocationRange> map = new ConcurrentSkipListMap<>();

    /** Partitions forced to be skipped. */
    @GridToStringInclude
    private final Set<GroupPartitionId> skippedParts = new GridConcurrentHashSet<>();

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * @param key to get
     * @return value or null
     */
    @Nullable public PagesAllocationRange get(GroupPartitionId key) {
        return map.get(key);
    }

    /**
     * @param fullPageId Full page id.
     */
    @Nullable public PagesAllocationRange get(FullPageId fullPageId) {
        return map.get(createCachePartId(fullPageId));
    }

    /**
     * Extracts partition information from full page ID
     *
     * @param fullId page related to some cache
     * @return pair of cache ID and partition ID
     */
    @NotNull public static GroupPartitionId createCachePartId(@NotNull final FullPageId fullId) {
        return new GroupPartitionId(fullId.groupId(), PageIdUtils.partId(fullId.pageId()));
    }

    /** @return <tt>true</tt> if this map contains no key-value mappings */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /** @return the number of key-value mappings in this map. */
    public int size() {
        return map.size();
    }

    /** @return keys (all caches partitions) */
    public Set<GroupPartitionId> keySet() {
        return map.keySet();
    }

    /** @return values (allocation ranges) */
    public Iterable<PagesAllocationRange> values() {
        return map.values();
    }

    /** @return Returns the first (lowest) key currently in this map. */
    public GroupPartitionId firstKey() {
        return map.firstKey();
    }

    /**
     * Forces the index partition for the given group ID to be skipped in collected map.
     *
     * @param grpId Group ID to skip.
     * @return {@code true} if skipped partition was added to the ignore list during this call.
     */
    public boolean forceSkipIndexPartition(int grpId) {
        GroupPartitionId idxId = new GroupPartitionId(grpId, PageIdAllocator.INDEX_PARTITION);

        map.remove(idxId);

        return skippedParts.add(idxId);
    }

    /**
     * Returns next (higher) key for provided cache and partition or null
     *
     * @param key cache and partition to search
     * @return first found key which is greater than provided
     */
    @Nullable public GroupPartitionId nextKey(@NotNull final GroupPartitionId key) {
        return map.navigableKeySet().higher(key);
    }

    /** @return set view of the mappings contained in this map, sorted in ascending key order */
    public Set<Map.Entry<GroupPartitionId, PagesAllocationRange>> entrySet() {
        return map.entrySet();
    }

    /** @return <tt>true</tt> if this map contains a mapping for the specified key */
    public boolean containsKey(GroupPartitionId key) {
        return map.containsKey(key);
    }

    /**
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no mapping for
     * <tt>key</tt>.
     */
    public PagesAllocationRange put(GroupPartitionId key, PagesAllocationRange val) {
        return !skippedParts.contains(key) ? map.put(key, val) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionAllocationMap.class, this);
    }
}
