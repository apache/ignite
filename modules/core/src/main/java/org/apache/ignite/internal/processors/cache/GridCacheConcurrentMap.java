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

package org.apache.ignite.internal.processors.cache;

import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Concurrent cache map.
 */
public interface GridCacheConcurrentMap {
    /**
     * Returns the entry associated with the specified key in the
     * HashMap.  Returns null if the HashMap contains no mapping
     * for this key.
     *
     * @param key Key.
     * @return Entry.
     */
    @Nullable public GridCacheMapEntry getEntry(KeyCacheObject key);

    /**
     * @param topVer Topology version.
     * @param key Key.
     * @param val Value.
     * @param create Create flag.
     * @return Triple where the first element is current entry associated with the key,
     *      the second is created entry and the third is doomed (all may be null).
     */
    @Nullable public GridCacheMapEntry putEntryIfObsoleteOrAbsent(
        AffinityTopologyVersion topVer,
        KeyCacheObject key,
        @Nullable CacheObject val,
        boolean create,
        boolean touch);

    /**
     * Removes passed in entry if it presents in the map.
     *
     * @param entry Entry to remove.
     * @return {@code True} if remove happened.
     */
    public boolean removeEntry(GridCacheEntryEx entry);

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map.
     */
    public int size();

    /**
     * Returns the number of publicly available key-value mappings in this map.
     * It excludes entries that are marked as deleted.
     *
     * @return the number of publicly available key-value mappings in this map.
     */
    public int publicSize();

    /**
     * Increments public size.
     *
     * @param e Entry that caused public size change.
     */
    public void incrementPublicSize(GridCacheEntryEx e);

    /**
     * Decrements public size.
     *
     * @param e Entry that caused public size change.
     */
    public void decrementPublicSize(GridCacheEntryEx e);

    @Nullable public GridCacheMapEntry randomEntry();

    /**
     * @return Random entry out of hash map.
     */
    public Set<KeyCacheObject> keySet(CacheEntryPredicate... filter);

    /**
     * @param filter Filter.
     * @return Iterable of the mappings contained in this map, excluding entries in unvisitable state.
     */
    public Iterable<GridCacheMapEntry> entries(CacheEntryPredicate... filter);

    /**
     * @param filter Filter.
     * @return Iterable of the mappings contained in this map, including entries in unvisitable state.
     */
    public Iterable<GridCacheMapEntry> allEntries(CacheEntryPredicate... filter);

    /**
     * @param filter Filter.
     * @return Set of the mappings contained in this map.
     */
    public Set<GridCacheMapEntry> entrySet(CacheEntryPredicate... filter);
}
