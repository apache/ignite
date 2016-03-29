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
 *
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Set;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.lang.GridTriple;
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
    @Nullable GridCacheMapEntry getEntry(Object key);

    /**
     * @param topVer Topology version.
     * @param key Key.
     * @param val Value.
     * @param create Create flag.
     * @return Triple where the first element is current entry associated with the key,
     *      the second is created entry and the third is doomed (all may be null).
     */
    GridTriple<GridCacheMapEntry> putEntryIfObsoleteOrAbsent(
        AffinityTopologyVersion topVer,
        KeyCacheObject key,
        @Nullable CacheObject val,
        boolean create);

    /**
     * Removes passed in entry if it presents in the map.
     *
     * @param entry Entry to remove.
     * @return {@code True} if remove happened.
     */
    boolean removeEntry(GridCacheEntryEx entry);

    /**
     * Removes and returns the entry associated with the specified key
     * in the HashMap if entry is obsolete. Returns null if the HashMap
     * contains no mapping for this key.
     *
     * @param key Key.
     * @return Removed entry, possibly {@code null}.
     */
    GridCacheMapEntry removeEntryIfObsolete(KeyCacheObject key);

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map.
     */
    int size();

    int publicSize();

    void incrementPublicSize(GridCacheEntryEx e);

    void decrementPublicSize(GridCacheEntryEx e);

    @Nullable GridCacheMapEntry randomEntry();

    /**
     * @return Random entry out of hash map.
     */
    Set<KeyCacheObject> keySet(CacheEntryPredicate... filter);

    /**
     * @param filter Filter.
     * @return Iterable of the mappings contained in this map.
     */
    Iterable<GridCacheMapEntry> entries(CacheEntryPredicate... filter);

    /**
     * @param filter Filter.
     * @return Set of the mappings contained in this map.
     */
    Set<GridCacheMapEntry> entrySet(CacheEntryPredicate... filter);
}
