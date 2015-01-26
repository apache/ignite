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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.datastructures.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
public class CacheDataStructuresManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Set keys used for set iteration. */
    private ConcurrentMap<IgniteUuid, GridConcurrentHashSet<GridCacheSetItemKey>> setDataMap =
        new ConcurrentHashMap8<>();

    /**
     * Entry update callback.
     *
     * @param key Key.
     * @param rmv {@code True} if entry was removed.
     */
    public void onEntryUpdated(K key, boolean rmv) {
        if (key instanceof GridCacheSetItemKey)
            onSetItemUpdated((GridCacheSetItemKey)key, rmv);
    }

    /**
     * Partition evicted callback.
     *
     * @param part Partition number.
     */
    public void onPartitionEvicted(int part) {
        GridCacheAffinityManager aff = cctx.affinity();

        for (GridConcurrentHashSet<GridCacheSetItemKey> set : setDataMap.values()) {
            Iterator<GridCacheSetItemKey> iter = set.iterator();

            while (iter.hasNext()) {
                GridCacheSetItemKey key = iter.next();

                if (aff.partition(key) == part)
                    iter.remove();
            }
        }
    }

    /**
     * @param id Set ID.
     * @return Data for given set.
     */
    @Nullable public GridConcurrentHashSet<GridCacheSetItemKey> setData(IgniteUuid id) {
        return setDataMap.get(id);
    }

    /**
     * @param key Set item key.
     * @param rmv {@code True} if item was removed.
     */
    private void onSetItemUpdated(GridCacheSetItemKey key, boolean rmv) {
        GridConcurrentHashSet<GridCacheSetItemKey> set = setDataMap.get(key.setId());

        if (set == null) {
            if (rmv)
                return;

            GridConcurrentHashSet<GridCacheSetItemKey> old = setDataMap.putIfAbsent(key.setId(),
                set = new GridConcurrentHashSet<>());

            if (old != null)
                set = old;
        }

        if (rmv)
            set.remove(key);
        else
            set.add(key);
    }
}
