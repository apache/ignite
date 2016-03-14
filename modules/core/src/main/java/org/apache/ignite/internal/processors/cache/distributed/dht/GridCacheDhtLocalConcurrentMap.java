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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMapInterface;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.jetbrains.annotations.Nullable;

public class GridCacheDhtLocalConcurrentMap implements GridCacheConcurrentMapInterface {

    private final GridCacheAffinityManager affMgr;

    private final GridDhtPartitionTopology top;

    public GridCacheDhtLocalConcurrentMap(GridCacheAffinityManager affMgr,
        GridDhtPartitionTopology top) {
        this.affMgr = affMgr;
        this.top = top;
    }

    @Nullable @Override public GridCacheMapEntry getEntry(Object key) {
        GridDhtLocalPartition part = top.localPartition(key, false);

        if (part == null)
            return null;

        return part.getEntry(key);
    }

    @Override
    public GridTriple<GridCacheMapEntry> putEntryIfObsoleteOrAbsent(AffinityTopologyVersion topVer, KeyCacheObject key,
        @Nullable CacheObject val, boolean create) {
        return top.localPartition(key, true).putEntryIfObsoleteOrAbsent(topVer, key, val, create);
    }

    @Override public GridCacheMapEntry removeEntryIfObsolete(KeyCacheObject key) {
        GridDhtLocalPartition part = top.localPartition(key, false);

        if (part == null)
            return null;

        return part.removeEntryIfObsolete(key);
    }

    @Override public int size() {
        int size = 0;

        for (GridDhtLocalPartition part : top.localPartitions()) {
            size += part.size();
        }

        return size;
    }

    @Override public boolean removeEntry(GridCacheEntryEx entry) {
        GridDhtLocalPartition part = top.localPartition(entry.key(), false);

        if (part == null)
            return false;

        return part.removeEntry(entry);
    }

    @Nullable @Override public GridCacheMapEntry randomEntry() {
        return null;
    }

    @Override public <K, V> Set<K> keySet(CacheEntryPredicate... filter) {
        Set set = new HashSet();

        for (GridDhtLocalPartition part : top.localPartitions()) {
            set.addAll(part.keySet(filter));
        }

        return set;
    }

    @Override public Set<GridCacheEntryEx> entries0() {
        Set set = new HashSet();

        for (GridDhtLocalPartition part : top.localPartitions()) {
            set.addAll(part.entries0());
        }

        return set;
    }

    @Override public <K, V> Set<Cache.Entry<K, V>> entries(CacheEntryPredicate... filter) {
        Set set = new HashSet();

        for (GridDhtLocalPartition part : top.localPartitions()) {
            set.addAll(part.entries(filter));
        }

        return set;
    }

    @Override public <K, V> Collection<V> values(CacheEntryPredicate... filter) {
        List list = new ArrayList<>();

        for (GridDhtLocalPartition part : top.localPartitions()) {
            list.addAll(part.values(filter));
        }

        return list;
    }

    @Override public void incrementSize(GridCacheMapEntry e) {

    }

    @Override public void decrementSize(GridCacheMapEntry e) {

    }
}
