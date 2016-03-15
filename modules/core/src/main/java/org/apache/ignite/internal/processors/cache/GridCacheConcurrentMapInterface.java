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
import javax.cache.Cache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.jetbrains.annotations.Nullable;

public interface GridCacheConcurrentMapInterface {

    @Nullable GridCacheMapEntry getEntry(Object key);

    GridTriple<GridCacheMapEntry> putEntryIfObsoleteOrAbsent(
        AffinityTopologyVersion topVer,
        KeyCacheObject key,
        @Nullable CacheObject val,
        boolean create);

    GridCacheMapEntry removeEntryIfObsolete(KeyCacheObject key);

    int size();

    <K, V> Set<K> keySet(CacheEntryPredicate... filter);

    boolean removeEntry(GridCacheEntryEx entry);

    Iterable<GridCacheEntryEx> entries0();

    <K, V> Iterable<Cache.Entry<K, V>> entries(CacheEntryPredicate... filter);

    @Nullable GridCacheMapEntry randomEntry();

    <K, V> Iterable<V> values(CacheEntryPredicate... filter);

    void incrementSize(GridCacheMapEntry e);

    void decrementSize(GridCacheMapEntry e);
}
