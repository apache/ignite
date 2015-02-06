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

package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.cache.*;
import org.jetbrains.annotations.*;

/**
 * Cache version conflict context.
 */
public interface GridCacheVersionConflictContext<K, V> {
    /**
     * Gets old (existing) cache entry.
     *
     * @return Old (existing) cache entry.
     */
    public GridCacheVersionedEntry<K, V> oldEntry();

    /**
     * Gets new cache entry.
     *
     * @return New cache entry.
     */
    public GridCacheVersionedEntry<K, V> newEntry();

    /**
     * Force cache to ignore new entry and leave old (existing) entry unchanged.
     */
    public void useOld();

    /**
     * Force cache to apply new entry overwriting old (existing) entry.
     * <p>
     * Note that updates from remote data centers always have explicit TTL , while local data center
     * updates will only have explicit TTL in case {@link Entry#timeToLive(long)} was called
     * before update. In the latter case new entry will pick TTL of the old (existing) entry, even
     * if it was set through update from remote data center. it means that depending on concurrent
     * update timings new update might pick unexpected TTL. For example, consider that three updates
     * of the same key are performed: local update with explicit TTL (1) followed by another local
     * update without explicit TTL (2) and one remote update (3). In this case you might expect that
     * update (2) will pick TTL set during update (1). However, in case update (3) occurrs between (1)
     * and (2) and it overwrites (1) during conflict resolution, then update (2) will pick TTL of
     * update (3). To have predictable TTL in such cases you should either always set it explicitly
     * through {@code GridCacheEntry.timeToLive(long)} or use {@link #merge(Object, long)}.
     */
    public void useNew();

    /**
     * Force cache to use neither old, nor new, but some other value passed as argument. In this case old
     * value will be replaced with merge value and update will be considered as local.
     * <p>
     * Also in case of merge you have to specify new TTL explicitly. For unlimited TTL use {@code 0}.
     *
     * @param mergeVal Merge value or {@code null} to force remove.
     * @param ttl Time to live in milliseconds.
     */
    public void merge(@Nullable V mergeVal, long ttl);
}
