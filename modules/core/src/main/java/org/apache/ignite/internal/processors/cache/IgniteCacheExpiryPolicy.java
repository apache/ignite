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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper for {@link ExpiryPolicy} used to track information about cache entries
 * whose time to live was modified after access.
 */
public interface IgniteCacheExpiryPolicy {
    /**
     * @return TTL.
     */
    public long forCreate();

    /**
     * @return TTL.
     */
    public long forUpdate();

    /**
     * @return TTL.
     */
    public long forAccess();

    /**
     * Callback for ttl update on entry access.
     *
     * @param key Entry key.
     * @param ver Entry version.
     * @param rdrs Entry readers.
     */
    public void ttlUpdated(KeyCacheObject key,
       GridCacheVersion ver,
       @Nullable Collection<UUID> rdrs);

    /**
     * Clears information about updated entries.
     */
    public void reset();

    /**
     * @param cnt Entries count.
     * @return {@code True} if number of entries or readers is greater than given number.
     */
    public boolean readyToFlush(int cnt);

    /**
     * @return Entries with TTL updated on access.
     */
    @Nullable public Map<KeyCacheObject, GridCacheVersion> entries();

    /**
     * @return Readers for updated entries.
     */
    @Nullable Map<UUID, Collection<IgniteBiTuple<KeyCacheObject, GridCacheVersion>>> readers();
}