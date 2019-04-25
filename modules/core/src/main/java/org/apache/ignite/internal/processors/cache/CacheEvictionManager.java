/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface CacheEvictionManager extends GridCacheManager {
    /**
     * @param txEntry Transactional entry.
     * @param loc Local transaction flag.
     */
    public void touch(IgniteTxEntry txEntry, boolean loc);

    /**
     * @param e Entry for eviction policy notification.
     */
    public void touch(GridCacheEntryEx e);

    /**
     * @param entry Entry to attempt to evict.
     * @param obsoleteVer Obsolete version.
     * @param filter Optional entry filter.
     * @param explicit {@code True} if evict is called explicitly, {@code false} if it's called
     *      from eviction policy.
     * @return {@code True} if entry was marked for eviction.
     * @throws IgniteCheckedException In case of error.
     */
    public boolean evict(@Nullable GridCacheEntryEx entry,
        @Nullable GridCacheVersion obsoleteVer,
        boolean explicit,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException;

    /**
     * @param keys Keys to evict.
     * @param obsoleteVer Obsolete version.
     * @throws IgniteCheckedException In case of error.
     */
    public void batchEvict(Collection<?> keys, @Nullable GridCacheVersion obsoleteVer) throws IgniteCheckedException;
}
