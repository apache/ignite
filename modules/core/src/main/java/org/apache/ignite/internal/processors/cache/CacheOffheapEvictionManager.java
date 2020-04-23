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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheOffheapEvictionManager extends GridCacheManagerAdapter implements CacheEvictionManager {
    /** {@inheritDoc} */
    @Override public void touch(IgniteTxEntry txEntry, boolean loc) {
        touch(txEntry.cached());
    }

    /** {@inheritDoc} */
    @Override public void touch(GridCacheEntryEx e) {
        if (e.detached())
            return;

        cctx.shared().database().checkpointReadLock();

        try {
            boolean evicted = e.evictInternal(GridCacheVersionManager.EVICT_VER, null, false)
                || e.markObsoleteIfEmpty(null);

            if (evicted && !e.isDht()) // GridDhtCacheEntry removes entry when obsoleted.
                cctx.cache().removeEntry(e);
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to evict entry from cache: " + e, ex);
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean evict(@Nullable GridCacheEntryEx entry,
        @Nullable GridCacheVersion obsoleteVer,
        boolean explicit,
        @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void batchEvict(Collection<?> keys, @Nullable GridCacheVersion obsoleteVer) {
        // No-op.
    }
}
