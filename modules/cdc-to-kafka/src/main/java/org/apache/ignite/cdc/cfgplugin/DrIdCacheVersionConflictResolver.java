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

package org.apache.ignite.cdc.cfgplugin;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;

/**
 * Basic replication conflict resolver.
 */
public class DrIdCacheVersionConflictResolver implements CacheVersionConflictResolver {
    /** */
    private final byte drId;

    /** */
    private final IgniteLogger log;

    /**
     * @param drId Data center id.
     */
    public DrIdCacheVersionConflictResolver(byte drId, IgniteLogger log) {
        this.drId = drId;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheVersionConflictContext<K, V> resolve(
        CacheObjectValueContext ctx,
        GridCacheVersionedEntryEx<K, V> oldEntry,
        GridCacheVersionedEntryEx<K, V> newEntry,
        boolean atomicVerComparator
    ) {
        GridCacheVersionConflictContext<K, V> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

        if (isUseNew(oldEntry, newEntry))
            res.useNew();
        else {
            log.warning("Skip update due to the conflict[key=" + newEntry.key() + ",fromDC=" + newEntry.dataCenterId() + ",toDC=" + oldEntry.dataCenterId() + ']');

            res.useOld();
        }

        return res;
    }

    /**
     * @param oldEntry Old entry.
     * @param newEntry New entry.
     * @param <K> Key type.
     * @param <V> Key type.
     * @return {@code True} is should use new entry.
     */
    private <K, V> boolean isUseNew(
        GridCacheVersionedEntryEx<K, V> oldEntry,
        GridCacheVersionedEntryEx<K, V> newEntry) {
        if (oldEntry.isStartVersion()) // New entry.
            return true;

        if (newEntry.dataCenterId() == drId) // Update made on the same DC.
            return true;

        if (oldEntry.dataCenterId() == newEntry.dataCenterId())
            return newEntry.order() > oldEntry.order(); // New version from the same DC.

        return newEntry.dataCenterId() < oldEntry.dataCenterId(); // DC with the lower ID have biggest priority.
    }
}
