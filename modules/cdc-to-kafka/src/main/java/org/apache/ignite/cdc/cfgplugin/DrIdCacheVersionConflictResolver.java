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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Basic replication conflict resolver.
 */
public class DrIdCacheVersionConflictResolver implements CacheVersionConflictResolver {
    /** */
    private final byte drId;

    /** */
    private final String conflictResolveField;

    /** */
    private boolean conflictResolveFieldEnabled;

    /** */
    private final IgniteLogger log;

    /**
     * @param drId Data center id.
     * @param conflictResolveField Field to resolve conflicts.
     */
    public DrIdCacheVersionConflictResolver(byte drId, String conflictResolveField, IgniteLogger log) {
        this.drId = drId;
        this.conflictResolveField = conflictResolveField;
        this.log = log;
        this.conflictResolveFieldEnabled = conflictResolveField != null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheVersionConflictContext<K, V> resolve(
        CacheObjectValueContext ctx,
        GridCacheVersionedEntryEx<K, V> oldEntry,
        GridCacheVersionedEntryEx<K, V> newEntry,
        boolean atomicVerComparator
    ) {
        GridCacheVersionConflictContext<K, V> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

        if (isUseNew(ctx, oldEntry, newEntry))
            res.useNew();
        else {
            log.warning("Skip update due to the conflict[key=" + newEntry.key() + ",fromDC=" + newEntry.dataCenterId()
                + ",toDC=" + oldEntry.dataCenterId() + ']');

            res.useOld();
        }

        return res;
    }

    /**
     * @param ctx Context.
     * @param oldEntry Old entry.
     * @param newEntry New entry.
     * @param <K> Key type.
     * @param <V> Key type.
     * @return {@code True} is should use new entry.
     */
    private <K, V> boolean isUseNew(
        CacheObjectValueContext ctx,
        GridCacheVersionedEntryEx<K, V> oldEntry,
        GridCacheVersionedEntryEx<K, V> newEntry

    ) {
        if (oldEntry.isStartVersion()) // New entry.
            return true;

        if (newEntry.dataCenterId() == drId) // Update made on the same DC.
            return true;

        if (oldEntry.dataCenterId() == newEntry.dataCenterId())
            return newEntry.order() > oldEntry.order(); // New version from the same DC.

        if (conflictResolveFieldEnabled) {
            Object oldVal = oldEntry.value(ctx);
            Object newVal = newEntry.value(ctx);

            if (oldVal != null && newVal != null) {
                Comparable o;
                Comparable n;

                try {
                    if (oldVal instanceof BinaryObject) {
                        o = ((BinaryObject)oldVal).field(conflictResolveField);
                        n = ((BinaryObject)newVal).field(conflictResolveField);
                    }
                    else {
                        o = U.field(oldVal, conflictResolveField);
                        n = U.field(newVal, conflictResolveField);
                    }

                    if (o != null)
                        return o.compareTo(n) < 0;

                    if (n != null)
                        return n.compareTo(o) > 0;
                }
                catch (Exception e) {
                    log.error("Error while resolving replication conflict with field '" +
                        conflictResolveField + "'.\nConflict resolve with the field disabled.", e);

                    conflictResolveFieldEnabled = false;
                }
            }
        }

        return newEntry.dataCenterId() < oldEntry.dataCenterId(); // DC with the lower ID have biggest priority.
    }
}
