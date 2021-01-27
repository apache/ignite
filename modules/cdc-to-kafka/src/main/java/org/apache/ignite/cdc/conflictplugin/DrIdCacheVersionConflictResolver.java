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

package org.apache.ignite.cdc.conflictplugin;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class implements simple coflict resolution algorithm.
 * Algorith decides which version of the entry should be used "new" or "old".
 * The following steps performed:
 * <ul>
 *     <li>If entry is freshly created then new version used - {@link GridCacheVersionedEntryEx#isStartVersion()}.</li>
 *     <li>If change made in this data center then new version used - {@link GridCacheVersionedEntryEx#dataCenterId()}.</li>
 *     <li>If data center of new entry equal to data center of old entry then entry with the greater {@link GridCacheVersionedEntryEx#order()} used.</li>
 *     <li>If {@link #conflictResolveField} provided and field of new entry greater then new version used.</li>
 *     <li>If {@link #conflictResolveField} provided and field of old entry greater then old version used.</li>
 *     <li>Entry with the lower value of {@link GridCacheVersionedEntryEx#dataCenterId()} used.</li>
 * </ul>
 *
 * Note, data center with lower value has greater priority e.g first (1) data center is main in case conflict can't be resolved automatically.
 */
public class DrIdCacheVersionConflictResolver implements CacheVersionConflictResolver {
    /**
     * Data center replication id.
     * Note, data center with lower value has greater priority e.g first (1) data center is main in case conflict can't be resolved automatically.
     */
    private final byte drId;

    /**
     * Field for conflict resolve.
     * Value of this field will be used to compare two entries in case of conflicting changes.
     * Note, values of this field must implement {@link Comparable} interface.
     *
     * @see DrIdCacheVersionConflictResolver
     */
    private final String conflictResolveField;

    /** Logger. */
    private final IgniteLogger log;

    /** If {@code true} then conflict resolving with the value field enabled. */
    private boolean conflictResolveFieldEnabled;

    /**
     * @param drId Data center id.
     * @param conflictResolveField Field to resolve conflicts.
     * @param log Logger.
     */
    public DrIdCacheVersionConflictResolver(byte drId, String conflictResolveField, IgniteLogger log) {
        this.drId = drId;
        this.conflictResolveField = conflictResolveField;
        this.log = log;

        conflictResolveFieldEnabled = conflictResolveField != null;
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
