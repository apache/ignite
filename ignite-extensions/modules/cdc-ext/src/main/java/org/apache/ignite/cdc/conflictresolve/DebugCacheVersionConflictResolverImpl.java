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

package org.apache.ignite.cdc.conflictresolve;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.util.typedef.internal.S;

/** Debug aware resolver. */
public class DebugCacheVersionConflictResolverImpl extends CacheVersionConflictResolverImpl {
    /**
     * @param clusterId Data center id.
     * @param conflictResolveField Field to resolve conflicts.
     * @param log Logger.
     */
    public DebugCacheVersionConflictResolverImpl(byte clusterId, String conflictResolveField, IgniteLogger log) {
        super(clusterId, conflictResolveField, log);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> boolean isUseNew(
        CacheObjectValueContext ctx,
        GridCacheVersionedEntryEx<K, V> oldEntry,
        GridCacheVersionedEntryEx<K, V> newEntry
    ) {
        boolean res = super.isUseNew(ctx, oldEntry, newEntry);

        Object oldVal = conflictResolveFieldEnabled ? oldEntry.value(ctx) : null;
        Object newVal = conflictResolveFieldEnabled ? newEntry.value(ctx) : null;

        if (oldVal != null)
            oldVal = value(oldVal);

        if (newVal != null)
            newVal = value(newVal);

        log.debug("isUseNew[" +
            "start=" + oldEntry.isStartVersion() +
            ", oldVer=" + oldEntry.version() +
            ", newVer=" + newEntry.version() +
            ", old=" + oldVal +
            ", new=" + newVal +
            ", res=" + res + ']');

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheVersionConflictResolverImpl.class, this);
    }
}
