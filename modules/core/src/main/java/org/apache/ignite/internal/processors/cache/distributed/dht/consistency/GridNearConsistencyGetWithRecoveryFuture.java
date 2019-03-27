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

package org.apache.ignite.internal.processors.cache.distributed.dht.consistency;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;

/**
 *
 */
public class GridNearConsistencyGetWithRecoveryFuture extends GridDhtConsistencyAbstractGetFuture {
    /**
     *
     */
    public GridNearConsistencyGetWithRecoveryFuture(
        AffinityTopologyVersion topVer,
        GridCacheContext ctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        String txLbl,
        MvccSnapshot mvccSnapshot) {
        super(topVer,
            ctx,
            keys,
            readThrough,
            subjId,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            skipVals,
            txLbl,
            mvccSnapshot,
            false);
    }

    /** {@inheritDoc} */
    @Override protected void onResult() {
        if (isDone())
            return;

        if (checkIsDone())
            onDone(checkAndFix());
    }

    /**
     *
     */
    private boolean checkIsDone() {
        for (IgniteInternalFuture fut : futs) {
            if (!fut.isDone())
                return false;
        }

        return true;
    }

    /**
     * Returns latest (by version) entry for each key with consistency violation.
     */
    private Map<KeyCacheObject, EntryGetResult> checkAndFix() {
        Map<KeyCacheObject, EntryGetResult> newestMap = new HashMap<>();
        Map<KeyCacheObject, EntryGetResult> fixedMap = new HashMap<>();

        for (IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> fut : futs) {
            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.result().entrySet()) {
                EntryGetResult candidate = entry.getValue();

                newestMap.putIfAbsent(entry.getKey(), candidate);

                EntryGetResult newest = newestMap.get(entry.getKey());

                if (newest.version().compareTo(candidate.version()) < 0) {
                    newestMap.put(entry.getKey(), candidate);
                    fixedMap.put(entry.getKey(), candidate);
                }

                if (newest.version().compareTo(candidate.version()) > 0)
                    fixedMap.put(entry.getKey(), newest);
            }
        }

        // Todo event

        return fixedMap;
    }
}
