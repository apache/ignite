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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.jetbrains.annotations.Nullable;

/**
 * Checks data consistency. Checks that each backup value equals to primary value.
 */
public class GridDhtConsistencyCheckFuture
    extends GridDhtConsistencyAbstractFuture<Map<KeyCacheObject, EntryGetResult>> {
    /** Primary node's (current) get future. */
    private final IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> primaryFut;

    /**
     *
     */
    public GridDhtConsistencyCheckFuture(
        AffinityTopologyVersion topVer,
        IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> primaryFut,
        GridCacheContext cctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        @Nullable String txLbl,
        @Nullable MvccSnapshot mvccSnapshot) {
        super(topVer,
            cctx,
            keys,
            readThrough,
            subjId,
            taskName,
            deserializeBinary,
            recovery,
            expiryPlc,
            skipVals,
            txLbl,
            mvccSnapshot);

        this.primaryFut = primaryFut;

        primaryFut.listen(this::onResult);
    }

    /** {@inheritDoc} */
    @Override protected void onResult() {
        if (isDone())
            return;

        if (checkIsDone()) {
            if (check())
                onDone(primaryFut.result());
            else {
                // todo event

                onDone(null, new IgniteConsistencyViolationException(
                    "Distributed cache consistency violation detected. " +
                        "Perform same read under Pessimistic RepeatableRead/Serialisable transaction " +
                        "with Consistency Check enabled to fix the violation."));
            }
        }
    }

    /**
     *
     */
    private boolean checkIsDone() {
        for (IgniteInternalFuture fut : backupFuts) {
            if (!fut.isDone())
                return false;
        }

        return primaryFut.isDone();
    }

    /**
     *
     */
    private boolean check() {
        Map<KeyCacheObject, EntryGetResult> primaryRes = primaryFut.result();

        for (IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> fut : backupFuts) {
            Map<KeyCacheObject, EntryGetResult> backupRes = fut.result();

            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : backupRes.entrySet()) {
                EntryGetResult primary = primaryRes.get(entry.getKey());
                EntryGetResult backup = backupRes.get(entry.getKey());

                if (!primary.version().equals(backup.version()))
                    return false;
            }
        }

        return true;
    }
}
