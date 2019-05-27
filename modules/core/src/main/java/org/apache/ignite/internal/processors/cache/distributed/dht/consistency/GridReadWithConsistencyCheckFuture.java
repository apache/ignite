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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;

/**
 * Checks data consistency. Checks that each backup value equals to primary value.
 */
public class GridReadWithConsistencyCheckFuture extends GridReadWithConsistencyAbstractFuture {
    /**
     *
     */
    public GridReadWithConsistencyCheckFuture(
        AffinityTopologyVersion topVer,
        GridCacheContext cctx,
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
    }

    /** {@inheritDoc} */
    @Override protected void reduce() {
        Map<KeyCacheObject, EntryGetResult> map = new HashMap<>();

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fut.result().entrySet()) {
                KeyCacheObject key = entry.getKey();
                EntryGetResult candidae = entry.getValue();
                EntryGetResult old = map.get(key);

                if (old != null && old.version().compareTo(candidae.version()) != 0) {
                    onDone(new IgniteConsistencyViolationException("Distributed cache consistency violation detected."));

                    return;
                }

                map.put(key, candidae);
            }
        }

        onDone(map);
    }
}
