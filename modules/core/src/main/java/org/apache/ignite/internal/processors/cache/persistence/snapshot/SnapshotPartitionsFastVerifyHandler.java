/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Quick partitions verifier. Warns if partiton counters or size are different among the nodes what can be caused by
 * canceled/failed DataStreamer.
 */
public class SnapshotPartitionsFastVerifyHandler extends SnapshotPartitionsVerifyHandler {
    /** */
    public static final String WRN_MSG_BASE = "This may happen if DataStreamer with property 'allowOverwrite' set " +
        "to `false` is loading during the snapshot or hadn't successfully finished earlier. However, you will be " +
        "able restore rest the caches from this snapshot.";

    /**
     * @param cctx Shared context.
     */
    public SnapshotPartitionsFastVerifyHandler(GridCacheSharedContext<?, ?> cctx) {
        super(cctx);
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return SnapshotHandlerType.CREATE;
    }

    /** {@inheritDoc} */
    @Override public Map<PartitionKeyV2, PartitionHashRecordV2> invoke(
        SnapshotHandlerContext opCtx) throws IgniteCheckedException {
        return super.invoke(opCtx);
    }

    /** {@inheritDoc} */
    @Override protected PartitionHashRecordV2 partHash(PartitionKeyV2 key, Object updCntr, Object consId,
        GridDhtPartitionState state, boolean isPrimary, long partSize,
        GridIterator<CacheDataRow> it) throws IgniteCheckedException {
        return new PartitionHashRecordV2(key, isPrimary, null, 0, updCntr, partSize, null);
    }

    /** {@inheritDoc} */
    @Override public void complete(
        String name,
        Collection<SnapshotHandlerResult<Map<PartitionKeyV2, PartitionHashRecordV2>>> results
    ) throws IgniteCheckedException {
        Map<Integer, Map<Integer, PartitionHashRecordV2>> counters = new ConcurrentHashMap<>();

        Set<Integer> wrnGroups = new GridConcurrentHashSet<>();

        U.doInParallel(
            cctx.snapshotMgr().snapshotExecutorService(),
            results,
            nodeResult -> {
                U.doInParallel(
                    cctx.snapshotMgr().snapshotExecutorService(),
                    nodeResult.data() == null ? Collections.emptySet() : nodeResult.data().entrySet(),
                    newResult -> {
                        if (wrnGroups.contains(newResult.getKey().groupId()))
                            return null;

                        counters.compute(newResult.getKey().groupId(), (p, partIdMap) -> {
                            if (partIdMap == null)
                                partIdMap = new ConcurrentHashMap<>();

                            partIdMap.compute(newResult.getKey().partitionId(), (partId, storedResult) -> {
                                if (storedResult == null)
                                    return newResult.getValue();

                                if (!storedResult.updateCounter().equals(newResult.getValue().updateCounter())
                                    || storedResult.size() != newResult.getValue().size())
                                    wrnGroups.add(newResult.getKey().groupId());

                                return storedResult;
                            });

                            return partIdMap;
                        });

                        return null;
                    }
                );

                return null;
            }
        );

        if (!wrnGroups.isEmpty())
            throw new SnapshotHandlerWarningException("Cache partitions differ for cache groups " +
                wrnGroups.stream().map(String::valueOf).collect(Collectors.joining(", ")) + ". " + WRN_MSG_BASE);
    }
}
