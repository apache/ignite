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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

/**
 * Quick partitions verifier. Warns if partiton counters or size are different among the nodes. May be caused by
 * canceled/failed DataStreamer.
 */
public class SnapshotPartitionsFastVerifyHandler extends AbstractSnapshotPartitionsVerifyHandler<Map.Entry<Long, Long>> {
    /** */
    private static final String WRN_MSG_BASE = "This may happen if DataStreamer with property 'allowOverwrite' set " +
        "to `false` is loading during the snapshot or hadn't successfully finished earlier. However, you will be " +
        "able restore rest the caches of the snapshot. For more details of snapshotted partitions states lauch the " +
        "snapshot chack task.";

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
    @Override public Map<PartitionKeyV2, Map.Entry<Long, Long>> invoke(SnapshotHandlerContext hndCtx)
        throws IgniteCheckedException {
        if (hndCtx.createSnpFut().streamUpdates())
            return null;

        return super.invoke(hndCtx);
    }

    /**
     * Validates certain partition. Including the meta and index.
     *
     * @param hndCtx Snapshot handler context.
     * @param opCtx Snapshot operation context.
     * @param partKey Partition key.
     * @param pageStore Store to read and check from.
     * @return Update counter for data partition. {@code Null} for other partitions.
     */
    @Override protected Map.Entry<Long, Long> validatePartition(
        SnapshotHandlerContext hndCtx,
        GridKernalContext opCtx,
        PartitionKeyV2 partKey,
        FilePageStore pageStore
    ) throws IgniteCheckedException {
        if (partKey.partitionId() == INDEX_PARTITION || partKey.groupId() == MetaStorage.METASTORAGE_CACHE_ID)
            return null;

        ThreadLocal<ByteBuffer> buff =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(hndCtx.metadata().pageSize()).
                order(ByteOrder.nativeOrder()));
        ByteBuffer pageBuff = buff.get();
        pageBuff.clear();
        pageStore.read(0, pageBuff, false);

        long pageAddr = GridUnsafe.bufferAddress(pageBuff);

        PagePartitionMetaIO io = PageIO.getPageIO(pageBuff);

        return new AbstractMap.SimpleEntry<>(io.getSize(pageAddr), io.getUpdateCounter(pageAddr));
    }

    /** {@inheritDoc} */
    @Override public void complete(String name,
        Collection<SnapshotHandlerResult<Map<PartitionKeyV2, Map.Entry<Long, Long>>>> results)
        throws IgniteCheckedException {
        // Group id -> Part size, part counter -> Counters set without node id.
        Map<Integer, Map<Integer, Map.Entry<Long, Long>>> counters = new ConcurrentHashMap<>();

        Set<Integer> wrnGroups = new GridConcurrentHashSet<>();

        U.doInParallel(
            cctx.snapshotMgr().snapshotExecutorService(),
            results,
            nodeResult -> {
                U.doInParallel(
                    cctx.snapshotMgr().snapshotExecutorService(),
                    nodeResult.data() == null ? Collections.emptySet() : nodeResult.data().entrySet(),
                    partResult -> {
                        if (wrnGroups.contains(partResult.getKey().groupId()))
                            return null;

                        counters.compute(partResult.getKey().groupId(), (p, partIdMap) -> {
                            if (partIdMap == null)
                                partIdMap = new ConcurrentHashMap<>();

                            partIdMap.compute(partResult.getKey().partitionId(), (partId, savedSizeAndCnt) -> {
                                if (savedSizeAndCnt == null)
                                    return partResult.getValue();

                                if (!savedSizeAndCnt.equals(partResult.getValue()))
                                    wrnGroups.add(partResult.getKey().groupId());

                                return savedSizeAndCnt;
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
                wrnGroups.stream().map(String::valueOf).collect(Collectors.joining(",")) + ". " + WRN_MSG_BASE);
    }
}
