/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.fromOrdinal;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.calculatePartitionHash;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.checkPartitionsPageCrcSum;

/**
 * Default snapshot restore handler for checking snapshot partitions consistency.
 */
public class SnapshotPartitionsVerifyHandler extends AbstractSnapshotPartitionsVerifyHandler<PartitionHashRecordV2> {
    /**
     * @param cctx Shared context.
     */
    SnapshotPartitionsVerifyHandler(GridCacheSharedContext<?, ?> cctx) {
        super(cctx);
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return SnapshotHandlerType.RESTORE;
    }

    /** {@inheritDoc} */
    @Override protected PartitionHashRecordV2 validatePartition(
        SnapshotHandlerContext hndCtx,
        GridKernalContext opCtx,
        PartitionKeyV2 key,
        FilePageStore pageStore
    ) throws IgniteCheckedException {
        if (key.partitionId() == INDEX_PARTITION) {
            checkPartitionsPageCrcSum(() -> pageStore, INDEX_PARTITION, FLAG_IDX);

            return null;
        }

        if (key.groupId() == MetaStorage.METASTORAGE_CACHE_ID) {
            checkPartitionsPageCrcSum(() -> pageStore, key.partitionId(), FLAG_DATA);

            return null;
        }

        ThreadLocal<ByteBuffer> buff =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(hndCtx.metadata().pageSize()).order(ByteOrder.nativeOrder()));
        ByteBuffer pageBuff = buff.get();
        pageBuff.clear();
        pageStore.read(0, pageBuff, true);

        long pageAddr = GridUnsafe.bufferAddress(pageBuff);

        PagePartitionMetaIO io = PageIO.getPageIO(pageBuff);
        GridDhtPartitionState partState = fromOrdinal(io.getPartitionState(pageAddr));

        if (partState != OWNING) {
            throw new IgniteCheckedException("Snapshot partitions must be in the OWNING " +
                "state only: " + partState);
        }

        // Snapshot partitions must always be in OWNING state.
        // There is no `primary` partitions for snapshot.
        long updateCntr = io.getUpdateCounter(pageAddr);
        long size = io.getSize(pageAddr);

        if (log.isDebugEnabled()) {
            log.debug("Partition [grpId=" + key.groupId()
                + ", id=" + key.partitionId()
                + ", counter=" + updateCntr
                + ", size=" + size + "]");
        }

        PartitionHashRecordV2 hash = calculatePartitionHash(key,
            updateCntr,
            hndCtx.metadata().consistentId(),
            GridDhtPartitionState.OWNING,
            false,
            size,
            cctx.snapshotMgr().partitionRowIterator(opCtx, key.groupName(), key.partitionId(), pageStore));

        assert hash != null : "OWNING must have hash: " + key;

        return hash;
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerWarning complete(String name,
        Collection<SnapshotHandlerResult<Map<PartitionKeyV2, PartitionHashRecordV2>>> results) throws IgniteCheckedException {
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> clusterHashes = new HashMap<>();
        Map<ClusterNode, Exception> errs = new HashMap<>();

        for (SnapshotHandlerResult<Map<PartitionKeyV2, PartitionHashRecordV2>> res : results) {
            if (res.error() != null) {
                errs.put(res.node(), res.error());

                continue;
            }

            for (Map.Entry<PartitionKeyV2, PartitionHashRecordV2> entry : res.data().entrySet())
                clusterHashes.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(entry.getValue());
        }

        IdleVerifyResultV2 verifyResult = new IdleVerifyResultV2(clusterHashes, errs);

        if (errs.isEmpty() && !verifyResult.hasConflicts())
            return null;

        GridStringBuilder buf = new GridStringBuilder();

        verifyResult.print(buf::a, true);

        throw new IgniteCheckedException(buf.toString());
    }
}
