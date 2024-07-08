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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.management.cache.IdleVerifyResultV2;
import org.apache.ignite.internal.management.cache.PartitionKeyV2;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Default snapshot restore handler for checking snapshot partitions consistency.
 */
public class SnapshotPartitionsVerifyHandler implements SnapshotHandler<Map<PartitionKeyV2, PartitionHashRecordV2>> {
    /** Shared context. */
    protected final GridCacheSharedContext<?, ?> cctx;

    /** Logger. */
    private final IgniteLogger log;

    /** @param cctx Shared context. */
    public SnapshotPartitionsVerifyHandler(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;

        log = cctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return SnapshotHandlerType.RESTORE;
    }

    /** {@inheritDoc} */
    @Override public Map<PartitionKeyV2, PartitionHashRecordV2> invoke(SnapshotHandlerContext opCtx) throws IgniteCheckedException {
        if (!opCtx.snapshotDirectory().exists())
            throw new IgniteCheckedException("Snapshot directory doesn't exists: " + opCtx.snapshotDirectory());

        SnapshotMetadata meta = opCtx.metadata();

        Set<Integer> grps = F.isEmpty(opCtx.groups())
            ? new HashSet<>(meta.partitions().keySet())
            : opCtx.groups().stream().map(CU::cacheId).collect(Collectors.toSet());

        if (type() == SnapshotHandlerType.CREATE) {
            grps = grps.stream().filter(grp -> grp == MetaStorage.METASTORAGE_CACHE_ID ||
                CU.affinityNode(
                    cctx.localNode(),
                    cctx.kernalContext().cache().cacheGroupDescriptor(grp).config().getNodeFilter()
                )
            ).collect(Collectors.toSet());
        }

        if (!opCtx.check()) {
            log.info("Snapshot data integrity check skipped [snpName=" + meta.snapshotName() + ']');

            return Collections.emptyMap();
        }

        if (meta.dump())
            return cctx.snapshotMgr().checker().checkDumpFiles(opCtx.snapshotDirectory(), meta, grps, cctx.localNode().consistentId(),
                skipHash());

        return cctx.snapshotMgr().checker().checkSnapshotFiles(opCtx.snapshotDirectory(), grps, meta,
            type() == SnapshotHandlerType.CREATE, skipHash(), isPunchHoleEnabled(opCtx, grps));
    }

    /** {@inheritDoc} */
    @Override public void complete(String name,
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
            return;

        GridStringBuilder buf = new GridStringBuilder();

        verifyResult.print(buf::a, true);

        throw new IgniteCheckedException(buf.toString());
    }

    /**
     * Provides flag of full hash calculation.
     *
     * @return {@code True} if full partition hash calculation is required. {@code False} otherwise.
     */
    protected boolean skipHash() {
        return false;
    }

    /** */
    protected boolean isPunchHoleEnabled(SnapshotHandlerContext opCtx, Set<Integer> grpIds) {
        SnapshotMetadata meta = opCtx.metadata();
        Path snapshotDir = opCtx.snapshotDirectory().toPath();

        if (meta.hasCompressedGroups() && grpIds.stream().anyMatch(meta::isGroupWithCompression)) {
            try {
                cctx.kernalContext().compress().checkPageCompressionSupported(snapshotDir, meta.pageSize());

                return true;
            }
            catch (Exception e) {
                log.info("File system doesn't support page compression on snapshot directory: " + snapshotDir
                    + ", snapshot may have larger size than expected.");
            }
        }

        return false;
    }
}
