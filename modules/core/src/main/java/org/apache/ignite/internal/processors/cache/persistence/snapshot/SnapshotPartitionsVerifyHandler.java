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

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Default snapshot restore handler for checking snapshot partitions consistency.
 */
public class SnapshotPartitionsVerifyHandler implements SnapshotHandler<Map<PartitionKey, PartitionHashRecord>> {
    /** Shared context. */
    protected final GridCacheSharedContext<?, ?> cctx;

    /** @param cctx Shared context. */
    public SnapshotPartitionsVerifyHandler(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return SnapshotHandlerType.RESTORE;
    }

    /** {@inheritDoc} */
    @Override public Map<PartitionKey, PartitionHashRecord> invoke(SnapshotHandlerContext opCtx) throws IgniteCheckedException {
        try {
            return cctx.snapshotMgr().checker().checkPartitions(opCtx.metadata(), opCtx.snapshotFileTree(), opCtx.groups(),
                type() == SnapshotHandlerType.CREATE, opCtx.check(), skipHash()).get();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to get result of partitions validation of snapshot '"
                + opCtx.metadata().snapshotName() + "'.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void complete(String name,
        Collection<SnapshotHandlerResult<Map<PartitionKey, PartitionHashRecord>>> results) throws IgniteCheckedException {
        IdleVerifyResult.Builder bldr = IdleVerifyResult.builder();

        for (SnapshotHandlerResult<Map<PartitionKey, PartitionHashRecord>> res : results) {
            if (res.error() != null) {
                bldr.addException(res.node(), res.error());

                continue;
            }

            bldr.addPartitionHashes(res.data());
        }

        IdleVerifyResult verifyResult = bldr.build();

        if (verifyResult.exceptions().isEmpty() && !verifyResult.hasConflicts())
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
}
