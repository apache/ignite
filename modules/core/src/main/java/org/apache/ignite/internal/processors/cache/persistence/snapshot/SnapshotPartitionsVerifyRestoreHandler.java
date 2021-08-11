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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.util.typedef.F;

public class SnapshotPartitionsVerifyRestoreHandler implements SnapshotHandler<Map<PartitionKeyV2, PartitionHashRecordV2>> {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    public SnapshotPartitionsVerifyRestoreHandler(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return SnapshotHandlerType.RESTORE;
    }

    /** {@inheritDoc} */
    @Override public Map<PartitionKeyV2, PartitionHashRecordV2> handle(
        SnapshotHandlerContext opCtx
    ) throws IgniteCheckedException {
        return VisorVerifySnapshotPartitionsJob.checkPartitions(opCtx.metadata(), opCtx.groups(), ctx.cache().context(), log);
    }

    /** {@inheritDoc} */
    @Override public void reduce(
        String name,
        Collection<SnapshotHandlerResult<Map<PartitionKeyV2, PartitionHashRecordV2>>> results
    ) throws IgniteCheckedException {
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> clusterHashes = new HashMap<>();
        Map<ClusterNode, Exception> errs = new HashMap<>();

        for (SnapshotHandlerResult<Map<PartitionKeyV2, PartitionHashRecordV2>> res : results) {
            if (res.error() != null) {
                errs.put(res.node(), res.error());

                continue;
            }

            for (Map.Entry<PartitionKeyV2, PartitionHashRecordV2> e : res.data().entrySet()) {
                List<PartitionHashRecordV2> records = clusterHashes.computeIfAbsent(e.getKey(), k -> new ArrayList<>());

                records.add(e.getValue());
            }
        }

        IdleVerifyResultV2 verifyResult = new IdleVerifyResultV2(clusterHashes, errs);

        if (!F.isEmpty(errs) || verifyResult.hasConflicts()) {
            StringBuilder sb = new StringBuilder();

            verifyResult.print(sb::append, true);

            throw new IgniteCheckedException(sb.toString());
        }
    }
}
