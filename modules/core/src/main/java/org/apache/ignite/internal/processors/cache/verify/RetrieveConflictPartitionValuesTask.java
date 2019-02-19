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
package org.apache.ignite.internal.processors.cache.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@GridInternal
public class RetrieveConflictPartitionValuesTask extends ComputeTaskAdapter<Map<PartitionHashRecord, List<PartitionEntryHashRecord>>,
    Map<PartitionHashRecord, List<PartitionEntryHashRecord>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        Map<PartitionHashRecord, List<PartitionEntryHashRecord>> collectTaskRes
    ) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        Map<Object, ClusterNode> consIdToNode = new HashMap<>();

        for (ClusterNode node : subgrid)
            consIdToNode.put(node.consistentId(), node);

        for (Map.Entry<PartitionHashRecord, List<PartitionEntryHashRecord>> e : collectTaskRes.entrySet())
            jobs.put(new RetrieveConflictValuesJob(new T2<>(e.getKey(), e.getValue())), consIdToNode.get(e.getKey().consistentId()));

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<PartitionHashRecord, List<PartitionEntryHashRecord>> reduce(List<ComputeJobResult> results)
        throws IgniteException {
        Map<PartitionHashRecord, List<PartitionEntryHashRecord>> totalRes = new HashMap<>();

        for (ComputeJobResult res : results) {
            T2<PartitionHashRecord, List<PartitionEntryHashRecord>> nodeRes = res.getData();

            totalRes.put(nodeRes.get1(), nodeRes.get2());
        }

        return totalRes;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        ComputeJobResultPolicy superRes = super.result(res, rcvd);

        // Deny failover.
        if (superRes == ComputeJobResultPolicy.FAILOVER) {
            superRes = ComputeJobResultPolicy.WAIT;

            log.warning("RetrieveConflictValuesJob failed on node " +
                "[consistentId=" + res.getNode().consistentId() + "]", res.getException());
        }

        return superRes;
    }

    /**
     *
     */
    public static class RetrieveConflictValuesJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Partition hash record. */
        private PartitionHashRecord partHashRecord;

        /** Entry hash records. */
        private List<PartitionEntryHashRecord> entryHashRecords;

        /** Partition key. */
        private PartitionKey partKey;

        /**
         * @param arg Partition key.
         */
        private RetrieveConflictValuesJob(T2<PartitionHashRecord, List<PartitionEntryHashRecord>> arg) {
            partHashRecord = arg.get1();
            entryHashRecords = arg.get2();
            partKey = partHashRecord.partitionKey();
        }

        /** {@inheritDoc} */
        @Override public Map<PartitionHashRecord, List<PartitionEntryHashRecord>> execute() throws IgniteException {
            CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(partKey.groupId());

            if (grpCtx == null)
                return Collections.emptyMap();

            GridDhtLocalPartition part = grpCtx.topology().localPartition(partKey.partitionId());

            if (part == null || !part.reserve())
                return Collections.emptyMap();

            HashMap<Integer, GridCacheContext> cacheIdToCtx = new HashMap<>();

            for (GridCacheContext ctx : grpCtx.caches())
                cacheIdToCtx.put(ctx.cacheId(), ctx);

            try {
                if (part.state() != GridDhtPartitionState.OWNING)
                    return Collections.emptyMap();

                if (part.updateCounter() != partHashRecord.updateCounter()) {
                    throw new IgniteException("Cluster is not idle: update counter of partition " + partKey.toString() +
                        " changed during hash calculation [before=" + partHashRecord.updateCounter() +
                        ", after=" + part.updateCounter() + "]");
                }

                for (PartitionEntryHashRecord entryHashRecord : entryHashRecords) {
                    GridCacheContext ctx = cacheIdToCtx.get(entryHashRecord.cacheId());

                    if (ctx == null)
                        continue;

                    KeyCacheObject key = grpCtx.shared().kernalContext().cacheObjects().toKeyCacheObject(
                        grpCtx.cacheObjectContext(), entryHashRecord.key().cacheObjectType(), entryHashRecord.keyBytes());

                    CacheDataRow row = part.dataStore().find(ctx, key);

                    if (row == null)
                        continue;

                    CacheObject val = row.value();

                    Object o = CacheObjectUtils.unwrapBinaryIfNeeded(grpCtx.cacheObjectContext(), val, true, true);

                    if (o != null)
                        entryHashRecord.valueString(o.toString());

                    entryHashRecord.valueBytes(row.value().valueBytes(grpCtx.cacheObjectContext()));
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Can't retrieve value for partition " + partKey.toString(), e);

                return Collections.emptyMap();
            }
            finally {
                part.release();
            }

            return new T2<>(partHashRecord, entryHashRecords);
        }
    }
}
