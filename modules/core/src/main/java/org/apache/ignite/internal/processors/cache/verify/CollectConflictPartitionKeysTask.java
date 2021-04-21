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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.GRID_NOT_IDLE_MSG;

/**
 *
 */
@GridInternal
public class CollectConflictPartitionKeysTask extends ComputeTaskAdapter<PartitionKey,
    Map<PartitionHashRecord, List<PartitionEntryHashRecord>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid, PartitionKey partKey) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.put(new CollectConflictPartitionKeysTask.CollectPartitionEntryHashesJob(partKey), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<PartitionHashRecord, List<PartitionEntryHashRecord>> reduce(List<ComputeJobResult> results)
        throws IgniteException {
        Map<PartitionHashRecord, List<PartitionEntryHashRecord>> totalRes = new HashMap<>();

        for (ComputeJobResult res : results) {
            Map<PartitionHashRecord, List<PartitionEntryHashRecord>> nodeRes = res.getData();

            totalRes.putAll(nodeRes);
        }

        Set<PartitionEntryHashRecord> commonEntries = null;

        for (List<PartitionEntryHashRecord> nodeEntryHashRecords : totalRes.values()) {
            HashSet<PartitionEntryHashRecord> set = new HashSet<>(nodeEntryHashRecords);

            if (commonEntries == null)
                commonEntries = set;
            else
                commonEntries.retainAll(set);
        }

        if (commonEntries == null)
            return Collections.emptyMap();

        Map<PartitionHashRecord, List<PartitionEntryHashRecord>> conflictsRes = new HashMap<>();

        for (Map.Entry<PartitionHashRecord, List<PartitionEntryHashRecord>> e : totalRes.entrySet()) {
            HashSet<PartitionEntryHashRecord> conflicts = new HashSet<>(e.getValue());

            conflicts.removeAll(commonEntries);

            if (!conflicts.isEmpty())
                conflictsRes.put(e.getKey(), new ArrayList<>(conflicts));
        }

        return conflictsRes;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        ComputeJobResultPolicy superRes = super.result(res, rcvd);

        // Deny failover.
        if (superRes == ComputeJobResultPolicy.FAILOVER) {
            superRes = ComputeJobResultPolicy.WAIT;

            log.warning("CollectPartitionEntryHashesJob failed on node " +
                "[consistentId=" + res.getNode().consistentId() + "]", res.getException());
        }

        return superRes;
    }

    /**
     *
     */
    public static class CollectPartitionEntryHashesJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Partition key. */
        private PartitionKey partKey;

        /**
         * @param partKey Partition key.
         */
        private CollectPartitionEntryHashesJob(PartitionKey partKey) {
            this.partKey = partKey;
        }

        /** {@inheritDoc} */
        @Override public Map<PartitionHashRecord, List<PartitionEntryHashRecord>> execute() throws IgniteException {
            CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(partKey.groupId());

            if (grpCtx == null)
                return Collections.emptyMap();

            partKey.groupName(grpCtx.cacheOrGroupName());

            GridDhtLocalPartition part = grpCtx.topology().localPartition(partKey.partitionId());

            if (part == null || !part.reserve())
                return Collections.emptyMap();

            int partHash = 0;
            long partSize;
            PartitionUpdateCounter updateCntrBefore;
            List<PartitionEntryHashRecord> partEntryHashRecords;

            try {
                if (part.state() != GridDhtPartitionState.OWNING)
                    return Collections.emptyMap();

                @Nullable PartitionUpdateCounter updCntr = part.dataStore().partUpdateCounter();

                updateCntrBefore = updCntr == null ? null : updCntr.copy();

                partSize = part.dataStore().fullSize();

                GridIterator<CacheDataRow> it = grpCtx.offheap().partitionIterator(part.id());

                partEntryHashRecords = new ArrayList<>();

                while (it.hasNextX()) {
                    CacheDataRow row = it.nextX();

                    partHash += row.key().hashCode();

                    int valHash = Arrays.hashCode(row.value().valueBytes(grpCtx.cacheObjectContext()));
                    partHash += valHash;

                    int cacheId = row.cacheId() == 0 ? grpCtx.groupId() : row.cacheId();
                    DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(cacheId);

                    assert desc != null;

                    Object o = CacheObjectUtils.unwrapBinaryIfNeeded(grpCtx.cacheObjectContext(), row.key(), true, true);

                    partEntryHashRecords.add(new PartitionEntryHashRecord(
                        cacheId, desc.cacheName(), row.key(), o.toString(),
                        row.key().valueBytes(grpCtx.cacheObjectContext()), row.version(), valHash));
                }

                PartitionUpdateCounter updateCntrAfter = part.dataStore().partUpdateCounter();

                if (updateCntrAfter != null && !updateCntrAfter.equals(updateCntrBefore)) {
                    throw new GridNotIdleException(GRID_NOT_IDLE_MSG + "[grpName=" + grpCtx.cacheOrGroupName() +
                        ", grpId=" + grpCtx.groupId() + ", partId=" + part.id() + "] changed during hash calculation " +
                        "[before=" + updateCntrBefore + ", after=" + updateCntrAfter + "]");
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Can't calculate partition hash " + partKey.toString(), e);

                return Collections.emptyMap();
            }
            finally {
                part.release();
            }

            Object consId = ignite.context().discovery().localNode().consistentId();

            boolean isPrimary = part.primary(grpCtx.topology().readyTopologyVersion());

            PartitionHashRecord partHashRec = new PartitionHashRecord(
                partKey, isPrimary, consId, partHash, updateCntrBefore == null ? 0 : updateCntrBefore.get(), partSize);

            Map<PartitionHashRecord, List<PartitionEntryHashRecord>> res = new HashMap<>();

            res.put(partHashRec, partEntryHashRecords);

            return res;
        }
    }
}
