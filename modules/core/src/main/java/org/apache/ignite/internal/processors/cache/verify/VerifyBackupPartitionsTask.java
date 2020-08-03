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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
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
 * Task for comparing update counters and checksums between primary and backup partitions of specified caches.
 * <br>
 * Argument: Set of cache names, 'null' will trigger verification for all caches.
 * <br>
 * Result: If there are any update counter conflicts (which signals about concurrent updates in
 * cluster), map with all counter conflicts is returned. Otherwise, map with all hash conflicts is returned.
 * Each conflict is represented by list of {@link PartitionHashRecord} with data from different nodes.
 * Successful verification always returns empty map.
 * <br>
 * Works properly only on idle cluster - there may be false positive conflict reports if data in cluster is being
 * concurrently updated.
 *
 * @deprecated Legacy version of {@link VerifyBackupPartitionsTaskV2}.
 */
@GridInternal
@Deprecated
public class VerifyBackupPartitionsTask extends ComputeTaskAdapter<Set<String>,
    Map<PartitionKey, List<PartitionHashRecord>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid, Set<String> cacheNames) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.put(new VerifyBackupPartitionsJob(cacheNames), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<PartitionKey, List<PartitionHashRecord>> reduce(List<ComputeJobResult> results)
        throws IgniteException {
        Map<PartitionKey, List<PartitionHashRecord>> clusterHashes = new HashMap<>();

        for (ComputeJobResult res : results) {
            Map<PartitionKey, PartitionHashRecord> nodeHashes = res.getData();

            for (Map.Entry<PartitionKey, PartitionHashRecord> e : nodeHashes.entrySet()) {
                if (!clusterHashes.containsKey(e.getKey()))
                    clusterHashes.put(e.getKey(), new ArrayList<>());

                clusterHashes.get(e.getKey()).add(e.getValue());
            }
        }

        Map<PartitionKey, List<PartitionHashRecord>> hashConflicts = new HashMap<>();

        Map<PartitionKey, List<PartitionHashRecord>> updateCntrConflicts = new HashMap<>();

        for (Map.Entry<PartitionKey, List<PartitionHashRecord>> e : clusterHashes.entrySet()) {
            Integer partHash = null;
            Long updateCntr = null;

            for (PartitionHashRecord record : e.getValue()) {
                if (partHash == null) {
                    partHash = record.partitionHash();

                    updateCntr = record.updateCounter();
                }
                else {
                    if (record.updateCounter() != updateCntr) {
                        updateCntrConflicts.put(e.getKey(), e.getValue());

                        break;
                    }

                    if (record.partitionHash() != partHash) {
                        hashConflicts.put(e.getKey(), e.getValue());

                        break;
                    }
                }
            }
        }

        return updateCntrConflicts.isEmpty() ? hashConflicts : updateCntrConflicts;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        ComputeJobResultPolicy superRes = super.result(res, rcvd);

        // Deny failover.
        if (superRes == ComputeJobResultPolicy.FAILOVER) {
            superRes = ComputeJobResultPolicy.WAIT;

            log.warning("VerifyBackupPartitionsJob failed on node " +
                "[consistentId=" + res.getNode().consistentId() + "]", res.getException());
        }

        return superRes;
    }

    /**
     * Legacy version of {@link VerifyBackupPartitionsTaskV2} internal job, kept for compatibility.
     */
    @Deprecated
    private static class VerifyBackupPartitionsJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Cache names. */
        private Set<String> cacheNames;

        /** Counter of processed partitions. */
        private final AtomicInteger completionCntr = new AtomicInteger(0);

        /**
         * @param names Names.
         */
        public VerifyBackupPartitionsJob(Set<String> names) {
            cacheNames = names;
        }

        /** {@inheritDoc} */
        @Override public Map<PartitionKey, PartitionHashRecord> execute() throws IgniteException {
            Set<Integer> grpIds = new HashSet<>();

            Set<String> missingCaches = new HashSet<>();

            if (cacheNames != null) {
                for (String cacheName : cacheNames) {
                    DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(cacheName);

                    if (desc == null) {
                        missingCaches.add(cacheName);

                        continue;
                    }

                    grpIds.add(desc.groupId());
                }

                if (!missingCaches.isEmpty()) {
                    StringBuilder strBuilder = new StringBuilder("The following caches do not exist: ");

                    for (String name : missingCaches)
                        strBuilder.append(name).append(", ");

                    strBuilder.delete(strBuilder.length() - 2, strBuilder.length());

                    throw new IgniteException(strBuilder.toString());
                }
            }
            else {
                Collection<CacheGroupContext> groups = ignite.context().cache().cacheGroups();

                for (CacheGroupContext grp : groups) {
                    if (!grp.systemCache() && !grp.isLocal())
                        grpIds.add(grp.groupId());
                }
            }

            List<Future<Map<PartitionKey, PartitionHashRecord>>> partHashCalcFutures = new ArrayList<>();

            completionCntr.set(0);

            for (Integer grpId : grpIds) {
                CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(grpId);

                if (grpCtx == null)
                    continue;

                List<GridDhtLocalPartition> parts = grpCtx.topology().localPartitions();

                for (GridDhtLocalPartition part : parts)
                    partHashCalcFutures.add(calculatePartitionHashAsync(grpCtx, part));
            }

            Map<PartitionKey, PartitionHashRecord> res = new HashMap<>();

            long lastProgressLogTs = U.currentTimeMillis();

            for (int i = 0; i < partHashCalcFutures.size(); ) {
                Future<Map<PartitionKey, PartitionHashRecord>> fut = partHashCalcFutures.get(i);

                try {
                    Map<PartitionKey, PartitionHashRecord> partHash = fut.get(100, TimeUnit.MILLISECONDS);

                    res.putAll(partHash);

                    i++;
                }
                catch (InterruptedException | ExecutionException e) {
                    for (int j = i + 1; j < partHashCalcFutures.size(); j++)
                        partHashCalcFutures.get(j).cancel(false);

                    if (e instanceof InterruptedException)
                        throw new IgniteInterruptedException((InterruptedException)e);
                    else if (e.getCause() instanceof IgniteException)
                        throw (IgniteException)e.getCause();
                    else
                        throw new IgniteException(e.getCause());
                }
                catch (TimeoutException ignored) {
                    if (U.currentTimeMillis() - lastProgressLogTs > 3 * 60 * 1000L) {
                        lastProgressLogTs = U.currentTimeMillis();

                        log.warning("idle_verify is still running, processed " + completionCntr.get() + " of " +
                            partHashCalcFutures.size() + " local partitions");
                    }
                }
            }

            return res;
        }

        /**
         * @param grpCtx Group context.
         * @param part Local partition.
         */
        private Future<Map<PartitionKey, PartitionHashRecord>> calculatePartitionHashAsync(
            final CacheGroupContext grpCtx,
            final GridDhtLocalPartition part
        ) {
            return ForkJoinPool.commonPool().submit(new Callable<Map<PartitionKey, PartitionHashRecord>>() {
                @Override public Map<PartitionKey, PartitionHashRecord> call() throws Exception {
                    return calculatePartitionHash(grpCtx, part);
                }
            });
        }

        /**
         * @param grpCtx Group context.
         * @param part Local partition.
         */
        private Map<PartitionKey, PartitionHashRecord> calculatePartitionHash(
            CacheGroupContext grpCtx,
            GridDhtLocalPartition part
        ) {
            if (!part.reserve())
                return Collections.emptyMap();

            int partHash = 0;
            long partSize;
            long updateCntrBefore;

            try {
                if (part.state() != GridDhtPartitionState.OWNING)
                    return Collections.emptyMap();

                updateCntrBefore = part.updateCounter();

                partSize = part.dataStore().fullSize();

                GridIterator<CacheDataRow> it = grpCtx.offheap().partitionIterator(part.id());

                while (it.hasNextX()) {
                    CacheDataRow row = it.nextX();

                    partHash += row.key().hashCode();

                    partHash += Arrays.hashCode(row.value().valueBytes(grpCtx.cacheObjectContext()));
                }

                long updateCntrAfter = part.updateCounter();

                if (updateCntrBefore != updateCntrAfter) {
                    throw new GridNotIdleException(GRID_NOT_IDLE_MSG + "[grpName=" + grpCtx.cacheOrGroupName() +
                        ", grpId=" + grpCtx.groupId() + ", partId=" + part.id() + "] changed during hash calculation " +
                        "[before=" + updateCntrBefore + ", after=" + updateCntrAfter + "]");
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Can't calculate partition hash [grpId=" + grpCtx.groupId() +
                    ", partId=" + part.id() + "]", e);

                return Collections.emptyMap();
            }
            finally {
                part.release();
            }

            Object consId = ignite.context().discovery().localNode().consistentId();

            boolean isPrimary = part.primary(grpCtx.topology().readyTopologyVersion());

            PartitionKey partKey = new PartitionKey(grpCtx.groupId(), part.id(), grpCtx.cacheOrGroupName());

            PartitionHashRecord partRec = new PartitionHashRecord(
                partKey, isPrimary, consId, partHash, updateCntrBefore, partSize);

            completionCntr.incrementAndGet();

            return Collections.singletonMap(partKey, partRec);
        }
    }
}
