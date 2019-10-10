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
import java.util.UUID;
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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyDumpTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Task for comparing update counters and checksums between primary and backup partitions of specified caches.
 * <br>
 * Argument: Set of cache names, 'null' will trigger verification for all caches.
 * <br>
 * Result: {@link IdleVerifyResultV2} with conflict partitions.
 * <br>
 * Works properly only on idle cluster - there may be false positive conflict reports if data in cluster is being
 * concurrently updated.
 */
@GridInternal
public class VerifyBackupPartitionsTaskV2 extends ComputeTaskAdapter<VisorIdleVerifyTaskArg, IdleVerifyResultV2> {
    /** First version of Ignite that is capable of executing Idle Verify V2. */
    public static final IgniteProductVersion V2_SINCE_VER = IgniteProductVersion.fromString("2.5.3");

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid, VisorIdleVerifyTaskArg arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.put(new VerifyBackupPartitionsJobV2(arg), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IdleVerifyResultV2 reduce(List<ComputeJobResult> results)
        throws IgniteException {
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> clusterHashes = new HashMap<>();
        Map<UUID, Exception> exceptions = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                exceptions.put(res.getNode().id(), res.getException());

                continue;
            }

            Map<PartitionKeyV2, PartitionHashRecordV2> nodeHashes = res.getData();

            for (Map.Entry<PartitionKeyV2, PartitionHashRecordV2> e : nodeHashes.entrySet()) {
                List<PartitionHashRecordV2> records = clusterHashes.computeIfAbsent(e.getKey(), k -> new ArrayList<>());

                records.add(e.getValue());
            }
        }

        Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts = new HashMap<>();

        Map<PartitionKeyV2, List<PartitionHashRecordV2>> updateCntrConflicts = new HashMap<>();

        Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingParts = new HashMap<>();

        for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> e : clusterHashes.entrySet()) {
            Integer partHash = null;
            Long updateCntr = null;

            for (PartitionHashRecordV2 record : e.getValue()) {
                if (record.size() == PartitionHashRecordV2.MOVING_PARTITION_SIZE) {
                    List<PartitionHashRecordV2> records = movingParts.computeIfAbsent(
                        e.getKey(), k -> new ArrayList<>());

                    records.add(record);

                    continue;
                }

                if (partHash == null) {
                    partHash = record.partitionHash();

                    updateCntr = record.updateCounter();
                }
                else {
                    if (record.updateCounter() != updateCntr)
                        updateCntrConflicts.putIfAbsent(e.getKey(), e.getValue());

                    if (record.partitionHash() != partHash)
                        hashConflicts.putIfAbsent(e.getKey(), e.getValue());
                }
            }
        }

        return new IdleVerifyResultV2(updateCntrConflicts, hashConflicts, movingParts, exceptions);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws
        IgniteException {
        ComputeJobResultPolicy superRes = super.result(res, rcvd);

        // Deny failover.
        if (superRes == ComputeJobResultPolicy.FAILOVER) {
            superRes = ComputeJobResultPolicy.WAIT;

            log.warning("VerifyBackupPartitionsJobV2 failed on node " +
                "[consistentId=" + res.getNode().consistentId() + "]", res.getException());
        }

        return superRes;
    }

    /**
     * Job that collects update counters and hashes of local partitions.
     */
    private static class VerifyBackupPartitionsJobV2 extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Idle verify arguments. */
        private VisorIdleVerifyTaskArg arg;

        /** Counter of processed partitions. */
        private final AtomicInteger completionCntr = new AtomicInteger(0);

        /**
         * @param arg Argument.
         */
        public VerifyBackupPartitionsJobV2(VisorIdleVerifyTaskArg arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public Map<PartitionKeyV2, PartitionHashRecordV2> execute() throws IgniteException {
            Set<Integer> grpIds = new HashSet<>();

            if (arg.getCaches() != null && !arg.getCaches().isEmpty()) {
                Set<String> missingCaches = new HashSet<>();

                for (String cacheName : arg.getCaches()) {
                    DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(cacheName);

                    if (desc == null || !isCacheMatchFilter(cacheName)) {
                        missingCaches.add(cacheName);

                        continue;
                    }

                    grpIds.add(desc.groupId());
                }

                handlingMissedCaches(missingCaches);
            }
            else if (onlySpecificCaches()) {
                for (DynamicCacheDescriptor desc : ignite.context().cache().cacheDescriptors().values()) {
                    if (desc.cacheConfiguration().getCacheMode() != CacheMode.LOCAL
                        && isCacheMatchFilter(desc.cacheName()))
                        grpIds.add(desc.groupId());
                }
            }
            else
                grpIds = getCacheGroupIds();

            List<Future<Map<PartitionKeyV2, PartitionHashRecordV2>>> partHashCalcFutures = new ArrayList<>();

            completionCntr.set(0);

            for (Integer grpId : grpIds) {
                CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(grpId);

                if (grpCtx == null)
                    continue;

                List<GridDhtLocalPartition> parts = grpCtx.topology().localPartitions();

                for (GridDhtLocalPartition part : parts)
                    partHashCalcFutures.add(calculatePartitionHashAsync(grpCtx, part));
            }

            Map<PartitionKeyV2, PartitionHashRecordV2> res = new HashMap<>();

            long lastProgressLogTs = U.currentTimeMillis();

            for (int i = 0; i < partHashCalcFutures.size(); ) {
                Future<Map<PartitionKeyV2, PartitionHashRecordV2>> fut = partHashCalcFutures.get(i);

                try {
                    Map<PartitionKeyV2, PartitionHashRecordV2> partHash = fut.get(100, TimeUnit.MILLISECONDS);

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
         * Gets filtered group ids.
         */
        private Set<Integer> getCacheGroupIds() {
            Collection<CacheGroupContext> groups = ignite.context().cache().cacheGroups();

            Set<Integer> grpIds = new HashSet<>();

            if (arg.excludeCaches() == null || arg.excludeCaches().isEmpty()) {
                for (CacheGroupContext grp : groups) {
                    if (!grp.systemCache() && !grp.isLocal())
                        grpIds.add(grp.groupId());
                }
                return grpIds;
            }

            for (CacheGroupContext grp : groups) {
                if (!grp.systemCache() && !grp.isLocal() && !isGrpExcluded(grp))
                    grpIds.add(grp.groupId());
            }

            return grpIds;
        }

        /**
         * @param grp Group.
         */
        private boolean isGrpExcluded(CacheGroupContext grp) {
            if (arg.excludeCaches().contains(grp.name()))
                return true;

            for (GridCacheContext cacheCtx : grp.caches()) {
                if (arg.excludeCaches().contains(cacheCtx.name()))
                    return true;
            }

            return false;
        }

        /**
         *  Checks and throw exception if caches was missed.
         *
         * @param missingCaches Missing caches.
         */
        private void handlingMissedCaches(Set<String> missingCaches) {
            if (missingCaches.isEmpty())
                return;

            StringBuilder strBuilder = new StringBuilder("The following caches do not exist");

            if (onlySpecificCaches()) {
                VisorIdleVerifyDumpTaskArg vdta = (VisorIdleVerifyDumpTaskArg)arg;

                strBuilder.append(" or do not match to the given filter [")
                    .append(vdta.getCacheFilterEnum())
                    .append("]: ");
            }
            else
                strBuilder.append(": ");

            for (String name : missingCaches)
                strBuilder.append(name).append(", ");

            strBuilder.delete(strBuilder.length() - 2, strBuilder.length());

            throw new IgniteException(strBuilder.toString());
        }

        /**
         * @return True if validates only specific caches, else false.
         */
        private boolean onlySpecificCaches() {
            if (arg instanceof VisorIdleVerifyDumpTaskArg) {
                VisorIdleVerifyDumpTaskArg vdta = (VisorIdleVerifyDumpTaskArg)arg;

                return vdta.getCacheFilterEnum() != CacheFilterEnum.ALL;
            }

            return false;
        }

        /**
         * @param cacheName Cache name.
         */
        private boolean isCacheMatchFilter(String cacheName) {
            if (arg instanceof VisorIdleVerifyDumpTaskArg) {
                DataStorageConfiguration dsc = ignite.context().config().getDataStorageConfiguration();
                DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(cacheName);
                CacheConfiguration cc = desc.cacheConfiguration();
                VisorIdleVerifyDumpTaskArg vdta = (VisorIdleVerifyDumpTaskArg)arg;

                switch (vdta.getCacheFilterEnum()) {
                    case SYSTEM:
                        return !desc.cacheType().userCache();

                    case NOT_PERSISTENT:
                        return desc.cacheType().userCache() && !GridCacheUtils.isPersistentCache(cc, dsc);

                    case PERSISTENT:
                        return desc.cacheType().userCache() && GridCacheUtils.isPersistentCache(cc, dsc);

                    case ALL:
                        break;

                    default:
                        assert false: "Illegal cache filter: " + vdta.getCacheFilterEnum();
                }
            }

            return true;
        }

        /**
         * @param grpCtx Group context.
         * @param part Local partition.
         */
        private Future<Map<PartitionKeyV2, PartitionHashRecordV2>> calculatePartitionHashAsync(
            final CacheGroupContext grpCtx,
            final GridDhtLocalPartition part
        ) {
            return ForkJoinPool.commonPool().submit(new Callable<Map<PartitionKeyV2, PartitionHashRecordV2>>() {
                @Override public Map<PartitionKeyV2, PartitionHashRecordV2> call() throws Exception {
                    return calculatePartitionHash(grpCtx, part);
                }
            });
        }

        /**
         * @param grpCtx Group context.
         * @param part Local partition.
         */
        private Map<PartitionKeyV2, PartitionHashRecordV2> calculatePartitionHash(
            CacheGroupContext grpCtx,
            GridDhtLocalPartition part
        ) {
            if (!part.reserve())
                return Collections.emptyMap();

            int partHash = 0;
            long partSize;
            long updateCntrBefore = part.updateCounter();

            PartitionKeyV2 partKey = new PartitionKeyV2(grpCtx.groupId(), part.id(), grpCtx.cacheOrGroupName());

            Object consId = ignite.context().discovery().localNode().consistentId();

            boolean isPrimary = part.primary(grpCtx.topology().readyTopologyVersion());

            try {
                if (part.state() == GridDhtPartitionState.MOVING) {
                    PartitionHashRecordV2 movingHashRecord = new PartitionHashRecordV2(partKey, isPrimary, consId,
                        partHash, updateCntrBefore, PartitionHashRecordV2.MOVING_PARTITION_SIZE);

                    return Collections.singletonMap(partKey, movingHashRecord);
                }
                else if (part.state() != GridDhtPartitionState.OWNING)
                    return Collections.emptyMap();

                partSize = part.dataStore().fullSize();

                GridIterator<CacheDataRow> it = grpCtx.offheap().partitionIterator(part.id());

                while (it.hasNextX()) {
                    CacheDataRow row = it.nextX();

                    partHash += row.key().hashCode();

                    partHash += Arrays.hashCode(row.value().valueBytes(grpCtx.cacheObjectContext()));
                }

                long updateCntrAfter = part.updateCounter();

                if (updateCntrBefore != updateCntrAfter) {
                    throw new IgniteException("Cluster is not idle: update counter of partition [grpId=" +
                        grpCtx.groupId() + ", partId=" + part.id() + "] changed during hash calculation [before=" +
                        updateCntrBefore + ", after=" + updateCntrAfter + "]");
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

            PartitionHashRecordV2 partRec = new PartitionHashRecordV2(
                partKey, isPrimary, consId, partHash, updateCntrBefore, partSize);

            completionCntr.incrementAndGet();

            return Collections.singletonMap(partKey, partRec);
        }
    }
}
