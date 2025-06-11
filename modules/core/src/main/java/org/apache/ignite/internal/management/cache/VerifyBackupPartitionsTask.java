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
package org.apache.ignite.internal.management.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.regex.Pattern;
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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.verify.GridNotIdleException;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyMap;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.GRID_NOT_IDLE_MSG;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.calculatePartitionHash;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.checkPartitionsPageCrcSum;

/**
 * Task for comparing update counters and checksums between primary and backup partitions of specified caches.
 * <br>
 * Argument: Set of cache names, 'null' will trigger verification for all caches.
 * <br>
 * Result: {@link IdleVerifyResult} with conflict partitions.
 * <br>
 * Works properly only on idle cluster - there may be false positive conflict reports if data in cluster is being
 * concurrently updated.
 */
@GridInternal
public class VerifyBackupPartitionsTask extends ComputeTaskAdapter<CacheIdleVerifyCommandArg, IdleVerifyResult> {
    /** Error thrown when idle_verify is called on an inactive cluster with persistence. */
    public static final String IDLE_VERIFY_ON_INACTIVE_CLUSTER_ERROR_MESSAGE = "Cannot perform the operation because " +
        "the cluster is inactive.";

    /** */
    public static final String CACL_PART_HASH_ERR_MSG = "Can't calculate partition hash";

    /** Checkpoint reason. */
    public static final String CP_REASON = "VerifyBackupPartitions";

    /** Shared for tests. */
    public static Supplier<ForkJoinPool> poolSupplier = ForkJoinPool::commonPool;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        CacheIdleVerifyCommandArg arg
    ) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.put(new VerifyBackupPartitionsJob(arg), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IdleVerifyResult reduce(List<ComputeJobResult> results) throws IgniteException {
        return reduce0(results);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(
        ComputeJobResult res,
        List<ComputeJobResult> rcvd
    ) throws IgniteException {
        try {
            ComputeJobResultPolicy superRes = super.result(res, rcvd);

            // Deny failover.
            if (superRes == ComputeJobResultPolicy.FAILOVER) {
                superRes = ComputeJobResultPolicy.WAIT;

                if (log != null) {
                    log.warning("VerifyBackupPartitionsJob failed on node " +
                        "[consistentId=" + res.getNode().consistentId() + "]", res.getException());
                }
            }

            return superRes;
        }
        catch (IgniteException e) {
            return ComputeJobResultPolicy.WAIT;
        }
    }

    /**
     * @param results Received results of broadcast remote requests.
     * @return Idle verify job result constructed from results of remote executions.
     */
    public static IdleVerifyResult reduce0(List<ComputeJobResult> results) {
        IdleVerifyResult.Builder bldr = IdleVerifyResult.builder();

        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                bldr.addException(res.getNode(), res.getException());

                continue;
            }

            Map<PartitionKey, PartitionHashRecord> nodeHashes = res.getData();

            bldr.addPartitionHashes(nodeHashes);
        }

        return bldr.build();
    }

    /**
     * Job that collects update counters and hashes of local partitions.
     */
    private static class VerifyBackupPartitionsJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Idle verify arguments. */
        private CacheIdleVerifyCommandArg arg;

        /** Counter of processed partitions. */
        private final AtomicInteger completionCntr = new AtomicInteger(0);

        /**
         * @param arg Argument.
         */
        public VerifyBackupPartitionsJob(CacheIdleVerifyCommandArg arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public Map<PartitionKey, PartitionHashRecord> execute() throws IgniteException {
            if (!ignite.context().state().publicApiActiveState(true))
                throw new IgniteException(IDLE_VERIFY_ON_INACTIVE_CLUSTER_ERROR_MESSAGE);

            try {
                ignite.context().cache().context().database().waitForCheckpoint(CP_REASON);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(
                    "Failed to wait for checkpoint before executing verify backup partitions task", e);
            }

            if (isCancelled())
                throw new IgniteException(getClass().getSimpleName() + " was cancelled.");

            Set<Integer> grpIds = getGroupIds();

            if (log.isInfoEnabled()) {
                log.info("Idle verify procedure has started [skipZeros=" + arg.skipZeros() + ", checkCrc=" +
                    arg.checkCrc() + ", grpIds=" + grpIds + "]");
            }

            completionCntr.set(0);

            List<Future<Map<PartitionKey, PartitionHashRecord>>> partHashCalcFuts =
                calcPartitionHashAsync(grpIds);

            Map<PartitionKey, PartitionHashRecord> res = new HashMap<>();

            List<IgniteException> exceptions = new ArrayList<>();

            long lastProgressLogTs = U.currentTimeMillis();

            for (int i = 0; i < partHashCalcFuts.size(); ) {
                if (isCancelled()) {
                    cancelFuts(i, partHashCalcFuts);

                    throw new IgniteException(getClass().getSimpleName() + " was cancelled.");
                }

                Future<Map<PartitionKey, PartitionHashRecord>> fut = partHashCalcFuts.get(i);

                try {
                    Map<PartitionKey, PartitionHashRecord> partHash = fut.get(100, TimeUnit.MILLISECONDS);

                    res.putAll(partHash);

                    i++;
                }
                catch (InterruptedException | ExecutionException e) {
                    if (e.getCause() instanceof IgniteException && !(e.getCause() instanceof GridNotIdleException)) {
                        exceptions.add((IgniteException)e.getCause());

                        i++;

                        continue;
                    }

                    cancelFuts(i, partHashCalcFuts);

                    if (e instanceof InterruptedException)
                        throw new IgniteInterruptedException((InterruptedException)e);
                    else
                        throw new IgniteException(e.getCause());
                }
                catch (TimeoutException ignored) {
                    if (U.currentTimeMillis() - lastProgressLogTs > 3 * 60 * 1000L) {
                        lastProgressLogTs = U.currentTimeMillis();

                        log.warning("idle_verify is still running, processed " + completionCntr.get() + " of " +
                            partHashCalcFuts.size() + " local partitions");
                    }
                }
            }

            if (log.isInfoEnabled())
                log.info("Idle verify procedure has finished.");

            if (!F.isEmpty(exceptions))
                throw new IdleVerifyException(exceptions);

            return res;
        }

        /**
         * Cancels futures from given index.
         *
         * @param start Index to start from.
         * @param partHashCalcFuts Partitions hash calculation futures to cancel.
         */
        private void cancelFuts(int start, List<Future<Map<PartitionKey, PartitionHashRecord>>> partHashCalcFuts) {
            for (int i = start; i < partHashCalcFuts.size(); i++)
                partHashCalcFuts.get(i).cancel(false);
        }

        /**
         * Class that processes cache filtering chain.
         */
        private class CachesFiltering {
            /**
             * Initially all cache descriptors are included.
             */
            private final Set<CacheGroupContext> filteredCacheGroups;

            /** */
            public CachesFiltering(Collection<CacheGroupContext> cacheGroups) {
                filteredCacheGroups = new HashSet<>(cacheGroups);
            }

            /**
             * Applies filtering closure.
             *
             * @param closure filter
             * @return this
             */
            public CachesFiltering filter(IgniteInClosure<Set<CacheGroupContext>> closure) {
                closure.apply(filteredCacheGroups);

                return this;
            }

            /**
             * Returns result set of cache ids.
             *
             * @return set of filtered cache ids.
             */
            public Set<Integer> result() {
                Set<Integer> res = new HashSet<>();

                for (CacheGroupContext cacheGrp : filteredCacheGroups)
                    res.add(cacheGrp.groupId());

                return res;
            }
        }

        /** */
        private List<Future<Map<PartitionKey, PartitionHashRecord>>> calcPartitionHashAsync(
            Set<Integer> grpIds
        ) {
            List<Future<Map<PartitionKey, PartitionHashRecord>>> partHashCalcFutures = new ArrayList<>();

            for (Integer grpId : grpIds) {
                CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(grpId);

                if (grpCtx == null)
                    continue;

                ForkJoinPool pool = poolSupplier.get();

                for (GridDhtLocalPartition part : grpCtx.topology().currentLocalPartitions())
                    partHashCalcFutures.add(calculatePartitionHashAsync(pool, grpCtx, part, this::isCancelled));
            }

            return partHashCalcFutures;
        }

        /** */
        private Set<Integer> getGroupIds() {
            Collection<CacheGroupContext> cacheGrps = ignite.context().cache().cacheGroups();

            Set<Integer> grpIds = new CachesFiltering(cacheGrps)
                .filter(this::filterByCacheNames)
                .filter(this::filterByCacheFilter)
                .filter(this::filterByExcludeCaches)
                .result();

            if (F.isEmpty(grpIds))
                throw new NoMatchingCachesException();

            return grpIds;
        }

        /**
         * Filters cache groups by exclude regexps.
         *
         * @param cachesToFilter cache groups to filter
         */
        private void filterByExcludeCaches(Set<CacheGroupContext> cachesToFilter) {
            if (!F.isEmpty(arg.excludeCaches())) {
                Set<Pattern> excludedNamesPatterns = new HashSet<>();

                for (String excluded : arg.excludeCaches())
                    excludedNamesPatterns.add(Pattern.compile(excluded));

                cachesToFilter.removeIf(grp -> doesGrpMatchOneOfPatterns(grp, excludedNamesPatterns));
            }
        }

        /**
         * Filters cache groups by cache filter, also removes system (if not specified in filter option) and local
         * caches.
         *
         * @param cachesToFilter cache groups to filter
         */
        private void filterByCacheFilter(Set<CacheGroupContext> cachesToFilter) {
            cachesToFilter.removeIf(grp -> !doesGrpMatchFilter(grp));
        }

        /**
         * Filters cache groups by whitelist of cache name regexps. By default, all cache groups are included.
         *
         * @param cachesToFilter cache groups to filter
         */
        private void filterByCacheNames(Set<CacheGroupContext> cachesToFilter) {
            if (!F.isEmpty(arg.caches())) {
                Set<Pattern> cacheNamesPatterns = new HashSet<>();

                for (String cacheNameRegexp : arg.caches())
                    cacheNamesPatterns.add(Pattern.compile(cacheNameRegexp));

                cachesToFilter.removeIf(grp -> !doesGrpMatchOneOfPatterns(grp, cacheNamesPatterns));
            }
        }

        /**
         * Checks does the given group match filter.
         *
         * @param grp cache group.
         * @return boolean result
         */
        private boolean doesGrpMatchFilter(CacheGroupContext grp) {
            for (GridCacheContext cacheCtx : grp.caches()) {
                DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(cacheCtx.name());

                if (isCacheMatchFilter(desc))
                    return true;
            }

            return false;
        }

        /**
         * Checks does the name of given cache group or some of the names of its caches match at least one of regexp
         * from set.
         *
         * @param grp cache group
         * @param patterns compiled regexp patterns
         * @return boolean result
         */
        private boolean doesGrpMatchOneOfPatterns(CacheGroupContext grp, Set<Pattern> patterns) {
            for (Pattern pattern : patterns) {
                if (grp.name() != null && pattern.matcher(grp.name()).matches())
                    return true;

                for (GridCacheContext cacheCtx : grp.caches())
                    if (cacheCtx.name() != null && pattern.matcher(cacheCtx.name()).matches())
                        return true;
            }

            return false;
        }

        /**
         * @param desc Cache descriptor.
         */
        private boolean isCacheMatchFilter(DynamicCacheDescriptor desc) {
            DataStorageConfiguration dsCfg = ignite.context().config().getDataStorageConfiguration();

            CacheConfiguration cc = desc.cacheConfiguration();

            switch (arg.cacheFilter()) {
                case DEFAULT:
                    return desc.cacheType().userCache() || !F.isEmpty(arg.caches());

                case USER:
                    return desc.cacheType().userCache();

                case SYSTEM:
                    return !desc.cacheType().userCache();

                case NOT_PERSISTENT:
                    return desc.cacheType().userCache() && !GridCacheUtils.isPersistentCache(cc, dsCfg);

                case PERSISTENT:
                    return desc.cacheType().userCache() && GridCacheUtils.isPersistentCache(cc, dsCfg);

                case ALL:
                    return true;

                default:
                    throw new IgniteException("Illegal cache filter: " + arg.cacheFilter());
            }
        }

        /**
         * @param pool Pool for task submitting.
         * @param gctx Group context.
         * @param part Local partition.
         * @param cancelled Supplier of cancelled status.
         */
        private Future<Map<PartitionKey, PartitionHashRecord>> calculatePartitionHashAsync(
            final ForkJoinPool pool,
            final CacheGroupContext gctx,
            final GridDhtLocalPartition part,
            final BooleanSupplier cancelled
        ) {
            return pool.submit(() -> {
                Map<PartitionKey, PartitionHashRecord> res = emptyMap();

                if (!part.reserve())
                    return res;

                try {
                    PartitionUpdateCounter updCntr = part.dataStore().partUpdateCounter();
                    PartitionUpdateCounter updateCntrBefore = updCntr == null ? null : updCntr.copy();

                    if (arg.checkCrc() && gctx.persistenceEnabled()) {
                        FilePageStoreManager pageStoreMgr =
                            (FilePageStoreManager)ignite.context().cache().context().pageStore();

                        checkPartitionsPageCrcSum(
                            () -> (FilePageStore)pageStoreMgr.getStore(gctx.groupId(), part.id()),
                            part.id(),
                            FLAG_DATA,
                            cancelled
                        );
                    }

                    PartitionKey key = new PartitionKey(gctx.groupId(), part.id(), gctx.cacheOrGroupName());

                    PartitionHashRecord hash = calculatePartitionHash(key,
                        updateCntrBefore == null ? 0 : updateCntrBefore.comparableState(),
                        ignite.context().discovery().localNode().consistentId(),
                        part.state(),
                        part.primary(gctx.topology().readyTopologyVersion()),
                        part.dataStore().fullSize(),
                        gctx.offheap().partitionIterator(part.id()),
                        cancelled
                    );

                    if (hash != null)
                        res = Collections.singletonMap(key, hash);

                    PartitionUpdateCounter updateCntrAfter = part.dataStore().partUpdateCounter();

                    if (updateCntrAfter != null && !updateCntrAfter.equals(updateCntrBefore)) {
                        throw new GridNotIdleException(GRID_NOT_IDLE_MSG + "[grpName=" + gctx.cacheOrGroupName() +
                            ", grpId=" + gctx.groupId() + ", partId=" + part.id() + "] changed during size " +
                            "calculation [updCntrBefore=" + updateCntrBefore + ", updCntrAfter=" + updateCntrAfter + "]");
                    }
                }
                catch (IgniteCheckedException e) {
                    String msg = CACL_PART_HASH_ERR_MSG + " [grpId=" + gctx.groupId() + ", partId=" + part.id() + "]";

                    U.error(log, msg, e);

                    throw new IgniteException(msg, e);
                }
                finally {
                    part.release();
                }

                completionCntr.incrementAndGet();

                return res;
            });
        }
    }
}
