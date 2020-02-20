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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionKeyVersion;
import org.apache.ignite.internal.processors.cache.checker.util.KeyComparator;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects and returns a set of keys that have conflicts with {@link GridCacheVersion}.
 */
@GridInternal
public class CollectPartitionKeysByBatchTask extends ComputeTaskAdapter<PartitionBatchRequest, ExecutionResult<T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>>>> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    private static final KeyComparator KEY_COMPARATOR = new KeyComparator();

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** Partition batch. */
    private volatile PartitionBatchRequest partBatch;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        PartitionBatchRequest partBatch) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        this.partBatch = partBatch;

        for (ClusterNode node : subgrid)
            jobs.put(new CollectPartitionKeysByBatchJob(partBatch), node);

        return jobs;
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

    /** {@inheritDoc} */
    @Override public @Nullable ExecutionResult<T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>>> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        assert partBatch != null;

        GridCacheContext<Object, Object> ctx = ignite.context().cache().cache(partBatch.cacheName()).context();

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> totalRes = new HashMap<>();

        KeyCacheObject lastKey = null;

        for (int i = 0; i < results.size(); i++) {
            IgniteException exc = results.get(i).getException();

            if (exc != null)
                return new ExecutionResult<>(exc.getMessage());

            ExecutionResult<List<PartitionKeyVersion>> nodeRes = results.get(i).getData();

            if (nodeRes.getErrorMessage() != null)
                return new ExecutionResult<>(nodeRes.getErrorMessage());

            for (PartitionKeyVersion partKeyVer : nodeRes.getResult()) {
                try {
                    KeyCacheObject key = unmarshalKey(partKeyVer.getKey(), ctx);

                    if (lastKey == null || KEY_COMPARATOR.compare(lastKey, key) < 0)
                        lastKey = key;

                    Map<UUID, GridCacheVersion> map = totalRes.computeIfAbsent(key, k -> new HashMap<>());
                    map.put(partKeyVer.getNodeId(), partKeyVer.getVersion());

                    if (i == (results.size() - 1) && map.size() == results.size() && !hasConflict(map.values()))
                        totalRes.remove(key);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, e.getMessage(), e);

                    return new ExecutionResult<>(e.getMessage());
                }
            }
        }

        return new ExecutionResult<>(new T2<>(lastKey, totalRes));
    }

    /**
     *
     */
    private boolean hasConflict(Collection<GridCacheVersion> keyVersions) {
        assert !keyVersions.isEmpty();

        Iterator<GridCacheVersion> iter = keyVersions.iterator();
        GridCacheVersion ver = iter.next();

        while (iter.hasNext()) {
            if (!ver.equals(iter.next()))
                return true;
        }

        return false;
    }

    /**
     *
     */
    public static class CollectPartitionKeysByBatchJob extends ReconciliationResourceLimitedJob {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** Partition key. */
        private PartitionBatchRequest partBatch;

        /**
         * @param partBatch Partition key.
         */
        private CollectPartitionKeysByBatchJob(PartitionBatchRequest partBatch) {
            this.partBatch = partBatch;
        }

        /** {@inheritDoc} */
        @Override protected long sessionId() {
            return partBatch.sessionId();
        }

        /** {@inheritDoc} */
        @Override protected ExecutionResult<List<PartitionKeyVersion>> execute0() {
            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(partBatch.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            final int batchSize = partBatch.batchSize();
            final KeyCacheObject lowerKey;

            try {
                lowerKey = unmarshalKey(partBatch.lowerKey(), cctx);
            }
            catch (IgniteCheckedException e) {
                String errMsg = "Batch [" + partBatch + "] can't processed. Broken key.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }

            GridDhtLocalPartition part = grpCtx.topology().localPartition(partBatch.partitionId());

            assert part != null;

            part.reserve();

            try (GridCursor<? extends CacheDataRow> cursor = lowerKey == null ?
                grpCtx.offheap().dataStore(part).cursor(cctx.cacheId()) :
                grpCtx.offheap().dataStore(part).cursor(cctx.cacheId(), lowerKey, null)) {

                List<PartitionKeyVersion> partEntryHashRecords = new ArrayList<>();

                for (int i = 0; i < batchSize && cursor.next(); i++) {
                    CacheDataRow row = cursor.get();

                    if (lowerKey == null || KEY_COMPARATOR.compare(lowerKey, row.key()) != 0)
                        partEntryHashRecords.add(new PartitionKeyVersion(
                            ignite.localNode().id(),
                            row.key(),
                            row.version()
                        ));
                    else
                        i--;
                }

                return new ExecutionResult<>(partEntryHashRecords);
            }
            catch (Exception e) {
                String errMsg = "Batch [" + partBatch + "] can't processed. Broken cursor.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }
            finally {
                part.release();
            }
        }
    }
}
