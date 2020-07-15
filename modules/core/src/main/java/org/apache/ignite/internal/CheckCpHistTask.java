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

package org.apache.ignite.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Task that checks whether last checkpoint is applicable for providing history for all groups and partitions that are
 * passed as parameters. If at least one group or partition can't be supplied due to absence of last checkpoint, the task
 * enforces a checkpoint to ensure possibility of the historical rebalancing.
 * The task takes as parametes a collection by fillowing structure: Map{node id -> Map{Group id -> Set{Partition id}}}.
 * Each node that is mentioned in parameter is receiving own particular collection: Map{Group id -> Set{Partition id}}.
 */
@GridInternal
public class CheckCpHistTask extends ComputeTaskAdapter<Map<UUID, Map<Integer, Set<Integer>>>, Boolean> {
    /** Serial version id. */
    private static final long serialVersionUID = 0L;

    /** Reason of checkpoint, which can be triggered by this task. */
    public static final String CP_REASON = "required by another node which is performing a graceful shutdown";

    /** {@inheritDoc} */
    @Override public Map<CheckCpHistClosureJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        Map<UUID, Map<Integer, Set<Integer>>> arg
    ) throws IgniteException {
        Map<CheckCpHistClosureJob, ClusterNode> res = new HashMap<>();

        for (ClusterNode node : subgrid) {
            if (arg.containsKey(node.id()))
                res.put(new CheckCpHistClosureJob(arg.get(node.id())), node);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(
        ComputeJobResult res,
        List<ComputeJobResult> rcvd
    ) throws IgniteException {
        if (res.getException() != null)
            return super.result(res, rcvd);

        if (!(boolean)res.getData())
            return ComputeJobResultPolicy.REDUCE;

        return ComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
        for (ComputeJobResult result : results) {
            if (!(boolean)result.getData())
                return false;
        }

        return true;
    }

    /**
     * Job of checkpoint history task.
     */
    private static class CheckCpHistClosureJob implements ComputeJob {
        /** Serial version id. */
        private static final long serialVersionUID = 0L;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Auto-inject ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Cancelled job flag. */
        private volatile boolean cancelled;

        /** list of group's ids. */
        Map<Integer, Set<Integer>> grpIds;

        /**
         * @param grpIds Map of group id to set of partitions.
         */
        public CheckCpHistClosureJob(Map<Integer, Set<Integer>> grpIds) {
            this.grpIds = grpIds;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            cancelled = true;
        }

        /** {@inheritDoc} */
        @Override public Boolean execute() throws IgniteException {
            IgniteEx igniteEx = (IgniteEx)ignite;

            if (igniteEx.context().cache().context().database() instanceof GridCacheDatabaseSharedManager) {
                GridCacheSharedContext cctx = igniteEx.context().cache().context();
                GridCacheDatabaseSharedManager databaseMng = (GridCacheDatabaseSharedManager)cctx.database();
                CheckpointHistory cpHist = databaseMng.checkpointHistory();

                CheckpointEntry lastCp = cpHist.lastCheckpoint();

                try {
                    Map<Integer, CheckpointEntry.GroupState> states = lastCp.groupState(cctx);

                    for (Integer grpId : grpIds.keySet()) {
                        if (cancelled)
                            return false;

                        if (!cpHist.isCheckpointApplicableForGroup(grpId, lastCp)) {
                            databaseMng.forceCheckpoint(CP_REASON);

                            break;
                        }

                        CheckpointEntry.GroupState groupState = states.get(grpId);

                        for (int p : grpIds.get(grpId)) {
                            if (groupState.indexByPartition(p) < 0) {
                                databaseMng.forceCheckpoint(CP_REASON);

                                break;
                            }
                        }
                    }
                }
                catch (IgniteCheckedException e) {
                    log.warning("Can not read checkpoint [cp=" + lastCp.checkpointId() + ']', e);

                    return false;
                }
            }

            return true;
        }
    }
}
