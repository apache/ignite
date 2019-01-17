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

package org.apache.ignite.internal.visor.query;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.util.VisorClusterGroupEmptyException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.removeQueryHolder;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.log;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.logMapped;

/**
 * Task for cleanup not needed SCAN or SQL queries result futures from node local.
 */
@GridInternal
public class VisorQueryCleanupTask extends VisorMultiNodeTask<VisorQueryCleanupTaskArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorQueryCleanupTaskArg, Void> job(VisorQueryCleanupTaskArg arg) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(List<ClusterNode> subgrid,
        @Nullable VisorTaskArgument<VisorQueryCleanupTaskArg> arg) {
        Set<UUID> nodeIds = taskArg.getQueryIds().keySet();

        if (nodeIds.isEmpty())
            throw new VisorClusterGroupEmptyException("Nothing to clear. List with node IDs is empty!");

        Map<ComputeJob, ClusterNode> map = U.newHashMap(nodeIds.size());

        try {
            for (ClusterNode node : subgrid)
                if (nodeIds.contains(node.id()))
                    map.put(new VisorQueryCleanupJob(taskArg.getQueryIds().get(node.id()), debug), node);

            if (map.isEmpty()) {
                StringBuilder notFoundNodes = new StringBuilder();

                for (UUID nid : nodeIds)
                    notFoundNodes.append((notFoundNodes.length() == 0) ? "" : ",").append(U.id8(nid));

                throw new VisorClusterGroupEmptyException("Failed to clear query results. Nodes are not available: [" +
                    notFoundNodes + "]");
            }

            return map;
        }
        finally {
            if (debug)
                logMapped(ignite.log(), getClass(), map.values());
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Void reduce0(List list) {
        return null;
    }

    /**
     * Job for cleanup not needed SCAN or SQL queries result futures from node local.
     */
    private static class VisorQueryCleanupJob extends VisorJob<Collection<String>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        protected VisorQueryCleanupJob(Collection<String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Collection<String> qryIds) {
            long start = U.currentTimeMillis();

            if (debug) {
                start = log(
                    ignite.log(),
                    "Queries cancellation started: [" + String.join(", ", qryIds) + "]",
                    getClass(),
                    start);
            }

            for (String qryId : qryIds)
                removeQueryHolder(ignite, qryId);

            if (debug)
                log(ignite.log(), "Queries cancellation finished", getClass(), start);

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryCleanupJob.class, this);
        }
    }
}
