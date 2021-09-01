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

package org.apache.ignite.internal.visor.cache.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

/**
 * Task that collects caches that have index rebuild in progress.
 */
@GridInternal
public class IndexRebuildStatusTask extends VisorMultiNodeTask<
    IndexRebuildStatusTaskArg,
    Map<UUID, Set<IndexRebuildStatusInfoContainer>>,
    Set<IndexRebuildStatusInfoContainer>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected IndexRebuildStatusJob job(IndexRebuildStatusTaskArg arg) {
        return new IndexRebuildStatusJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, Set<IndexRebuildStatusInfoContainer>> reduce0(List<ComputeJobResult> results)
        throws IgniteException
    {
        Map<UUID, Set<IndexRebuildStatusInfoContainer>> reduceRes = new HashMap<>();

        for (ComputeJobResult jobResult: results) {
            if (jobResult.<Set<IndexRebuildStatusInfoContainer>>getData().isEmpty())
                continue;

            reduceRes.put(jobResult.getNode().id(), jobResult.getData());
        }

        return reduceRes;
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<IndexRebuildStatusTaskArg> arg) {
        UUID targetNodeId = arg.getArgument().nodeId();

        if (targetNodeId == null)
            // Collect status from all nodes if nodeId is not specified.
            return ignite.cluster().forServers().nodes().stream().map(ClusterNode::id).collect(Collectors.toSet());
        else
            return Arrays.asList(targetNodeId);
    }

    /**  */
    private static class IndexRebuildStatusJob extends VisorJob<IndexRebuildStatusTaskArg, Set<IndexRebuildStatusInfoContainer>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected IndexRebuildStatusJob(@Nullable IndexRebuildStatusTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Set<IndexRebuildStatusInfoContainer> run(@Nullable IndexRebuildStatusTaskArg arg)
            throws IgniteException
        {
            Set<IgniteCache> rebuildIdxCaches =
                ignite.context().cache().publicCaches().stream()
                    .filter(c -> !c.indexReadyFuture().isDone())
                    .collect(Collectors.toSet());

            Set<IndexRebuildStatusInfoContainer> res = new HashSet<>();

            for (IgniteCache<?, ?> cache : rebuildIdxCaches)
                res.add(new IndexRebuildStatusInfoContainer(cache.getConfiguration(CacheConfiguration.class)));

            return res;
        }
    }
}
