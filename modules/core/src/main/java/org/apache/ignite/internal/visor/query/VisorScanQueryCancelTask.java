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
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

/**
 * Task to cancel scan queries.
 */
@GridInternal
@GridVisorManagementTask
public class VisorScanQueryCancelTask extends VisorOneNodeTask<VisorScanQueryCancelTaskArg, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorScanQueryCancelJob job(VisorScanQueryCancelTaskArg arg) {
        return new VisorScanQueryCancelJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<VisorScanQueryCancelTaskArg> arg) {
        return F.transform(ignite.cluster().nodes(), ClusterNode::id);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Boolean reduce0(List<ComputeJobResult> results) throws IgniteException {
        for (ComputeJobResult res : results) {
            if (res.getData() != null && ((Boolean)res.getData()))
                return true;
        }

        return false;
    }

    /**
     * Job to cancel scan queries on node.
     */
    private static class VisorScanQueryCancelJob extends VisorJob<VisorScanQueryCancelTaskArg, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorScanQueryCancelJob(@Nullable VisorScanQueryCancelTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Boolean run(@Nullable VisorScanQueryCancelTaskArg arg) throws IgniteException {
            IgniteLogger log = ignite.log().getLogger(VisorScanQueryCancelJob.class);

            int cacheId = CU.cacheId(arg.getCacheName());

            GridCacheContext<?, ?> ctx = ignite.context().cache().context().cacheContext(cacheId);

            if (ctx == null) {
                log.warning("Cache not found[cacheName=" + arg.getCacheName() + ']');

                return false;
            }

            return ctx.queries().removeQueryResult(arg.getOriginNodeId(), arg.getQueryId());
        }
    }
}
