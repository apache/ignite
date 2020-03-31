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

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Task to cancel scan queries.
 */
@GridInternal
@GridVisorManagementTask
public class VisorScanQueryCancelTask extends VisorOneNodeTask<VisorScanQueryCancelTaskArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorScanQueryCancelJob job(VisorScanQueryCancelTaskArg arg) {
        return new VisorScanQueryCancelJob(arg, debug);
    }

    /**
     * Job to cancel scan queries on node.
     */
    private static class VisorScanQueryCancelJob extends VisorJob<VisorScanQueryCancelTaskArg, Void> {
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
        @Override protected Void run(@Nullable VisorScanQueryCancelTaskArg arg) throws IgniteException {
            ignite.compute(ignite.cluster()).broadcast(new IgniteClosure<Long, Void>() {
                /** Auto-injected grid instance. */
                @IgniteInstanceResource
                private transient IgniteEx ignite;

                /** {@inheritDoc} */
                @Override public Void apply(Long qryId) {
                    IgniteLogger log = ignite.log().getLogger(getClass());

                    int cacheId = CU.cacheId(arg.getCacheName());

                    GridCacheContext<?, ?> ctx = ignite.context().cache().context().cacheContext(cacheId);

                    if (ctx == null) {
                        log.warning("Cache not found[cacheName=" + arg.getCacheName() + ']');

                        return null;
                    }

                    ctx.queries().removeQueryResult(arg.getOriginNodeId(), arg.getQueryId());

                    return null;
                }
            }, arg.getQueryId());

            return null;
        }
    }
}
