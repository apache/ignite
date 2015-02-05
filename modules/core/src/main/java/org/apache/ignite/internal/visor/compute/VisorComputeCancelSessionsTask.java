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

package org.apache.ignite.internal.visor.compute;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cancels given tasks sessions.
 */
@GridInternal
public class VisorComputeCancelSessionsTask extends VisorMultiNodeTask<Map<UUID, Set<IgniteUuid>>, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorComputeCancelSessionsJob job(Map<UUID, Set<IgniteUuid>> arg) {
        return new VisorComputeCancelSessionsJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Void reduce0(List<ComputeJobResult> results) {
        // No-op, just awaiting all jobs done.
        return null;
    }

    /**
     * Job that cancel tasks.
     */
    private static class VisorComputeCancelSessionsJob extends VisorJob<Map<UUID, Set<IgniteUuid>>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Map with task sessions IDs to cancel.
         * @param debug Debug flag.
         */
        private VisorComputeCancelSessionsJob(Map<UUID, Set<IgniteUuid>> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Map<UUID, Set<IgniteUuid>> arg) {
            Set<IgniteUuid> sesIds = arg.get(g.localNode().id());

            if (sesIds != null && !sesIds.isEmpty()) {
                IgniteCompute compute = g.compute(g.forLocal());

                Map<IgniteUuid, ComputeTaskFuture<Object>> futs = compute.activeTaskFutures();

                for (IgniteUuid sesId : sesIds) {
                    ComputeTaskFuture<Object> fut = futs.get(sesId);

                    if (fut != null)
                        fut.cancel();
                }
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeCancelSessionsJob.class, this);
        }
    }
}
