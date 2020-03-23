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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Cancels given tasks sessions on all cluster nodes.
 */
@GridInternal
@GridVisorManagementTask
public class VisorComputeCancelSessionTask
    extends VisorOneNodeTask<VisorComputeCancelSessionTaskArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorComputeCancelSessionJob job(VisorComputeCancelSessionTaskArg arg) {
        return new VisorComputeCancelSessionJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Void reduce0(List<ComputeJobResult> results) {
        // No-op, just awaiting all jobs done.
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<VisorComputeCancelSessionTaskArg> arg) {
        List<UUID> collect = ignite.cluster().nodes().stream().map(ClusterNode::id).collect(Collectors.toList());

        System.out.println("collect = " + collect);

        return arg.getNodes();
    }

    /**
     * Job that cancel tasks.
     */
    private static class VisorComputeCancelSessionJob extends VisorJob<VisorComputeCancelSessionTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Map with task sessions IDs to cancel.
         * @param debug Debug flag.
         */
        private VisorComputeCancelSessionJob(VisorComputeCancelSessionTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorComputeCancelSessionTaskArg arg) {
            ignite.compute(ignite.cluster()).broadcast(new IgniteClosure<IgniteUuid, Void>() {
                /** Auto-injected grid instance. */
                @IgniteInstanceResource
                private transient IgniteEx ignite;

                /** {@inheritDoc} */
                @Override public Void apply(IgniteUuid uuid) {
                    IgniteUuid sesId = arg.getSessionId();

                    ignite.context().job().cancelJob(sesId, null, false);

                    IgniteCompute compute = ignite.compute(ignite.cluster().forLocal());

                    Map<IgniteUuid, ComputeTaskFuture<Object>> futs = compute.activeTaskFutures();

                    ComputeTaskFuture<Object> fut = futs.get(sesId);

                    if (fut != null)
                        fut.cancel();

                    return null;
                }
            }, arg.getSessionId());

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeCancelSessionJob.class, this);
        }
    }
}
