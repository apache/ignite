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

package org.apache.ignite.internal.visor.node;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.logMapped;

/**
 * Collects current Grid state mostly topology and metrics.
 */
@GridInternal
public class VisorNodeDataCollectorTask extends VisorMultiNodeTask<VisorNodeDataCollectorTaskArg,
    VisorNodeDataCollectorTaskResult, VisorNodeDataCollectorJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(List<ClusterNode> subgrid,
        VisorTaskArgument<VisorNodeDataCollectorTaskArg> arg) {
        assert arg != null;

        Map<ComputeJob, ClusterNode> map = U.newHashMap(subgrid.size());

        try {
            for (ClusterNode node : subgrid)
                map.put(job(taskArg), node);

            return map;
        }
        finally {
            if (debug)
                logMapped(ignite.log(), getClass(), map.values());
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorNodeDataCollectorJob job(VisorNodeDataCollectorTaskArg arg) {
        return new VisorNodeDataCollectorJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected VisorNodeDataCollectorTaskResult reduce0(List<ComputeJobResult> results) {
        return reduce(new VisorNodeDataCollectorTaskResult(), results);
    }

    /**
     * @param taskRes Task result.
     * @param results Results.
     * @return Data collector task result.
     */
    protected VisorNodeDataCollectorTaskResult reduce(VisorNodeDataCollectorTaskResult taskRes,
        List<ComputeJobResult> results) {
        for (ComputeJobResult res : results) {
            VisorNodeDataCollectorJobResult jobRes = res.getData();

            if (jobRes != null) {
                UUID nid = res.getNode().id();

                IgniteException unhandledEx = res.getException();

                if (unhandledEx == null)
                    reduceJobResult(taskRes, jobRes, nid);
                else {
                    // Ignore nodes that left topology.
                    if (!(unhandledEx instanceof ClusterGroupEmptyException))
                        taskRes.unhandledEx().put(nid, new VisorExceptionWrapper(unhandledEx));
                }
            }
        }

        return taskRes;
    }

    /**
     * Reduce job result.
     *
     * @param taskRes Task result.
     * @param jobRes Job result.
     * @param nid Node ID.
     */
    protected void reduceJobResult(VisorNodeDataCollectorTaskResult taskRes,
        VisorNodeDataCollectorJobResult jobRes, UUID nid) {
        taskRes.gridNames().put(nid, jobRes.gridName());

        taskRes.topologyVersions().put(nid, jobRes.topologyVersion());

        taskRes.taskMonitoringEnabled().put(nid, jobRes.taskMonitoringEnabled());

        taskRes.errorCounts().put(nid, jobRes.errorCount());

        if (!jobRes.events().isEmpty())
            taskRes.events().addAll(jobRes.events());

        if (jobRes.eventsEx() != null)
            taskRes.eventsEx().put(nid, new VisorExceptionWrapper(jobRes.eventsEx()));

        if (!jobRes.caches().isEmpty())
            taskRes.caches().put(nid, jobRes.caches());

        if (jobRes.cachesEx() != null)
            taskRes.cachesEx().put(nid, new VisorExceptionWrapper(jobRes.cachesEx()));

        if (!jobRes.igfss().isEmpty())
            taskRes.igfss().put(nid, jobRes.igfss());

        if (!jobRes.igfsEndpoints().isEmpty())
            taskRes.igfsEndpoints().put(nid, jobRes.igfsEndpoints());

        if (jobRes.igfssEx() != null)
            taskRes.igfssEx().put(nid, new VisorExceptionWrapper(jobRes.igfssEx()));
    }
}