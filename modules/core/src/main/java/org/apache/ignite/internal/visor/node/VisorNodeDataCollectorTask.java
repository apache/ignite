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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

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
                logMapped(g.log(), getClass(), map.values());
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

    protected VisorNodeDataCollectorTaskResult reduce(VisorNodeDataCollectorTaskResult taskResult,
        List<ComputeJobResult> results) {
        for (ComputeJobResult res : results) {
            VisorNodeDataCollectorJobResult jobResult = res.getData();

            if (jobResult != null) {
                UUID nid = res.getNode().id();

                IgniteException unhandledEx = res.getException();

                if (unhandledEx == null)
                    reduceJobResult(taskResult, jobResult, nid);
                else {
                    // Ignore nodes that left topology.
                    if (!(unhandledEx instanceof ClusterGroupEmptyException))
                        taskResult.unhandledEx().put(nid, unhandledEx);
                }
            }
        }

        return taskResult;
    }

    protected void reduceJobResult(VisorNodeDataCollectorTaskResult taskResult,
        VisorNodeDataCollectorJobResult jobResult, UUID nid) {
        taskResult.gridNames().put(nid, jobResult.gridName());

        taskResult.topologyVersions().put(nid, jobResult.topologyVersion());

        taskResult.taskMonitoringEnabled().put(nid, jobResult.taskMonitoringEnabled());

        if (!jobResult.events().isEmpty())
            taskResult.events().addAll(jobResult.events());

        if (jobResult.eventsEx() != null)
            taskResult.eventsEx().put(nid, jobResult.eventsEx());

        if (!jobResult.caches().isEmpty())
            taskResult.caches().put(nid, jobResult.caches());

        if (jobResult.cachesEx() != null)
            taskResult.cachesEx().put(nid, jobResult.cachesEx());

        if (!jobResult.streamers().isEmpty())
            taskResult.streamers().put(nid, jobResult.streamers());

        if (jobResult.streamersEx() != null)
            taskResult.streamersEx().put(nid, jobResult.streamersEx());

        if (!jobResult.ggfss().isEmpty())
            taskResult.ggfss().put(nid, jobResult.ggfss());

        if (!jobResult.ggfsEndpoints().isEmpty())
            taskResult.ggfsEndpoints().put(nid, jobResult.ggfsEndpoints());

        if (jobResult.ggfssEx() != null)
            taskResult.ggfssEx().put(nid, jobResult.ggfssEx());
    }
}
