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
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;
import org.jetbrains.annotations.Nullable;

/**
 * Collects current Grid state mostly topology and metrics.
 */
@GridInternal
public class VisorNodeDataCollectorTask extends VisorMultiNodeTask<VisorNodeDataCollectorTaskArg,
    VisorNodeDataCollectorTaskResult, VisorNodeDataCollectorJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

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
                        taskRes.getUnhandledEx().put(nid, new VisorExceptionWrapper(unhandledEx));
                }
            }
        }

        taskRes.setActive(ignite.active());

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
        taskRes.getGridNames().put(nid, jobRes.getGridName());

        taskRes.getTopologyVersions().put(nid, jobRes.getTopologyVersion());

        taskRes.isTaskMonitoringEnabled().put(nid, jobRes.isTaskMonitoringEnabled());

        taskRes.getErrorCounts().put(nid, jobRes.getErrorCount());

        if (!jobRes.getEvents().isEmpty())
            taskRes.getEvents().addAll(jobRes.getEvents());

        if (jobRes.getEventsEx() != null)
            taskRes.getEventsEx().put(nid, new VisorExceptionWrapper(jobRes.getEventsEx()));

        if (!jobRes.getCaches().isEmpty())
            taskRes.getCaches().put(nid, jobRes.getCaches());

        if (jobRes.getCachesEx() != null)
            taskRes.getCachesEx().put(nid, new VisorExceptionWrapper(jobRes.getCachesEx()));

        if (!jobRes.getIgfss().isEmpty())
            taskRes.getIgfss().put(nid, jobRes.getIgfss());

        if (!jobRes.getIgfsEndpoints().isEmpty())
            taskRes.getIgfsEndpoints().put(nid, jobRes.getIgfsEndpoints());

        if (jobRes.getIgfssEx() != null)
            taskRes.getIgfssEx().put(nid, new VisorExceptionWrapper(jobRes.getIgfssEx()));

        taskRes.getReadyAffinityVersions().put(nid, jobRes.getReadyAffinityVersion());

        taskRes.getPendingExchanges().put(nid, jobRes.isHasPendingExchange());
    }
}
