/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.node;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
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

        taskRes.setActive(ignite.cluster().active());

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

        taskRes.getTaskMonitoringEnabled().put(nid, jobRes.isTaskMonitoringEnabled());

        taskRes.getErrorCounts().put(nid, jobRes.getErrorCount());

        if (!F.isEmpty(jobRes.getEvents()))
            taskRes.getEvents().addAll(jobRes.getEvents());

        if (jobRes.getEventsEx() != null)
            taskRes.getEventsEx().put(nid, jobRes.getEventsEx());

        if (!F.isEmpty(jobRes.getMemoryMetrics()))
            taskRes.getMemoryMetrics().put(nid, jobRes.getMemoryMetrics());

        if (jobRes.getMemoryMetricsEx() != null)
            taskRes.getMemoryMetricsEx().put(nid, jobRes.getMemoryMetricsEx());

        if (!F.isEmpty(jobRes.getCaches()))
            taskRes.getCaches().put(nid, jobRes.getCaches());

        if (jobRes.getCachesEx() != null)
            taskRes.getCachesEx().put(nid, jobRes.getCachesEx());

        if (!F.isEmpty(jobRes.getIgfss()))
            taskRes.getIgfss().put(nid, jobRes.getIgfss());

        if (!F.isEmpty(jobRes.getIgfsEndpoints()))
            taskRes.getIgfsEndpoints().put(nid, jobRes.getIgfsEndpoints());

        if (jobRes.getIgfssEx() != null)
            taskRes.getIgfssEx().put(nid, jobRes.getIgfssEx());

        if (jobRes.getPersistenceMetrics() != null)
            taskRes.getPersistenceMetrics().put(nid, jobRes.getPersistenceMetrics());

        if (jobRes.getPersistenceMetricsEx() != null)
            taskRes.getPersistenceMetricsEx().put(nid, jobRes.getPersistenceMetricsEx());

        taskRes.getReadyAffinityVersions().put(nid, jobRes.getReadyAffinityVersion());

        taskRes.getPendingExchanges().put(nid, jobRes.isHasPendingExchange());

        taskRes.getRebalance().put(nid, jobRes.getRebalance());
    }
}
