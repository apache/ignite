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

package org.apache.ignite.internal.management.kill;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheDistributedQueryFuture;
import org.apache.ignite.internal.processors.cache.query.GridCacheDistributedQueryManager;
import org.apache.ignite.internal.processors.cache.query.ScanQueryIterator;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.processors.query.running.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.resources.LoggerResource;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Task to cancel multiple SQL queries, scan queries, continuous queries based on specified criteria.
 */
@GridInternal
public class KillAllTask extends VisorMultiNodeTask<KillAllCommandArg, Map<ClusterNode, KillAllTaskResult>, KillAllTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<KillAllCommandArg, KillAllTaskResult> job(KillAllCommandArg arg) {
        return new KillAllJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Map<ClusterNode, KillAllTaskResult> reduce0(
        List<ComputeJobResult> results
    ) throws IgniteException {
        Map<ClusterNode, KillAllTaskResult> mapRes = new HashMap<>();

        for (ComputeJobResult result : results) {
            if (result.getException() != null)
                throw result.getException();

            KillAllTaskResult data = result.getData();

            if (data != null && (data.killed() > 0 || data.failed() > 0))
                mapRes.put(result.getNode(), data);
        }

        return mapRes;
    }

    /**
     * Job to cancel multiple targets on a node.
     */
    private static class KillAllJob extends VisorJob<KillAllCommandArg, KillAllTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param arg   Job argument.
         * @param debug Debug flag.
         */
        protected KillAllJob(KillAllCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected KillAllTaskResult run(KillAllCommandArg arg) throws IgniteException {
            switch (arg.target()) {
                case SQL:
                    return cancelSqlQueries(arg);

                case SCAN:
                    return cancelScanQueries(arg);

                case CONTINUOUS:
                    return cancelContinuousQueries(arg);

                default:
                    throw new IgniteException("Unknown target type: " + arg.target());
            }
        }

        /**
         * Cancel SQL queries matching criteria.
         *
         * @param arg Command argument.
         * @return Result.
         */
        private KillAllTaskResult cancelSqlQueries(KillAllCommandArg arg) {
            List<GridRunningQueryInfo> qrys = ignite.context().query().runningQueryManager().runningSqlQueries();

            qrys.removeIf(qry -> qry.mapQuery() || (arg.minDuration() != null
                && U.currentTimeMillis() - qry.startTime() <= SECONDS.toMillis(arg.minDuration())));

            for (GridRunningQueryInfo qry : qrys)
                ignite.context().query().runningQueryManager().cancelLocalQuery(qry.id());

            return new KillAllTaskResult(qrys.size(), 0);
        }

        /**
         * Cancel scan queries matching criteria.
         *
         * @param arg Command argument.
         * @return Result.
         */
        private KillAllTaskResult cancelScanQueries(KillAllCommandArg arg) {
            long ts = arg.minDuration() == null ? 0 : U.currentTimeMillis() - SECONDS.toMillis(arg.minDuration());
            int killed = 0;
            int failed = 0;

            for (GridCacheContext<?, ?> cctx : ignite.context().cache().context().cacheContexts()) {
                // Scan queries can be registered in multiple structures. There is no single registry for all scan
                // queries. To properly cancel a scan query, all relevant structures must be analyzed:
                // - cctx.queries().localQueryIterators() - iterators for local-only scans and local parts of
                //   distributed scans (on initiator node). If initiator is not affinity node, there will be no
                //   local iterator for distributed scan.
                // - cctx.queries().distributedQueryFutures() - futures for distributed scans (on initiator node).
                //   For local-only data (REPLICATED cache or local scans), there will be no distributed future.
                // - cctx.queries().queryIterators() - remote iterators for distributed scans (on affinity nodes).
                //   Keyed by originator node ID, each entry contains map of requests to iterators.
                // Correct way to kill scan (see also GridCacheDistributedQueryManager.scanQueryDistributed ->
                // new GridCloseableIteratorAdapter.onClose)
                // - Close local iterator (removes iterator from localQueryIterators())
                // - Cancel distributed future (removes future from distrivuted future list, completes future,
                //   sends cancel to remote nodes, removes iterator from queryIterrators() on remote nodes)
                GridCacheDistributedQueryManager<?, ?> mgr = (GridCacheDistributedQueryManager<?, ?>)cctx.queries();

                // Kill local-only scans and local part of distributed scans.
                for (ScanQueryIterator<?, ?, ?> locIter : mgr.localQueryIterators()) {
                    if (ts > 0 && locIter.startTime() >= ts)
                        continue;

                    try {
                        locIter.close();

                        killed++;
                    }
                    catch (IgniteCheckedException e) {
                        log.warning("Failed to close local iterator for scan query", e);

                        failed++;
                    }
                }

                // Kill remote part of distributed scans.
                for (GridCacheDistributedQueryFuture<?, ?, ?> fut : mgr.distributedQueryFutures()) {
                    if (ts > 0 && fut.startTime() >= ts)
                        continue;

                    try {
                        fut.cancel();

                        if (!cctx.affinityNode()) // For affinity nodes killed count is already incremented by locIter.
                            killed++;
                    }
                    catch (IgniteCheckedException e) {
                        log.warning("Failed to cancel distributed query future for scan query", e);

                        failed++;
                    }
                }
            }

            return new KillAllTaskResult(killed, failed);
        }

        /**
         * Cancel continuous queries matching criteria.
         *
         * @param arg Command argument.
         * @return Result.
         */
        private KillAllTaskResult cancelContinuousQueries(KillAllCommandArg arg) {
            GridContinuousProcessor proc = ignite.context().continuous();

            List<IgniteInternalFuture<?>> futs = new ArrayList<>();

            for (Map.Entry<UUID, GridContinuousProcessor.LocalRoutineInfo> e : proc.localRoutineInfos().entrySet()) {
                if (arg.nodeId == null || arg.nodeId.equals(e.getValue().nodeId()))
                    futs.add(proc.stopRoutine(e.getKey()));
            }

            int killed = 0;
            int failed = 0;

            for (IgniteInternalFuture<?> fut : futs) {
                try {
                    fut.get();

                    killed++;
                }
                catch (IgniteCheckedException e) {
                    log.warning("Failed to stop continuous query routine", e);

                    failed++;
                }
            }

            return new KillAllTaskResult(killed, failed);
        }
    }
}
