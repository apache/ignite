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

package org.apache.ignite.internal.processors.cache.verify.checker.tasks;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntriesExtended;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.internal.processors.cache.checker.processor.PartitionReconciliationProcessor.ERROR_REASON;

/**
 * The main task which services {@link PartitionReconciliationProcessor} per nodes.
 */
@GridInternal
public class PartitionReconciliationProcessorTask extends ComputeTaskAdapter<VisorPartitionReconciliationTaskArg, ReconciliationResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** Ignite logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Flag indicates that the result of the utility should be logged to the console. */
    private boolean localOutoutMode;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        VisorPartitionReconciliationTaskArg arg
    ) throws IgniteException {
        localOutoutMode = arg.locOutput();

        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        LocalDateTime startTime = LocalDateTime.now();
        long sesId = System.currentTimeMillis() / 1000;

        int availableProcessors = Runtime.getRuntime().availableProcessors();

        if (arg.parallelism() == 0 || arg.parallelism() > availableProcessors) {
            U.warn(log, "Partition reconciliation [session=" + sesId + "] will be executed with " +
                "[parallelism=" + availableProcessors + "] according to number of CPU cores of the local node" );

            arg = new VisorPartitionReconciliationTaskArg.Builder(arg).parallelism(availableProcessors).build();
        }

        ignite.compute().broadcastAsync(new ReconciliationSessionId(sesId, arg.parallelism())).get();

        if (arg.fastCheck()) {
            assert ignite.context().discovery().discoCache().oldestAliveServerNode().id()
                .equals(ignite.context().localNodeId()) :
                "PartitionReconciliationProcessorTask must be executed on the coordinator node " +
                    "[locNodeId=" + ignite.context().localNodeId() +
                    ", crd=" + ignite.context().discovery().discoCache().oldestAliveServerNode().id() + ']';

            Map<Integer, Set<Integer>> invalidParts = Collections.emptyMap();

            GridDhtPartitionsExchangeFuture lastFut =
                (GridDhtPartitionsExchangeFuture)ignite.context().cache().context().exchange().lastFinishedFuture();
            if (lastFut == null) {
                if (log.isInfoEnabled()) {
                    log.info("PartitionReconciliationProcessorTask has nothing to check, " +
                        "the initial exchnage has not completed yet.");
                }
            }
            else
                invalidParts = lastFut.invalidPartitions();

            arg = new VisorPartitionReconciliationTaskArg.Builder(arg)
                .partitionsToRepair(invalidParts)
                .build();
        }

        for (ClusterNode node : subgrid)
            jobs.put(new PartitionReconciliationJob(arg, startTime, sesId), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public ReconciliationResult reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<UUID, String> nodeIdToFolder = new HashMap<>();

        ReconciliationAffectedEntries res = localOutoutMode ?
            new ReconciliationAffectedEntries() :
            new ReconciliationAffectedEntriesExtended();

        List<String> errors = new ArrayList<>();

        for (ComputeJobResult result : results) {
            UUID nodeId = result.getNode().id();
            IgniteException exc = result.getException();

            if (exc != null) {
                errors.add(nodeId + " - " + exc.getMessage());

                continue;
            }

            T2<String, ExecutionResult<ReconciliationAffectedEntries>> data = result.getData();

            nodeIdToFolder.put(nodeId, data.get1());
            res.merge(data.get2().result());

            if (data.get2().errorMessage() != null)
                errors.add(nodeId + " - " + data.get2().errorMessage());
        }

        return new ReconciliationResult(res, nodeIdToFolder, errors);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        IgniteException e = res.getException();

        if (e != null) {
            log.warning("PartitionReconciliationProcessorTask failed on node " +
                "[consistentId=" + res.getNode().consistentId() + ", e=" + e.getMessage() + "]", res.getException());
        }

        return ComputeJobResultPolicy.WAIT;
    }

    /**
     * Holder of execution {@link PartitionReconciliationProcessor}
     */
    private static class PartitionReconciliationJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private final VisorPartitionReconciliationTaskArg reconciliationTaskArg;

        /** */
        private final LocalDateTime startTime;

        /** */
        private long sesId;

        /**
         * Create a new instance of the job.
         *
         * @param arg Reconciliation parameters.
         * @param startTime Start time of the whole task.
         * @param sesId Session identifier.
         */
        public PartitionReconciliationJob(
            VisorPartitionReconciliationTaskArg arg,
            LocalDateTime startTime,
            long sesId
        ) {
            this.reconciliationTaskArg = arg;
            this.startTime = startTime;
            this.sesId = sesId;
        }

        /** {@inheritDoc} */
        @Override public T2<String, ExecutionResult<ReconciliationAffectedEntries>> execute() throws IgniteException {
            Set<String> caches = new HashSet<>();

            if (reconciliationTaskArg.caches() == null || reconciliationTaskArg.caches().isEmpty())
                caches.addAll(ignite.context().cache().publicCacheNames());
            else {
                for (String cacheRegexp : reconciliationTaskArg.caches()) {
                    List<String> acceptedCaches = new ArrayList<>();

                    for (String cacheName : ignite.context().cache().publicCacheNames()) {
                        if (cacheName.matches(cacheRegexp))
                            acceptedCaches.add(cacheName);
                    }

                    if (acceptedCaches.isEmpty())
                        return new T2<>(null, new ExecutionResult<>(new ReconciliationAffectedEntriesExtended(), "The cache '" + cacheRegexp + "' doesn't exist."));

                    caches.addAll(acceptedCaches);
                }
            }

            try {
                PartitionReconciliationProcessor proc = new PartitionReconciliationProcessor(
                    sesId,
                    ignite,
                    caches,
                    reconciliationTaskArg.partitionsToRepair(),
                    reconciliationTaskArg.repair(),
                    reconciliationTaskArg.repairAlg(),
                    reconciliationTaskArg.parallelism(),
                    reconciliationTaskArg.batchSize(),
                    reconciliationTaskArg.recheckAttempts(),
                    reconciliationTaskArg.recheckDelay(),
                    !reconciliationTaskArg.locOutput(),
                    reconciliationTaskArg.includeSensitive());

                ExecutionResult<ReconciliationAffectedEntries> reconciliationRes = proc.execute();

                File path = proc.collector().flushResultsToFile(startTime);

                return new T2<>((path != null)? path.getAbsolutePath() : null, reconciliationRes);
            }
            catch (Exception e) {
                String msg = "Reconciliation job failed on node [id=" + ignite.localNode().id() + "]. ";
                log.error(msg, e);

                throw new IgniteException(msg + String.format(ERROR_REASON, e.getMessage(), e.getClass()), e);
            }
        }
    }

    /**
     * Task that is used to set a partition reconciliation session identifier.
     */
    @GridInternal
    public static class ReconciliationSessionId implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Session id. */
        private final long sesId;

        /** Parallelism level. */
        private final int parallelism;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /**
         * Creates a new instance.
         *
         * @param sesId Partition reconsiliation session identifier.
         */
        public ReconciliationSessionId(long sesId, int parallelism) {
            this.sesId = sesId;
            this.parallelism = parallelism;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ignite.context().diagnostic().reconciliationExecutionContext().registerSession(sesId, parallelism);
        }
    }
}
