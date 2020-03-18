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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Batch;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Recheck;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Repair;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairRequestTask;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Collections.EMPTY_SET;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.checkConflicts;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * The base point of partition reconciliation processing.
 */
public class PartitionReconciliationProcessor extends AbstractPipelineProcessor {
    /** Session change message. */
    public static final String SESSION_CHANGE_MSG = "Reconciliation session has changed.";

    /** Topology change message. */
    public static final String TOPOLOGY_CHANGE_MSG = "Topology has changed. Partition reconciliation task was stopped.";

    /** Work progress message. */
    public static final String WORK_PROGRESS_MSG = "Partition reconciliation task [sesId=%s, total=%s, remaining=%s]";

    /** Start execution message. */
    public static final String START_EXECUTION_MSG = "Partition reconciliation has started [repair=%s, repairAlg=%s, " +
        "fastCheck=%s, batchSize=%s, recheckAttempts=%s, parallelismLevel=%s, caches=%s]";

    /** Error reason. */
    public static final String ERROR_REASON = "Reason [msg=%s, exception=%s]";

    /** Work progress print interval. */
    private final long workProgressPrintInterval = getLong("WORK_PROGRESS_PRINT_INTERVAL", 1000 * 60 * 3);

    /** Recheck delay seconds. */
    private final int recheckDelay;

    /** Caches. */
    private final Collection<String> caches;

    /** Indicates that inconsistent key values should be repaired in accordance with {@link #repairAlg}. */
    private final boolean repair;

    /**
     * Represents a cache group mapping to set of partitions which should be validated.
     * If this field is {@code null} all partitions will be validated.
     */
    private final Map<Integer, Set<Integer>> partsToValidate;

    /** Amount of keys to retrieve within one job. */
    private final int batchSize;

    /** Amount of potentially inconsistent keys recheck attempts. */
    private final int recheckAttempts;

    /**
     * Specifies which fix algorithm to use: options {@code PartitionReconciliationRepairMeta.RepairAlg} while repairing
     * doubtful keys.
     */
    private final RepairAlgorithm repairAlg;

    /** Tracks workload chains based on its lifecycle. */
    private final WorkloadTracker workloadTracker = new WorkloadTracker();

    /** Results collector. */
    final ReconciliationResultCollector collector;

    /**
     * Creates a new instance of Partition reconciliation processor.
     *
     * @param sesId Session identifier that allows to identify different runs of the utility.
     * @param ignite Local Ignite instance to be used as an entry point for the execution of the utility.
     * @param caches Collection of cache names to be checked.
     * @param repair Flag indicates that inconsistencies should be repaired.
     * @param partsToValidate Optional collection of partition which shoulb be validated.
     *                        If value is {@code null} all partitions will be validated.
     * @param parallelismLevel Number of batches that can be handled simultaneously.
     * @param batchSize Amount of keys to retrieve within one job.
     * @param recheckAttempts Amount of potentially inconsistent keys recheck attempts.
     * @param repairAlg Repair algorithm to be used to fix inconsistency.
     * @param recheckDelay Specifies the time interval between two consequent attempts to check keys.
     * @param compact {@code true} if the result should be returned in compact form.
     * @param includeSensitive {@code true} if sensitive information should be included in the result.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionReconciliationProcessor(
        long sesId,
        IgniteEx ignite,
        Collection<String> caches,
        Map<Integer, Set<Integer>> partsToValidate,
        boolean repair,
        RepairAlgorithm repairAlg,
        int parallelismLevel,
        int batchSize,
        int recheckAttempts,
        int recheckDelay,
        boolean compact,
        boolean includeSensitive
    ) throws IgniteCheckedException {
        super(sesId, ignite, parallelismLevel);

        this.recheckDelay = recheckDelay;
        this.caches = caches;
        this.repair = repair;
        this.partsToValidate = partsToValidate;
        this.batchSize = batchSize;
        this.recheckAttempts = recheckAttempts;
        this.repairAlg = repairAlg;

        registerListener(workloadTracker.andThen(evtLsnr));

        collector = (compact)?
            new ReconciliationResultCollector.Compact(ignite, log, sesId, includeSensitive) :
            new ReconciliationResultCollector.Simple(ignite, log, includeSensitive);
    }

    /**
     * @return Partition reconciliation result
     */
    public ExecutionResult<ReconciliationAffectedEntries> execute() {
        if (log.isInfoEnabled()) {
            log.info(String.format(
                START_EXECUTION_MSG,
                repair,
                repairAlg,
                partsToValidate != null,
                batchSize,
                recheckAttempts,
                parallelismLevel,
                caches));
        }

        try {
            for (String cache : caches) {
                IgniteInternalCache<Object, Object> cachex = ignite.cachex(cache);

                ExpiryPolicy expPlc = cachex.context().expiry();
                if (expPlc != null && !(expPlc instanceof EternalExpiryPolicy)) {
                    log.warning("The cache '" + cache + "' was skipped because CacheConfiguration#setExpiryPolicyFactory is set.");

                    continue;
                }

                int[] partitions = partitions(cache);

                for (int partId : partitions) {
                    Batch workload = new Batch(sesId, UUID.randomUUID(), cache, partId, null);

                    workloadTracker.addTrackingChain(workload);

                    schedule(workload);
                }
            }

            boolean live = false;
            long lastUpdateTime = 0;

            while (!isEmpty() || (live = hasLiveHandlers())) {
                if (topologyChanged())
                    throw new IgniteException(TOPOLOGY_CHANGE_MSG);

                if (isSessionExpired())
                    throw new IgniteException(SESSION_CHANGE_MSG);

                if (isInterrupted())
                    throw new IgniteException(error.get());

                if (isEmpty() && live) {
                    U.sleep(100);

                    continue;
                }

                long currTimeMillis = System.currentTimeMillis();

                if (currTimeMillis >= lastUpdateTime + workProgressPrintInterval) {
                    if (log.isInfoEnabled()) {
                        log.info(String.format(
                            WORK_PROGRESS_MSG,
                            sesId,
                            workloadTracker.totalChains(),
                            workloadTracker.remaningChains()));
                    }

                    lastUpdateTime = currTimeMillis;
                }

                PipelineWorkload workload = takeTask();

                if (workload instanceof Batch)
                    handle((Batch)workload);
                else if (workload instanceof Recheck)
                    handle((Recheck)workload);
                else if (workload instanceof Repair)
                    handle((Repair)workload);
                else {
                    String err = "Unsupported workload type: " + workload;

                    log.error(err);

                    throw new IgniteException(err);
                }
            }

            return new ExecutionResult<>(collector.result());
        }
        catch (InterruptedException | IgniteException e) {
            String errMsg = "Partition reconciliation was interrupted.";

            waitWorkFinish();

            log.warning(errMsg, e);

            return new ExecutionResult<>(collector.result(), errMsg + ' ' + String.format(ERROR_REASON, e.getMessage(), e.getClass()));
        }
        catch (Exception e) {
            String errMsg = "Unexpected error.";

            log.error(errMsg, e);

            return new ExecutionResult<>(collector.result(), errMsg + ' ' + String.format(ERROR_REASON, e.getMessage(), e.getClass()));
        }
    }

    /**
     * @return Reconciliation result collector which can be used to store the result to a file.
     */
    public ReconciliationResultCollector collector() {
        return collector;
    }

    /**
     * Returns primary partitions that belong to the local node for the given cache name.
     *
     * @param name Cache name.
     * @return Primary partitions that belong to the local node.
     */
    private int[] partitions(String name) {
        int[] cacheParts = ignite.affinity(name).primaryPartitions(ignite.localNode());

        if (partsToValidate == null) {
            // All local primary partitions should be validated.
            return cacheParts;
        }

        Set<Integer> parts = partsToValidate.getOrDefault(ctx.cache().cacheDescriptor(name).groupId(), EMPTY_SET);

        return IntStream.of(cacheParts).filter(p -> parts.contains(p)).toArray();
    }

    /**
     * @param workload Workload.
     */
    private void handle(Batch workload) throws InterruptedException {
        compute(
            CollectPartitionKeysByBatchTask.class,
            new PartitionBatchRequest(workload.sessionId(), workload.workloadChainId(), workload.cacheName(), workload.partitionId(), batchSize, workload.lowerKey(), startTopVer),
            res -> {
                KeyCacheObject nextBatchKey = res.get1();

                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys = res.get2();

                assert nextBatchKey != null || recheckKeys.isEmpty();

                if (nextBatchKey != null)
                    schedule(new Batch(workload.sessionId(), workload.workloadChainId(), workload.cacheName(), workload.partitionId(), nextBatchKey));

                if (!recheckKeys.isEmpty()) {
                    schedule(
                        new Recheck(workload.sessionId(), workload.workloadChainId(), recheckKeys,
                            workload.cacheName(), workload.partitionId(), 0, 0),
                        recheckDelay,
                        TimeUnit.SECONDS
                    );
                }
            }
        );
    }

    /**
     * @param workload Workload.
     */
    private void handle(Recheck workload) throws InterruptedException {
        compute(
            CollectPartitionKeysByRecheckRequestTask.class,
            new RecheckRequest(workload.sessionId(), workload.workloadChainId(), new ArrayList<>(workload.recheckKeys().keySet()), workload.cacheName(),
                workload.partitionId(), startTopVer),
            actualKeys -> {
                Map<KeyCacheObject, Map<UUID, GridCacheVersion>> conflicts
                    = checkConflicts(workload.recheckKeys(), actualKeys,
                    ignite.cachex(workload.cacheName()).context(), startTopVer);

                if (!conflicts.isEmpty()) {
                    if (workload.recheckAttempt() < recheckAttempts) {
                        schedule(new Recheck(
                                workload.sessionId(),
                                workload.workloadChainId(),
                                conflicts,
                                workload.cacheName(),
                                workload.partitionId(),
                                workload.recheckAttempt() + 1,
                                workload.repairAttempt()
                            ),
                            recheckDelay,
                            TimeUnit.SECONDS
                        );
                    }
                    else if (repair) {
                        scheduleHighPriority(repair(workload.sessionId(), workload.workloadChainId(), workload.cacheName(), workload.partitionId(), conflicts,
                            actualKeys, workload.repairAttempt()));
                    }
                    else {
                        collector.appendConflictedEntries(
                            workload.cacheName(),
                            workload.partitionId(),
                            conflicts,
                            actualKeys);
                    }
                }
            });
    }

    /**
     * @param workload Workload.
     */
    private void handle(Repair workload) throws InterruptedException {
        compute(
            RepairRequestTask.class,
            new RepairRequest(workload.sessionId(), workload.workloadChainId(), workload.data(), workload.cacheName(), workload.partitionId(), startTopVer, repairAlg,
                workload.repairAttempt()),
            repairRes -> {
                if (!repairRes.repairedKeys().isEmpty())
                    collector.appendRepairedEntries(workload.cacheName(), workload.partitionId(), repairRes.repairedKeys());

                if (!repairRes.keysToRepair().isEmpty()) {
                    // Repack recheck keys.
                    Map<KeyCacheObject, Map<UUID, GridCacheVersion>> recheckKeys = new HashMap<>();

                    for (Map.Entry<VersionedKey, Map<UUID, VersionedValue>> dataEntry :
                        repairRes.keysToRepair().entrySet()) {
                        KeyCacheObject keyCacheObj;

                        try {
                            keyCacheObj = unmarshalKey(
                                dataEntry.getKey().key(),
                                ignite.cachex(workload.cacheName()).context());
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Unable to unmarshal key=[" + dataEntry.getKey().key() +
                                "], key is skipped.");

                            continue;
                        }

                        recheckKeys.put(keyCacheObj, dataEntry.getValue().entrySet().stream().
                            collect(Collectors.toMap(Map.Entry::getKey, e2 -> e2.getValue().version())));
                    }

                    if (workload.repairAttempt() < RepairRequestTask.MAX_REPAIR_ATTEMPTS) {
                        schedule(
                            new Recheck(
                                workload.sessionId(),
                                workload.workloadChainId(),
                                recheckKeys,
                                workload.cacheName(),
                                workload.partitionId(),
                                recheckAttempts,
                                workload.repairAttempt() + 1
                            ));
                    }
                }
            });
    }

    /**
     *
     */
    private Repair repair(
        long sesId,
        UUID workloadChainId,
        String cacheName,
        int partId,
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> notResolvingConflicts,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys,
        int repairAttempts
    ) {
        Map<KeyCacheObject, Map<UUID, VersionedValue>> res = new HashMap<>();

        for (KeyCacheObject key : notResolvingConflicts.keySet()) {
            Map<UUID, VersionedValue> versionedByNodes = actualKeys.get(key);
            if (versionedByNodes != null)
                res.put(key, versionedByNodes);
        }

        return new Repair(sesId, workloadChainId, cacheName, partId, res, repairAttempts);
    }

    /**
     * This class allows tracking workload chains based on its lifecycle.
     */
    private class WorkloadTracker implements ReconciliationEventListener {
        /** Map of trackable chains. */
        private final Map<UUID, ChainDescriptor> chanIds = new ConcurrentHashMap<>();

        /** Total number of tracked chains. */
        private final AtomicInteger trackedChainsCnt = new AtomicInteger();

        /** Number of completed chains. */
        private final AtomicInteger completedChainsCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void onEvent(WorkLoadStage stage, PipelineWorkload workload) {
            switch (stage) {
                case SCHEDULED:
                    attachWorkload(workload);

                    break;

                case FINISHED:
                    detachWorkload(workload);

                    break;
                default:
                    // There is no need to process other stages.
            }
        }

        /**
         * @return Total number of tracked chains.
         */
        public Integer totalChains() {
            return trackedChainsCnt.get();
        }

        /**
         * @return Number of chains that are not completed yet.
         */
        public Integer remaningChains() {
            return trackedChainsCnt.get() - completedChainsCnt.get();
        }

        /**
         * Adds the provided chain to a set of trackable chains.
         *
         * @param batch Chain to track.
         */
        public void addTrackingChain(Batch batch) {
            assert batch.sessionId() == sesId : "New tracking chain does not correspond to the current session " +
                "[currSesId=" + sesId + ", chainSesId=" + batch.sessionId() + ", chainId=" + batch.workloadChainId() + ']';

            chanIds.putIfAbsent(
                batch.workloadChainId(),
                new ChainDescriptor(batch.workloadChainId(), batch.cacheName(), batch.partitionId()));

            trackedChainsCnt.incrementAndGet();
        }

        /**
         * Incremets the number of workloads for the corresponding chain if it is trackable.
         *
         * @param workload Workload to add.
         */
        private void attachWorkload(PipelineWorkload workload) {
            // It should be guaranteed that the workload can be scheduled
            // strictly before its parent workload is finished.
            Optional.ofNullable(chanIds.get(workload.workloadChainId()))
                .map(d -> d.workloadCnt.incrementAndGet());
        }

        /**
         * Decrements the number of workloads for the corresponding chain if it is trackable
         *
         * If the given workload is the last one for corresponding chain
         * then {@link #onChainCompleted(UUID, String, int)} is triggered.
         *
         * @param workload Workload to detach.
         */
        private void detachWorkload(PipelineWorkload workload) {
            // It should be guaranteed that the workload can be finished
            // strictly after all subsequent workloads are scheduled.
            ChainDescriptor desc = chanIds.get(workload.workloadChainId());

            if (desc != null && desc.workloadCnt.decrementAndGet() == 0) {
                completedChainsCnt.incrementAndGet();

                chanIds.remove(desc.chainId);

                onChainCompleted(desc.chainId, desc.cacheName, desc.partId);
            }
        }

        /**
         * Callback that is triggered when the partition completelly processed.
         *
         * @param chainId Chain id.
         * @param cacheName Cache name.
         * @param partId Partition id.
         */
        private void onChainCompleted(UUID chainId, String cacheName, int partId) {
            collector.onPartitionProcessed(cacheName, partId);
        }

        /**
         * Workload chain descriptor.
         */
        private class ChainDescriptor {
            /** Chain identifier. */
            private final UUID chainId;

            /** Cache name. */
            private final String cacheName;

            /** Partition identifier. */
            private final int partId;

            /** Workload counter. */
            private final AtomicInteger workloadCnt = new AtomicInteger();

            /**
             * Creates a new instance of chain descriptor.
             *
             * @param chainId Chain id.
             * @param cacheName Cache name.
             * @param partId Partition id.
             */
            ChainDescriptor(UUID chainId, String cacheName, int partId) {
                this.chainId = chainId;
                this.cacheName = cacheName;
                this.partId = partId;
            }
        }
    }
}
