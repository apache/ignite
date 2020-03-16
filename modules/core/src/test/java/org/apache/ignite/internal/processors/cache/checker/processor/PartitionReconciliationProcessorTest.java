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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import junit.framework.TestCase;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.CachePartitionRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.RepairResult;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Batch;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Recheck;
import org.apache.ignite.internal.processors.cache.checker.processor.workload.Repair;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByBatchTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.CollectPartitionKeysByRecheckRequestTask;
import org.apache.ignite.internal.processors.cache.checker.tasks.RepairRequestTask;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor;
import org.apache.ignite.internal.processors.diagnostic.ReconciliationExecutionContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.ConsoleTestLogger;
import org.apache.ignite.testframework.GridTestNode;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Isolated test for {@link PartitionReconciliationProcessor}.
 */
public class PartitionReconciliationProcessorTest extends TestCase {
    /** Default cache. */
    private static final String DEFAULT_CACHE = "default-cache";

    /** Partition id. */
    private static final int PARTITION_ID = 123;

    /** */
    private static final long SESSION_ID = 123;

    /** */
    private static final int MAX_RECHECK_ATTEMPTS = 3;

    /**
     * Tests that empty result of batch doesn't schedule a work.
     */
    @Test
    public void testBatchDoesNotHaveElementsNothingSchedule() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(false);

        ExecutionResult<T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>>> emptyRes = new ExecutionResult<>(new T2<>(null, new HashMap<>()));

        processor.addTask(new Batch(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
            DEFAULT_CACHE, PARTITION_ID, null))
            .whereResult(CollectPartitionKeysByBatchTask.class, emptyRes)
            .execute();

        processor.verify(never()).schedule(any());
        processor.verify(never()).schedule(any(), anyInt(), any());
        processor.verify(never()).scheduleHighPriority(any());
    }

    /**
     * Tests that if batch has elements, next batch should scheduled.
     */
    @Test
    public void testBatchHasElementsRecheckAndNextBatchShouldSchedule() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(false);

        KeyCacheObject nextKey = new KeyCacheObjectImpl(1, null, PARTITION_ID);
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        batchRes.put(nextKey, new HashMap<>());
        ExecutionResult<T2<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>>> emptyRes = new ExecutionResult<>(new T2<>(nextKey, batchRes));

        processor.addTask(new Batch(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
            DEFAULT_CACHE, PARTITION_ID, null))
            .whereResult(CollectPartitionKeysByBatchTask.class, emptyRes)
            .execute();

        processor.verify(times(1)).schedule(any(Batch.class));
        processor.verify(times(1)).schedule(any(Recheck.class), eq(10L), eq(SECONDS));
    }

    /**
     * Tests that recheck stops if result is empty.
     */
    @Test
    public void testRecheckShouldFinishWithoutActionIfResultEmpty() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(false);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        batchRes.put(new KeyCacheObjectImpl(1, null, PARTITION_ID), new HashMap<>());

        ExecutionResult<Map<KeyCacheObject, Map<UUID, VersionedValue>>> emptyRes = new ExecutionResult<>(new HashMap<>());

        processor.addTask(new Recheck(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
            batchRes, DEFAULT_CACHE, PARTITION_ID, 0, 0))
            .whereResult(CollectPartitionKeysByRecheckRequestTask.class, emptyRes)
            .execute();

        processor.verify(never()).schedule(any());
        processor.verify(never()).schedule(any(), anyInt(), any());
        processor.verify(never()).scheduleHighPriority(any());
    }

    /**
     * Tests that recheck stops if all conflicts resolved.
     */
    @Test
    public void testRecheckShouldFinishWithoutActionIfConflictWasSolved() throws IgniteCheckedException {
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();

        MockedProcessor processor = MockedProcessor.create(false, Arrays.asList(nodeId1, nodeId2));

        KeyCacheObjectImpl key = new KeyCacheObjectImpl(1, null, PARTITION_ID);
        GridCacheVersion ver = new GridCacheVersion(1, 0, 0, 0);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeys = new HashMap<>();
        oldKeys.put(nodeId1, ver);
        oldKeys.put(nodeId2, ver);
        batchRes.put(key, oldKeys);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> sameRes = new HashMap<>();
        Map<UUID, VersionedValue> actualKey = new HashMap<>();
        actualKey.put(nodeId1, new VersionedValue(null, ver, 1, 1));
        actualKey.put(nodeId2, new VersionedValue(null, ver, 1, 1));
        sameRes.put(key, actualKey);

        processor.addTask(new Recheck(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
            batchRes, DEFAULT_CACHE, PARTITION_ID, 0, 0))
            .whereResult(CollectPartitionKeysByRecheckRequestTask.class, new ExecutionResult<>(sameRes))
            .execute();

        processor.verify(never()).schedule(any());
        processor.verify(never()).schedule(any(), anyInt(), any());
        processor.verify(never()).scheduleHighPriority(any());
    }

    /**
     * Tests that recheck schedule new recheck.
     */
    @Test
    public void testRecheckShouldTryAgainIfConflictAndAttemptsExist() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(false);

        KeyCacheObjectImpl key = new KeyCacheObjectImpl(1, null, PARTITION_ID);
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();
        GridCacheVersion ver = new GridCacheVersion(1, 0, 0, 0);
        GridCacheVersion ver2 = new GridCacheVersion(2, 0, 0, 0);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeys = new HashMap<>();
        oldKeys.put(nodeId1, ver);
        oldKeys.put(nodeId2, ver2);
        batchRes.put(key, oldKeys);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> sameRes = new HashMap<>();
        Map<UUID, VersionedValue> actualKey = new HashMap<>();
        actualKey.put(nodeId1, new VersionedValue(null, ver, 1, 1));
        actualKey.put(nodeId2, new VersionedValue(null, ver2, 1, 1));
        sameRes.put(key, actualKey);

        processor.addTask(new Recheck(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
            batchRes, DEFAULT_CACHE, PARTITION_ID, 0, 0))
            .whereResult(CollectPartitionKeysByRecheckRequestTask.class, new ExecutionResult<>(sameRes))
            .execute();

        processor.verify(times(1)).schedule(any(Recheck.class), eq(10L), eq(SECONDS));
    }

    /**
     * Tests that repair should try to repair if recheck attempts are finished.
     */
    @Test
    public void testRecheckShouldTryRepairIfAttemptsDoesNotExist() throws IgniteCheckedException {
        MockedProcessor processor = MockedProcessor.create(true);

        KeyCacheObjectImpl key = new KeyCacheObjectImpl(1, null, PARTITION_ID);
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();
        GridCacheVersion ver = new GridCacheVersion(1, 0, 0, 0);
        GridCacheVersion ver2 = new GridCacheVersion(2, 0, 0, 0);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeys = new HashMap<>();
        oldKeys.put(nodeId1, ver);
        oldKeys.put(nodeId2, ver2);
        batchRes.put(key, oldKeys);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> sameRes = new HashMap<>();
        Map<UUID, VersionedValue> actualKey = new HashMap<>();
        actualKey.put(nodeId1, new VersionedValue(null, ver, 1, 1));
        actualKey.put(nodeId2, new VersionedValue(null, ver2, 1, 1));
        sameRes.put(key, actualKey);

        processor.addTask(new Recheck(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
            batchRes, DEFAULT_CACHE, PARTITION_ID, MAX_RECHECK_ATTEMPTS, RepairRequestTask.MAX_REPAIR_ATTEMPTS))
            .whereResult(CollectPartitionKeysByRecheckRequestTask.class, new ExecutionResult<>(sameRes))
            .execute();

        processor.verify(times(1)).scheduleHighPriority(any(Repair.class));
    }

    /**
     * Check if jobs that are trying to fix inconsistency have higher priority (will be executed faster) then jobs that
     * are checking caches. This test schedules {@link Recheck} task with default priority and {@link Repair} with high.
     * High priority task should process first.
     */
    @Test
    public void testThatRepairHaveHigherPriorityThenChecking() throws IgniteCheckedException {
        List<String> evtHist = new ArrayList<>();
        MockedProcessor processor = MockedProcessor.create(true);
        processor.useRealScheduler = true;
        processor.registerListener((stage, workload) -> {
            if (stage.equals(ReconciliationEventListener.WorkLoadStage.RESULT_READY))
                evtHist.add(workload.getClass().getName());
        });

        KeyCacheObjectImpl key = new KeyCacheObjectImpl(1, null, PARTITION_ID);
        UUID nodeId1 = UUID.randomUUID();
        UUID nodeId2 = UUID.randomUUID();
        GridCacheVersion ver = new GridCacheVersion(1, 0, 0, 0);
        GridCacheVersion ver2 = new GridCacheVersion(2, 0, 0, 0);

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> batchRes = new HashMap<>();
        Map<UUID, GridCacheVersion> oldKeys = new HashMap<>();
        oldKeys.put(nodeId1, ver);
        oldKeys.put(nodeId2, ver2);
        batchRes.put(key, oldKeys);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> sameRes = new HashMap<>();
        Map<UUID, VersionedValue> actualKey = new HashMap<>();
        actualKey.put(nodeId1, new VersionedValue(null, ver, 1, 1));
        actualKey.put(nodeId2, new VersionedValue(null, ver2, 1, 1));
        sameRes.put(key, actualKey);

        processor
            .addTask(new Recheck(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
                batchRes, DEFAULT_CACHE, PARTITION_ID, MAX_RECHECK_ATTEMPTS, RepairRequestTask.MAX_REPAIR_ATTEMPTS))
            .addTask(new Recheck(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
                batchRes, DEFAULT_CACHE, PARTITION_ID, MAX_RECHECK_ATTEMPTS, RepairRequestTask.MAX_REPAIR_ATTEMPTS))
            .whereResult(CollectPartitionKeysByRecheckRequestTask.class, new ExecutionResult<>(sameRes))
            .whereResult(RepairRequestTask.class, new ExecutionResult<>(new RepairResult()))
            .execute();

        assertEquals(RecheckRequest.class.getName(), evtHist.get(0));
        assertEquals(RepairRequest.class.getName(), evtHist.get(1));
        assertEquals(RecheckRequest.class.getName(), evtHist.get(2));
    }

    /**
     * Mocked partition reconciliation processor for testing purposes.
     */
    private static class MockedProcessor extends PartitionReconciliationProcessor {
        /** */
        private final AbstractPipelineProcessor mock = mock(AbstractPipelineProcessor.class);

        /** */
        private final ConcurrentMap<Class, Object> computeResults = new ConcurrentHashMap<>();

        /** */
        public volatile boolean useRealScheduler = false;

        /**
         *
         */
        public static MockedProcessor create(boolean fixMode) throws IgniteCheckedException {
            return create(fixMode, Collections.emptyList());
        }

        /**
         *
         */
        public static MockedProcessor create(boolean fixMode, int parallelismLevel) throws IgniteCheckedException {
            return create(fixMode, Collections.emptyList(), parallelismLevel);
        }

        /**
         *
         */
        public static MockedProcessor create(boolean fixMode, List<UUID> nodeIds) throws IgniteCheckedException {
            return create(fixMode, nodeIds, 0);
        }

        /**
         *
         */
        public static MockedProcessor create(boolean fixMode, List<UUID> nodeIds,
            int parallelismLevel) throws IgniteCheckedException {
            List<ClusterNode> nodes = new ArrayList<>();

            for (UUID nodeId : nodeIds)
                nodes.add(new GridTestNode(nodeId));

            GridCachePartitionExchangeManager exchMgr = mock(GridCachePartitionExchangeManager.class);
            GridDhtPartitionsExchangeFuture fut = mock(GridDhtPartitionsExchangeFuture.class);

            when(fut.get()).thenReturn(new AffinityTopologyVersion());
            when(exchMgr.lastTopologyFuture()).thenReturn(fut);
            when(exchMgr.lastAffinityChangedTopologyVersion(any())).thenReturn(new AffinityTopologyVersion());

            IgniteEx igniteMock = mock(IgniteEx.class);

            when(igniteMock.log()).thenReturn(new ConsoleTestLogger(PartitionReconciliationProcessor.class.getName()));

            GridKernalContext ctxMock = mock(GridKernalContext.class);

            ReconciliationExecutionContext reconciliationCtxMock = mock(ReconciliationExecutionContext.class);
            DiagnosticProcessor diagnosticProcessorMock = mock(DiagnosticProcessor.class);
            GridCacheProcessor cacheProcessorMock = mock(GridCacheProcessor.class);
            GridCacheSharedContext cacheSharedCtxMock = mock(GridCacheSharedContext.class);
            when(diagnosticProcessorMock.reconciliationExecutionContext()).thenReturn(reconciliationCtxMock);
            when(reconciliationCtxMock.sessionId()).thenReturn(SESSION_ID);
            when(ctxMock.diagnostic()).thenReturn(diagnosticProcessorMock);
            when(ctxMock.cache()).thenReturn(cacheProcessorMock);
            when(cacheProcessorMock.context()).thenReturn(cacheSharedCtxMock);
            when(cacheSharedCtxMock.exchange()).thenReturn(exchMgr);

            when(igniteMock.context()).thenReturn(ctxMock);

            IgniteInternalCache cacheMock = mock(IgniteInternalCache.class);
            when(igniteMock.cachex(anyString())).thenReturn(cacheMock);

            GridCacheContext cacheCtxMock = mock(GridCacheContext.class);
            when(cacheMock.context()).thenReturn(cacheCtxMock);

            GridCacheAffinityManager affinityManager = mock(GridCacheAffinityManager.class);
            when(cacheCtxMock.affinity()).thenReturn(affinityManager);
            when(affinityManager.partition(any())).thenReturn(4);

            GridDhtPartitionTopology topMock = mock(GridDhtPartitionTopology.class);
            when(cacheCtxMock.topology()).thenReturn(topMock);

            when(topMock.owners(anyInt(), any())).thenReturn(nodes);

            IgniteClusterEx clusterMock = mock(IgniteClusterEx.class);
            when(clusterMock.nodes()).thenReturn(nodes);
            when(igniteMock.cluster()).thenReturn(clusterMock);

            IgniteCompute igniteComputeMock = mock(IgniteCompute.class);
            when(igniteComputeMock.executeAsync(any(Class.class), any())).thenReturn(mock(ComputeTaskFuture.class));
            when(igniteMock.compute(any())).thenReturn(igniteComputeMock);

            return new MockedProcessor(igniteMock, Collections.emptyList(), fixMode, parallelismLevel,
                10, MAX_RECHECK_ATTEMPTS, 10);
        }

        /**
         *
         */
        public MockedProcessor(
            IgniteEx ignite,
            Collection<String> caches,
            boolean fixMode,
            int parallelismLevel,
            int batchSize,
            int recheckAttempts,
            int recheckDelay
        ) throws IgniteCheckedException {
            super(SESSION_ID,
                ignite,
                caches,
                null,
                fixMode,
                RepairAlgorithm.MAJORITY,
                parallelismLevel,
                batchSize,
                recheckAttempts,
                recheckDelay);
        }

        /** {@inheritDoc} */
        @Override
        protected <T extends CachePartitionRequest, R> void compute(
            Class<? extends ComputeTask<T, ExecutionResult<R>>> taskCls, T arg,
            IgniteInClosure<? super R> lsnr) throws InterruptedException {

            if (this.parallelismLevel == 0) {
                ExecutionResult<R> res = (ExecutionResult<R>)computeResults.get(taskCls);

                if (res == null)
                    throw new IllegalStateException("Please add result for: " + taskCls.getSimpleName());

                evtLsnr.onEvent(ReconciliationEventListener.WorkLoadStage.RESULT_READY, arg);

                lsnr.apply(res.result());

                evtLsnr.onEvent(ReconciliationEventListener.WorkLoadStage.FINISHING, arg);
            }
            else
                super.compute(taskCls, arg, lsnr);
        }

        /** {@inheritDoc} */
        @Override protected void scheduleHighPriority(PipelineWorkload task) {
            mock.scheduleHighPriority(task);

            if (useRealScheduler)
                super.scheduleHighPriority(task);
        }

        /** {@inheritDoc} */
        @Override protected void schedule(PipelineWorkload task) {
            mock.schedule(task);
        }

        /** {@inheritDoc} */
        @Override protected void schedule(PipelineWorkload task, long duration, TimeUnit timeUnit) {
            mock.schedule(task, duration, timeUnit);

            if (useRealScheduler)
                super.schedule(task, duration, timeUnit);
        }

        /**
         *
         */
        public MockedProcessor addTask(PipelineWorkload workload) {
            super.schedule(workload, 0, MILLISECONDS);

            return this;
        }

        /**
         *
         */
        public <T extends CachePartitionRequest, R> MockedProcessor whereResult(
            Class<? extends ComputeTask<T, R>> taskCls, R res) {
            computeResults.put(taskCls, res);

            return this;
        }

        /**
         *
         */
        public AbstractPipelineProcessor verify(VerificationMode times) {
            return Mockito.verify(mock, times);
        }
    }
}
