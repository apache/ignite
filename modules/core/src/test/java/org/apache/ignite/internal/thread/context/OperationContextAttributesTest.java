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

package org.apache.ignite.internal.thread.context;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.thread.context.concurrent.IgniteCompletableFuture;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareWrapper;
import org.apache.ignite.internal.thread.pool.IgniteForkJoinPool;
import org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor;
import org.apache.ignite.internal.thread.pool.IgniteStripedExecutor;
import org.apache.ignite.internal.thread.pool.IgniteStripedThreadPoolExecutor;
import org.apache.ignite.internal.thread.pool.IgniteThreadPoolExecutor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.queue.IgniteAsyncObjectHandler;
import org.apache.ignite.internal.util.worker.queue.IgniteDelayedObjectHandler;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.messages.InetSocketAddressMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.junit.Test;
import org.springframework.lang.NonNull;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class OperationContextAttributesTest extends GridCommonAbstractTest {
    /** */
    private static final String DFLT_STR_VAL = "default";

    /** */
    private static final int DFLT_INT_VAL = -1;

    /** */
    private static final OperationContextAttribute<String> STR_ATTR = OperationContextAttribute.newInstance(DFLT_STR_VAL);

    /** */
    private static final OperationContextAttribute<Integer> INT_ATTR = OperationContextAttribute.newInstance(DFLT_INT_VAL);

    /** */
    private ExecutorService poolToShutdownAfterTest;

    /** */
    private int beforeTestReservedAttrIds;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        AttributeValueChecker.CHECKS.clear();

        beforeTestReservedAttrIds = OperationContextAttribute.ID_GEN.get();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        if (poolToShutdownAfterTest != null)
            poolToShutdownAfterTest.shutdownNow();

        // Releases attribute IDs reserved during the test.
        OperationContextAttribute.ID_GEN.set(beforeTestReservedAttrIds);
    }

    /** */
    @Test
    public void testNotAttachedAttribute() {
        // No opened scope.
        assertEquals(DFLT_STR_VAL, OperationContext.get(STR_ATTR));

        // Scope opened but testing attribute is not set.
        try (Scope ignored = OperationContext.set(INT_ATTR, 0)) {
            assertEquals(DFLT_STR_VAL, OperationContext.get(STR_ATTR));
        }
    }

    /** */
    @Test
    public void testAttachedAttribute() {
        try (Scope ignored = OperationContext.set(STR_ATTR, "test")) {
            assertEquals("test", OperationContext.get(STR_ATTR));
        }
    }

    /** */
    @Test
    public void testAttributeValueSearchUpScopeStack() {
        try (Scope ignored1 = OperationContext.set(STR_ATTR, "test1")) {
            try (Scope ignored2 = OperationContext.set(INT_ATTR, 2)) {
                checkAttributeValues("test1", 2);
            }
        }
    }

    /** */
    @Test
    public void testAttributeValueOverwrite() {
        try (Scope ignored = OperationContext.set(STR_ATTR, "test1", INT_ATTR, 1, STR_ATTR, "test2")) {
            checkAttributeValues("test2", 1);
        }
    }

    /** */
    @Test
    public void testConsequentScopes() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored1 = OperationContext.set(STR_ATTR, "test1", INT_ATTR, 1)) {
            checkAttributeValues("test1", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored2 = OperationContext.set(INT_ATTR, 2)) {
            checkAttributeValues(DFLT_STR_VAL, 2);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopes() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored1 = OperationContext.set(INT_ATTR, 1)) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            try (Scope ignored2 = OperationContext.set(STR_ATTR, "test2")) {
                checkAttributeValues("test2", 1);
            }

            checkAttributeValues(DFLT_STR_VAL, 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopesAttributeValueOverwriteAndInheritance() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored1 = OperationContext.set(INT_ATTR, 1, STR_ATTR, "test1")) {
            checkAttributeValues("test1", 1);

            try (Scope ignored2 = OperationContext.set(STR_ATTR, "test2")) {
                checkAttributeValues("test2", 1);
            }

            checkAttributeValues("test1", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNullAttributeValue() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored1 = OperationContext.set(INT_ATTR, null, STR_ATTR, null)) {
            checkAttributeValues(null, null);

            try (Scope ignored2 = OperationContext.set(STR_ATTR, "test2")) {
                checkAttributeValues("test2", null);

                try (Scope ignored3 = OperationContext.set(STR_ATTR, null)) {
                    checkAttributeValues(null, null);
                }

                checkAttributeValues("test2", null);
            }

            checkAttributeValues(null, null);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testScopeWithInitialAttributeValue() {
        try (Scope scope1 = OperationContext.set(INT_ATTR, DFLT_INT_VAL, STR_ATTR, DFLT_STR_VAL)) {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            assertTrue(scope1 == Scope.NOOP_SCOPE);

            try (Scope scope2 = OperationContext.set(INT_ATTR, DFLT_INT_VAL)) {
                checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

                assertTrue(scope2 == Scope.NOOP_SCOPE);
            }

            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
        }
    }

    /** */
    @Test
    public void testNestedScopeWithTheSameAttributeValue() {
        try (Scope ignored1 = OperationContext.set(INT_ATTR, 1)) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            try (Scope scope = OperationContext.set(INT_ATTR, 1)) {
                checkAttributeValues(DFLT_STR_VAL, 1);

                assertTrue(scope == Scope.NOOP_SCOPE);
            }

            checkAttributeValues(DFLT_STR_VAL, 1);
        }
    }

    /** */
    @Test
    public void testRuntimeAttributeCreation() {
        try (Scope ignored1 = OperationContext.set(INT_ATTR, 1)) {
            OperationContextAttribute<Object> attr = OperationContextAttribute.newInstance();

            assertNull(OperationContext.get(attr));

            try (Scope ignored2 = OperationContext.set(attr, "test")) {
                assertEquals("test", OperationContext.get(attr));
            }

            assertNull(OperationContext.get(attr));
        }
    }

    /** */
    @Test
    public void testMaximumAttributesInstanceCount() {
        int cnt = OperationContextAttribute.MAX_ATTR_CNT - OperationContextAttribute.ID_GEN.get();

        List<OperationContextAttribute<Integer>> attrs = new ArrayList<>(cnt);
        LinkedList<Scope> scopes = new LinkedList<>();

        for (int i = 0; i < cnt; i++) {
            attrs.add(OperationContextAttribute.newInstance());

            scopes.push(OperationContext.set(attrs.get(i), i));
        }

        try {
            for (int i = 0; i < cnt; i++)
                assertTrue(i == OperationContext.get(attrs.get(i)));
        }
        finally {
            scopes.forEach(Scope::close);
        }

        assertTrue(attrs.stream().allMatch(attr -> OperationContext.get(attr) == null));

        assertThrowsAnyCause(
            log,
            OperationContextAttribute::newInstance,
            AssertionError.class,
            "Exceeded maximum supported number of created Attributes instances"
        );
    }

    /** */
    @Test
    public void testUnorderedScopeClosing() {
        Scope scope1 = OperationContext.set(INT_ATTR, 0);

        try {
            try (Scope ignored = OperationContext.set(STR_ATTR, "test")) {
                assertThrowsWithCause(scope1::close, AssertionError.class);
            }
        }
        finally {
            scope1.close();
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        assertThrowsWithCause(scope1::close, AssertionError.class);
    }

    /** */
    @Test
    public void testEmptySnapshot() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        OperationContextSnapshot snapshot = OperationContext.createSnapshot();

        try (Scope ignored = OperationContext.restoreSnapshot(snapshot)) {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testSnapshot() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        OperationContextSnapshot snapshot;

        try (Scope ignored = OperationContext.set(INT_ATTR, 1, STR_ATTR, "test1")) {
            checkAttributeValues("test1", 1);

            snapshot = OperationContext.createSnapshot();
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored = OperationContext.restoreSnapshot(snapshot)) {
            checkAttributeValues("test1", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopeSnapshot() {
        OperationContextSnapshot snapshot;

        try (Scope ignored1 = OperationContext.set(INT_ATTR, 1, STR_ATTR, "test1")) {
            try (Scope ignored2 = OperationContext.set(STR_ATTR, "test2")) {
                checkAttributeValues("test2", 1);

                snapshot = OperationContext.createSnapshot();
            }
        }

        try (Scope ignored = OperationContext.restoreSnapshot(snapshot)) {
            checkAttributeValues("test2", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopeInSnapshotScope() {
        OperationContextSnapshot snapshot0;

        try (Scope ignored = OperationContext.set(INT_ATTR, 1, STR_ATTR, "test1")) {
            checkAttributeValues("test1", 1);

            snapshot0 = OperationContext.createSnapshot();
        }

        OperationContextSnapshot snapshot1;

        try (Scope ignored1 = OperationContext.restoreSnapshot(snapshot0)) {
            checkAttributeValues("test1", 1);

            try (Scope ignored2 = OperationContext.set(INT_ATTR, 2)) {
                checkAttributeValues("test1", 2);

                snapshot1 = OperationContext.createSnapshot();
            }

            checkAttributeValues("test1", 1);
        }

        try (Scope ignored0 = OperationContext.restoreSnapshot(snapshot1)) {
            checkAttributeValues("test1", 2);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testSnapshotRestoreInExistingScope() {
        OperationContextSnapshot snapshot;

        try (Scope ignored = OperationContext.set(STR_ATTR, "test1")) {
            checkAttributeValues("test1", DFLT_INT_VAL);

            snapshot = OperationContext.createSnapshot();
        }

        try (Scope ignored1 = OperationContext.set(INT_ATTR, 1)) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            // Note, snapshot restores the state of the entire context, including attributes that do not have a value set.
            try (Scope ignored2 = OperationContext.restoreSnapshot(snapshot)) {
                checkAttributeValues("test1", DFLT_INT_VAL);
            }

            checkAttributeValues(DFLT_STR_VAL, 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testSnapshotNotAffectedByConsequentContextUpdates() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        OperationContextSnapshot snapshot;

        try (Scope ignored0 = OperationContext.set(INT_ATTR, 1)) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            snapshot = OperationContext.createSnapshot();

            try (Scope ignored1 = OperationContext.set(STR_ATTR, "test")) {
                checkAttributeValues("test", 1);

                try (Scope ignored = OperationContext.restoreSnapshot(snapshot)) {
                    checkAttributeValues(DFLT_STR_VAL, 1);
                }

                checkAttributeValues("test", 1);
            }
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testSnapshotScopeUnorderedClosing() {
        OperationContextSnapshot snapshot;

        try (Scope ignored = OperationContext.set(STR_ATTR, "test1")) {
            checkAttributeValues("test1", DFLT_INT_VAL);

            snapshot = OperationContext.createSnapshot();
        }

        try (Scope snpScope = OperationContext.restoreSnapshot(snapshot)) {
            try (Scope ignored1 = OperationContext.set(INT_ATTR, 2)) {
                checkAttributeValues("test1", 2);

                assertThrowsWithCause(snpScope::close, AssertionError.class);
            }
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testContextAwareThreadPool() throws Exception {
        IgniteThreadPoolExecutor pool = deferShutdown(new IgniteThreadPoolExecutor(
            "test",
            null,
            1,
            1,
            Long.MAX_VALUE,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            null));

        doContextAwareExecutorServiceTest(pool);
    }

    /** */
    @Test
    public void testContextAwareStripedThreadPoolExecutor() throws Exception {
        IgniteStripedThreadPoolExecutor pool = deferShutdown(new IgniteStripedThreadPoolExecutor(
            2,
            getTestIgniteInstanceName(0),
            "",
            (t, e) -> log.error("", e),
            false,
            0
        ));

        BiConsumerX<String, Integer> checks = (s, i) -> pool.execute(new AttributeValueChecker(s, i), 1);

        execute(checks);

        AttributeValueChecker.assertAllCreatedChecksPassed();
    }

    /** */
    @Test
    public void testContextAwareStripedExecutor() throws Exception {
        IgniteStripedExecutor pool = deferShutdown(new IgniteStripedExecutor(
            2,
            getTestIgniteInstanceName(0),
            "",
            log,
            e -> {},
            false,
            null,
            getTestTimeout()
        ));

        BiConsumerX<String, Integer> checks = (s, i) -> {
            pool.execute(new AttributeValueChecker(s, i));
            pool.execute(1, new AttributeValueChecker(s, i));
        };

        execute(checks);

        AttributeValueChecker.assertAllCreatedChecksPassed();
    }

    /** */
    @Test
    public void testOperationContextAwareScheduledThreadPoolExecutor() throws Exception {
        IgniteScheduledThreadPoolExecutor pool = deferShutdown(new IgniteScheduledThreadPoolExecutor("test", "test", 1));

        doContextAwareExecutorServiceTest(pool);

        CountDownLatch poolUnblockedLatch = blockPool(pool);

        BiConsumerX<String, Integer> checks = (s, i) -> {
            pool.schedule((Callable<Integer>)new AttributeValueChecker(s, i), 100, MILLISECONDS);
            pool.schedule((Runnable)new AttributeValueChecker(s, i), 100, MILLISECONDS);
            pool.scheduleAtFixedRate(new AttributeValueChecker(s, i), 100, 100, MILLISECONDS);
            pool.scheduleWithFixedDelay(new AttributeValueChecker(s, i), 100, 100, MILLISECONDS);
        };

        execute(checks);

        poolUnblockedLatch.countDown();

        AttributeValueChecker.assertAllCreatedChecksPassed();
    }

    /** */
    @Test
    public void testOperationContextAwareForkJoinCommonPool() throws Exception {
        doContextAwareExecutorServiceTest(IgniteForkJoinPool.commonPool());
    }

    /** */
    @Test
    public void testOperationContextAwareForkJoinPool() throws Exception {
        doContextAwareExecutorServiceTest(deferShutdown(new IgniteForkJoinPool("test", "test", 2, null, false)));
    }

    /** */
    @Test
    public void testGridFutureAdapterContextPropagation() throws Exception {
        GridFutureAdapter<Integer> fut = new GridFutureAdapter<>();

        BiConsumerX<String, Integer> checks = (s, i) -> {
            fut.listen(new AttributeValueChecker(s, i));
            fut.listen(AttributeValueChecker.createInClosure(s, i));
            fut.chain(AttributeValueChecker.createClosure(s, i))
                .chain(AttributeValueChecker.createClosure(s, i), IgniteForkJoinPool.commonPool())
                .chain(AttributeValueChecker.createOutClosure(s, i))
                .chain(AttributeValueChecker.createOutClosure(s, i), IgniteForkJoinPool.commonPool())
                .chainCompose(AttributeValueChecker.createComposeClosure(s, i))
                .chainCompose(AttributeValueChecker.createComposeClosure(s, i), IgniteForkJoinPool.commonPool());
        };

        execute(checks);

        try (Scope ignored = OperationContext.set(STR_ATTR, "test", INT_ATTR, 5)) {
            checkAttributeValues("test", 5);

            fut.onDone(0);

            checkAttributeValues("test", 5);
        }

        AttributeValueChecker.assertAllCreatedChecksPassed();
    }

    /** */
    @Test
    public void testCompletableFutureContextPropagation() throws Exception {
        IgniteCompletableFuture<Integer> fut = new IgniteCompletableFuture<>();
        IgniteCompletableFuture<Integer> failedFut = new IgniteCompletableFuture<>();
        IgniteCompletableFuture<Integer> testCompletionStage = new IgniteCompletableFuture<>();

        IgniteCompletableFuture<Void> allFut = IgniteCompletableFuture.allOf(fut, testCompletionStage);
        IgniteCompletableFuture<Object> anyFut = IgniteCompletableFuture.anyOf(fut, testCompletionStage);

        BiConsumerX<String, Integer> checks = (s, i) -> {
            fut.thenCompose(AttributeValueChecker.createCompletableStageFactory(s, i))
                .thenComposeAsync(AttributeValueChecker.createCompletableStageFactory(s, i))
                .thenComposeAsync(AttributeValueChecker.createCompletableStageFactory(s, i), ForkJoinPool.commonPool())
                .thenApply(AttributeValueChecker.createFunction(s, i))
                .thenApplyAsync(AttributeValueChecker.createFunction(s, i))
                .thenApplyAsync(AttributeValueChecker.createFunction(s, i), ForkJoinPool.commonPool())
                .whenComplete(AttributeValueChecker.createBiConsumer(s, i))
                .whenCompleteAsync(AttributeValueChecker.createBiConsumer(s, i))
                .whenCompleteAsync(AttributeValueChecker.createBiConsumer(s, i), ForkJoinPool.commonPool())
                .thenCombine(testCompletionStage, AttributeValueChecker.createBiFunction(s, i))
                .thenCombineAsync(testCompletionStage, AttributeValueChecker.createBiFunction(s, i))
                .thenCombineAsync(testCompletionStage, AttributeValueChecker.createBiFunction(s, i), ForkJoinPool.commonPool())
                .applyToEither(testCompletionStage, AttributeValueChecker.createFunction(s, i))
                .applyToEitherAsync(testCompletionStage, AttributeValueChecker.createFunction(s, i))
                .applyToEitherAsync(testCompletionStage, AttributeValueChecker.createFunction(s, i), ForkJoinPool.commonPool())
                .handle(AttributeValueChecker.createBiFunction(s, i))
                .handleAsync(AttributeValueChecker.createBiFunction(s, i))
                .handleAsync(AttributeValueChecker.createBiFunction(s, i), ForkJoinPool.commonPool());

            fut.thenAccept(AttributeValueChecker.createConsumer(s, i));
            fut.thenAcceptAsync(AttributeValueChecker.createConsumer(s, i));
            fut.thenAcceptAsync(AttributeValueChecker.createConsumer(s, i), ForkJoinPool.commonPool());

            fut.thenRun(new AttributeValueChecker(s, i));
            fut.thenRunAsync(new AttributeValueChecker(s, i));
            fut.thenRunAsync(new AttributeValueChecker(s, i), ForkJoinPool.commonPool());

            fut.thenAcceptBoth(testCompletionStage, AttributeValueChecker.createBiConsumer(s, i));
            fut.thenAcceptBothAsync(testCompletionStage, AttributeValueChecker.createBiConsumer(s, i));
            fut.thenAcceptBothAsync(testCompletionStage, AttributeValueChecker.createBiConsumer(s, i), ForkJoinPool.commonPool());

            fut.runAfterBoth(testCompletionStage, new AttributeValueChecker(s, i));
            fut.runAfterBothAsync(testCompletionStage, new AttributeValueChecker(s, i));
            fut.runAfterBothAsync(testCompletionStage, new AttributeValueChecker(s, i), ForkJoinPool.commonPool());

            fut.acceptEither(testCompletionStage, AttributeValueChecker.createConsumer(s, i));
            fut.acceptEitherAsync(testCompletionStage, AttributeValueChecker.createConsumer(s, i));
            fut.acceptEitherAsync(testCompletionStage, AttributeValueChecker.createConsumer(s, i), ForkJoinPool.commonPool());

            fut.runAfterEither(testCompletionStage, new AttributeValueChecker(s, i));
            fut.runAfterEitherAsync(testCompletionStage, new AttributeValueChecker(s, i));
            fut.runAfterEitherAsync(testCompletionStage, new AttributeValueChecker(s, i), ForkJoinPool.commonPool());

            failedFut.exceptionally(AttributeValueChecker.createFunction(s, i));

            IgniteCompletableFuture.runAsync(new AttributeValueChecker(s, i));
            IgniteCompletableFuture.runAsync(new AttributeValueChecker(s, i), ForkJoinPool.commonPool());

            IgniteCompletableFuture.supplyAsync(AttributeValueChecker.createSupplier(s, i));
            IgniteCompletableFuture.supplyAsync(AttributeValueChecker.createSupplier(s, i), ForkJoinPool.commonPool());
        };

        execute(checks);

        try (Scope ignored = OperationContext.set(STR_ATTR, "test", INT_ATTR, 5)) {
            checkAttributeValues("test", 5);

            fut.complete(0);
            failedFut.completeExceptionally(new IgniteException());
            testCompletionStage.complete(0);

            checkAttributeValues("test", 5);
        }

        AttributeValueChecker.assertAllCreatedChecksPassed();

        anyFut.get(getTestTimeout(), MILLISECONDS);
        allFut.get(getTestTimeout(), MILLISECONDS);
    }

    /** */
    @Test
    public void testTimeoutWorker() throws Exception {
        startGrid(0);

        GridTimeoutProcessor timeoutProc = grid(0).context().timeout();

        List<GridTimeoutProcessor.CancelableTask> scheduledTasks = new ArrayList<>();

        try {
            BiConsumerX<String, Integer> checks = (s, i) -> {
                assertTrue(timeoutProc.addTimeoutObject(AttributeValueChecker.createTimeoutObject(s, i)));
                scheduledTasks.add(timeoutProc.schedule(new AttributeValueChecker(s, i), 100, 100));
            };

            execute(checks);

            AttributeValueChecker.assertAllCreatedChecksPassed();
        }
        finally {
            scheduledTasks.forEach(GridTimeoutProcessor.CancelableTask::close);
        }
    }

    /** */
    @Test
    public void testIgniteThread() throws Exception {
        List<IgniteThread> threads = new ArrayList<>();

        try {
            BiConsumerX<String, Integer> checks = (s, i) ->
                threads.add(new IgniteThread("test", "test", new AttributeValueChecker(s, i)));

            execute(checks);

            threads.forEach(IgniteThread::start);

            AttributeValueChecker.assertAllCreatedChecksPassed();
        }
        finally {
            threads.forEach(IgniteThread::interrupt);

            for (IgniteThread thread : threads)
                thread.join();
        }
    }

    /** */
    @Test
    public void testContextAwareQueue() throws Exception {
        IgniteAsyncObjectHandler<AttributeValueChecker> proc =
            new IgniteAsyncObjectHandler<>("test", "test", log, null) {
                @Override protected void body() throws InterruptedException {
                    while (!isCancelled()) {
                        OperationContextAwareWrapper<AttributeValueChecker> w = pollQueuedElement(100, MILLISECONDS);

                        if (w == null)
                            continue;

                        try (Scope ignored0 = OperationContext.set(STR_ATTR, "test", INT_ATTR, 5)) {
                            try (Scope ignored1 = OperationContext.restoreSnapshot(w.contextSnapshot())) {
                                w.delegate().run();
                            }

                            checkAttributeValues("test", 5);
                        }
                    }

                }
            };

        try {
            proc.start();

            execute((s, i) -> proc.addToQueue(new AttributeValueChecker(s, i)));

            AttributeValueChecker.assertAllCreatedChecksPassed();
        }
        finally {
            U.cancel(proc);
            U.join(proc);
        }
    }

    /** */
    @Test
    public void testContextAwareDelayQueue() throws Exception {
        IgniteDelayedObjectHandler<TestDelayedObject> proc = new IgniteDelayedObjectHandler<>("test", "test", log, null) {
            @Override protected void body() {
                try {
                    while (!isCancelled()) {
                        OperationContextAwareWrapper<TestDelayedObject> w = takeQueuedElement();

                        try (Scope ignored0 = OperationContext.set(STR_ATTR, "test", INT_ATTR, 5)) {
                            try (Scope ignored1 = OperationContext.restoreSnapshot(w.contextSnapshot())) {
                                w.delegate().checker.run();
                            }

                            checkAttributeValues("test", 5);
                        }
                    }
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        try {
            proc.start();

            execute((s, i) -> proc.addToQueue(new TestDelayedObject(new AttributeValueChecker(s, i))));

            AttributeValueChecker.assertAllCreatedChecksPassed();
        }
        finally {
            U.cancel(proc);
            U.join(proc);
        }
    }

    /** */
    @Test
    public void testSendAttributesByDiscovery() throws Exception {
        byte attrId1 = 0;
        byte attrId2 = DistributedOperationContextManager.MAX_DISTRIBUTED_ATTR_CNT - 1;

        InetSocketAddressMessage dfltDistAttr1Val = new InetSocketAddressMessage(InetAddress.getLoopbackAddress(), 80);
        GridCacheVersion dfltDistrAttr2Val = new GridCacheVersion(1, 1, 1);

        // Local attribute 1.
        OperationContextAttribute.newInstance(1000);

        // Distributed attribute 1.
        OperationContextAttribute<InetSocketAddressMessage> dAttr1 = DistributedOperationContextManager.instance()
            .createDistributedAttribute(attrId1, dfltDistAttr1Val);

        // Local attribute 2.
        OperationContextAttribute.newInstance("locaAttr2");

        // Distributed attribute 2.
        OperationContextAttribute<GridCacheVersion> dAttr2 = DistributedOperationContextManager.instance()
            .createDistributedAttribute(attrId2, dfltDistrAttr2Val);

        startGrids(2);
        startClientGrid(2);

        CountDownLatch coordLatch = new CountDownLatch(3);
        CountDownLatch srvrLatch = new CountDownLatch(3);
        CountDownLatch clientLatch = new CountDownLatch(3);

        InetSocketAddressMessage valToSend1 = new InetSocketAddressMessage(dfltDistAttr1Val.address(), 443);
        GridCacheVersion valToSend2 = new GridCacheVersion(2, 2, 2);

        for (int i = 0; i < G.allGrids().size(); ++i) {
            int i0 = i;

            grid(i).context().discovery().setCustomEventListener(
                DynamicCacheChangeBatch.class, new CustomEventListener<>() {
                    @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                        DynamicCacheChangeBatch msg) {

                        InetSocketAddressMessage receivedVal1 = OperationContext.get(dAttr1);
                        GridCacheVersion receivedVal2 = OperationContext.get(dAttr2);

                        assertNotNull(receivedVal1);
                        assertNotNull(receivedVal2);

                        assertFalse(dfltDistAttr1Val.port() == receivedVal1.port());
                        assertEquals(receivedVal1.port(), valToSend1.port());
                        assertEquals(receivedVal1.address(), valToSend1.address());

                        assertFalse(dfltDistrAttr2Val.equals(receivedVal2));
                        assertTrue(valToSend2.equals(receivedVal2));

                        if (grid(i0).localNode().isClient())
                            clientLatch.countDown();
                        else if (grid(i0).localNode().order() == 1)
                            coordLatch.countDown();
                        else
                            srvrLatch.countDown();
                    }
                });
        }

        // Send from the coordinator.
        try (Scope ignored = OperationContext.set(dAttr1, valToSend1, dAttr2, valToSend2)) {
            grid(0).createCache(defaultCacheConfiguration());
        }

        assertTrue(waitForCondition(() -> coordLatch.getCount() == 2, getTestTimeout()));
        assertTrue(waitForCondition(() -> srvrLatch.getCount() == 2, getTestTimeout()));
        assertTrue(waitForCondition(() -> clientLatch.getCount() == 2, getTestTimeout()));

        // Send from a server.
        try (Scope ignored = OperationContext.set(dAttr1, valToSend1, dAttr2, valToSend2)) {
            grid(1).destroyCache(DEFAULT_CACHE_NAME);
        }

        assertTrue(waitForCondition(() -> coordLatch.getCount() == 1, getTestTimeout()));
        assertTrue(waitForCondition(() -> srvrLatch.getCount() == 1, getTestTimeout()));
        assertTrue(waitForCondition(() -> clientLatch.getCount() == 1, getTestTimeout()));

        // Send from a client.
        try (Scope ignored = OperationContext.set(dAttr1, valToSend1, dAttr2, valToSend2)) {
            grid(2).createCache(defaultCacheConfiguration());
        }

        assertTrue(coordLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
        assertTrue(srvrLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
        assertTrue(clientLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
    }

    /** */
    private void doContextAwareExecutorServiceTest(ExecutorService pool) throws Exception {
        CountDownLatch poolUnblockedLatch = blockPool(pool);

        BiConsumerX<String, Integer> asyncChecks = (s, i) -> {
            pool.submit((Runnable)new AttributeValueChecker(s, i));
            pool.submit(new AttributeValueChecker(s, i), 0);
            pool.submit((Callable<Integer>)new AttributeValueChecker(s, i));
        };

        BiConsumerX<String, Integer> syncChecks = (s, i) -> {
            pool.invokeAny(List.of((Callable<Integer>)new AttributeValueChecker(s, i)));
            pool.invokeAny(List.of((Callable<Integer>)new AttributeValueChecker(s, i)), 1000, MILLISECONDS);
            pool.invokeAll(List.of((Callable<Integer>)new AttributeValueChecker(s, i)));
            pool.invokeAll(List.of((Callable<Integer>)new AttributeValueChecker(s, i)), 1000, MILLISECONDS);
        };

        execute(asyncChecks);

        poolUnblockedLatch.countDown();

        execute(syncChecks);

        AttributeValueChecker.assertAllCreatedChecksPassed();
    }

    /** */
    private CountDownLatch blockPool(ExecutorService pool) {
        CountDownLatch latch = new CountDownLatch(1);

        pool.submit(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        return latch;
    }

    /** */
    private <T extends ExecutorService> T deferShutdown(T pool) {
        poolToShutdownAfterTest = pool;

        return pool;
    }

    /** */
    private void execute(BiConsumerX<String, Integer> checks) throws Exception {
        try (Scope ignored = OperationContext.set(STR_ATTR, "test1", INT_ATTR, 1)) {
            checks.accept("test1", 1);
        }

        try (Scope ignored = OperationContext.set(INT_ATTR, 2)) {
            checks.accept(DFLT_STR_VAL, 2);
        }

        try (Scope ignored = OperationContext.set(STR_ATTR, "test2")) {
            checks.accept("test2", DFLT_INT_VAL);
        }

        checks.accept(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    private static void checkAttributeValues(String strAttrVal, Integer intAttrVal) {
        assertEquals(intAttrVal, OperationContext.get(INT_ATTR));
        assertEquals(strAttrVal, OperationContext.get(STR_ATTR));
    }

    /** */
    private static class AttributeValueChecker extends CompletableFuture<Void> implements IgniteRunnable, Callable<Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        static final List<AttributeValueChecker> CHECKS = Collections.synchronizedList(new ArrayList<>());

        /** */
        private final String expStrAttrVal;

        /** */
        private final Integer expIntAttrVal;

        /** */
        public AttributeValueChecker(String expStrAttrVal, Integer expIntAttrVal) {
            this.expStrAttrVal = expStrAttrVal;
            this.expIntAttrVal = expIntAttrVal;

            CHECKS.add(this);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                checkAttributeValues(expStrAttrVal, expIntAttrVal);

                complete(null);
            }
            catch (Throwable e) {
                completeExceptionally(e);
            }
        }

        /** {@inheritDoc} */
        @Override public Integer call() {
            run();

            return 0;
        }

        /** */
        static void assertAllCreatedChecksPassed() throws Exception {
            for (AttributeValueChecker check : CHECKS)
                check.get(5_000, MILLISECONDS);
        }

        /** */
        static GridTimeoutObject createTimeoutObject(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            IgniteUuid id = IgniteUuid.randomUuid();
            long endTime = System.currentTimeMillis() + 1000;

            return new GridTimeoutObject() {
                @Override public IgniteUuid timeoutId() {
                    return id;
                }

                @Override public long endTime() {
                    return endTime;
                }

                @Override public void onTimeout() {
                    checker.run();
                }
            };
        }

        /** */
        static IgniteClosure<IgniteInternalFuture<Integer>, Integer> createClosure(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return fut -> {
                checker.run();

                return 0;
            };
        }

        /** */
        static IgniteClosure<IgniteInternalFuture<Integer>, IgniteInternalFuture<Integer>> createComposeClosure(
            String strAttrVal,
            int intAttrVal
        ) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return fut -> {
                checker.run();

                return fut;
            };
        }

        /** */
        static IgniteInClosure<IgniteInternalFuture<Integer>> createInClosure(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return fut -> checker.run();
        }

        /** */
        static IgniteOutClosure<Integer> createOutClosure(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return () -> {
                checker.run();

                return 0;
            };
        }

        /** */
        static <T> BiFunction<Integer, T, Integer> createBiFunction(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return (r, t) -> {
                checker.run();

                return 0;
            };
        }

        /** */
        static <T> Function<T, Integer> createFunction(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return a -> {
                checker.run();

                return 0;
            };
        }

        /** */
        static Function<Integer, CompletionStage<Integer>> createCompletableStageFactory(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return a -> {
                checker.run();

                return IgniteCompletableFuture.completedFuture(0);
            };
        }

        /** */
        static Supplier<Integer> createSupplier(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return () -> {
                checker.run();

                return 0;
            };
        }

        /** */
        static Consumer<Integer> createConsumer(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return a -> checker.run();
        }

        /** */
        static <T, R> BiConsumer<T, R> createBiConsumer(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return (r, t) -> checker.run();
        }
    }

    /** */
    private static class TestDelayedObject implements Delayed {
        /** */
        private final AttributeValueChecker checker;

        /** */
        private TestDelayedObject(AttributeValueChecker checker) {
            this.checker = checker;
        }

        /** {@inheritDoc} */
        @Override public long getDelay(@NonNull TimeUnit unit) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NonNull Delayed o) {
            return 0;
        }
    }

    /** */
    private interface BiConsumerX<T, U> {
        /** */
        void accept(T t, U u) throws Exception;
    }
}
