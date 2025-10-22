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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.thread.IgniteForkJoinPool;
import org.apache.ignite.internal.thread.IgniteScheduledThreadPoolExecutor;
import org.apache.ignite.internal.thread.IgniteStripedExecutor;
import org.apache.ignite.internal.thread.IgniteStripedThreadPoolExecutor;
import org.apache.ignite.internal.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.internal.thread.context.concurrent.IgniteCompletableFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/** */
public class ThreadContextAttributesTest extends GridCommonAbstractTest {
    /** */
    private static final String DFLT_STR_VAL = null;

    /** */
    private static final Integer DFLT_INT_VAL = 0;

    /** */
    private static final ThreadContextAttribute<String> STR_ATTR = ThreadContextAttributeRegistry.instance().register();

    /** */
    private static final ThreadContextAttribute<Integer> INT_ATTR = ThreadContextAttributeRegistry.instance().register(DFLT_INT_VAL);

    /** */
    private ExecutorService poolToShutdownAfterTest;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        AttributeValueChecker.CHECKS.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (poolToShutdownAfterTest != null)
            poolToShutdownAfterTest.shutdownNow();
    }

    /** */
    @Test
    public void testThreadScopeAttributes() {
        String strAttrVal = "test";
        int intAttrVal = 1;

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored0 = ThreadContext.withAttribute(STR_ATTR, DFLT_STR_VAL).withAttribute(INT_ATTR, DFLT_INT_VAL)) {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            try (Scope ignored1 = ThreadContext.withAttribute(STR_ATTR, strAttrVal)) {
                checkAttributeValues(strAttrVal, DFLT_INT_VAL);

                try (Scope ignored2 = ThreadContext.withAttribute(INT_ATTR, intAttrVal)) {
                    checkAttributeValues(strAttrVal, intAttrVal);

                    try (Scope ignored3 = ThreadContext.withAttribute(STR_ATTR, DFLT_STR_VAL)) {
                        checkAttributeValues(DFLT_STR_VAL, intAttrVal);
                    }

                    checkAttributeValues(strAttrVal, intAttrVal);
                }

                checkAttributeValues(strAttrVal, DFLT_INT_VAL);
            }

            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testScopeAttributeValueOverwrite() {
        try (Scope ignored0 = ThreadContext.withAttribute(STR_ATTR, "test").withAttribute(INT_ATTR, 3)) {
            checkAttributeValues("test", 3);

            try (Scope ignored1 = ThreadContext.withAttribute(INT_ATTR, 1).withAttribute(INT_ATTR, 2)) {
                checkAttributeValues("test", 2);
            }

            checkAttributeValues("test", 3);
        }
    }

    /** */
    @Test
    public void testThreadContextSnapshot() {
        List<ThreadContextSnapshotChecker> checkers = new ArrayList<>();

        checkers.add(ThreadContextSnapshotChecker.create());

        try (Scope ignored0 = ThreadContext.withAttribute(STR_ATTR, DFLT_STR_VAL).withAttribute(INT_ATTR, DFLT_INT_VAL)) {
            checkers.add(ThreadContextSnapshotChecker.create());

            try (Scope ignored1 = ThreadContext.withAttribute(STR_ATTR, "test")) {
                checkers.add(ThreadContextSnapshotChecker.create());

                try (Scope ignored2 = ThreadContext.withAttribute(INT_ATTR, 1)) {
                    checkers.add(ThreadContextSnapshotChecker.create());

                    try (Scope ignored3 = ThreadContext.withAttribute(STR_ATTR, DFLT_STR_VAL)) {
                        checkers.add(ThreadContextSnapshotChecker.create());
                    }

                    checkers.add(ThreadContextSnapshotChecker.create());
                }

                checkers.add(ThreadContextSnapshotChecker.create());
            }

            checkers.add(ThreadContextSnapshotChecker.create());
        }

        checkers.add(ThreadContextSnapshotChecker.create());

        checkers.forEach(ThreadContextSnapshotChecker::check);
    }

    /** */
    @Test
    public void testRuntimeAttributeRegistration() {
        // Initializes the Context Data for the current thread to check the growth of the array containing attribute values.
        assertEquals(DFLT_INT_VAL, ThreadContext.get(INT_ATTR));

        ThreadContextAttribute<Object> attr = ThreadContextAttributeRegistry.instance().register();

        assertEquals(null, ThreadContext.get(attr));

        try (Scope ignored0 = ThreadContext.withAttribute(attr, "test")) {
            assertEquals("test", ThreadContext.get(attr));
        }

        assertEquals(null, ThreadContext.get(attr));
    }

    /** */
    @Test
    public void testThreadContextAwareThreadPool() throws Exception {
        IgniteThreadPoolExecutor pool = deferShutdown(new IgniteThreadPoolExecutor(
            "test",
            null,
            1,
            1,
            Long.MAX_VALUE,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            null));

        doThreadContextAwareExecutorServiceTest(pool);
    }

    /** */
    @Test
    public void testThreadContextAwareStripedThreadPoolExecutor() throws Exception {
        IgniteStripedThreadPoolExecutor pool = deferShutdown(new IgniteStripedThreadPoolExecutor(
            2,
            getTestIgniteInstanceName(0),
            "",
            (t, e) -> log.error("", e),
            false,
            0
        ));

        BiConsumerX<String, Integer> checks = (s, i) -> pool.execute(new AttributeValueChecker(s, i), 1);

        createAttributeChecks(checks);

        AttributeValueChecker.assertAllCreatedChecksPassed();
    }

    /** */
    @Test
    public void testThreadContextAwareStripedExecutor() throws Exception {
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
            pool.execute( new AttributeValueChecker(s, i));
            pool.execute(1, new AttributeValueChecker(s, i));
        };

        createAttributeChecks(checks);

        AttributeValueChecker.assertAllCreatedChecksPassed();
    }

    /** */
    @Test
    public void testThreadContextAwareScheduledThreadPoolExecutor() throws Exception {
        IgniteScheduledThreadPoolExecutor pool = deferShutdown(new IgniteScheduledThreadPoolExecutor("test", "test", 1));

        doThreadContextAwareExecutorServiceTest(pool);

        CountDownLatch poolUnblockedLatch = blockPool(pool);

        BiConsumerX<String, Integer> checks = (s, i) -> {
            pool.schedule((Callable<Integer>)new AttributeValueChecker(s, i), 100, MILLISECONDS);
            pool.schedule((Runnable)new AttributeValueChecker(s, i), 100, MILLISECONDS);
            pool.scheduleAtFixedRate(new AttributeValueChecker(s, i), 100, 100, MILLISECONDS);
            pool.scheduleWithFixedDelay(new AttributeValueChecker(s, i), 100, 100, MILLISECONDS);
        };

        createAttributeChecks(checks);

        poolUnblockedLatch.countDown();

        AttributeValueChecker.assertAllCreatedChecksPassed();
    }

    /** */
    @Test
    public void testThreadContextAwareForkJoinCommonPool() throws Exception {
        doThreadContextAwareExecutorServiceTest(IgniteForkJoinPool.commonPool());
    }

    /** */
    @Test
    public void testThreadContextAwareForkJoinPool() throws Exception {
        doThreadContextAwareExecutorServiceTest(deferShutdown(new IgniteForkJoinPool()));
    }

    /** */
    @Test
    public void testGridFutureAdapterContextPropagation() throws Exception {
        GridFutureAdapter<Integer> fut = new GridFutureAdapter<>();

        BiConsumerX<String, Integer> checks = (s, i) -> {
            fut.listen(new AttributeValueChecker(s, i));
            fut.listen(AttributeValueChecker.createListenInClosureChecker(s, i));
            fut.chain(AttributeValueChecker.createChainClosureChecker(s, i))
                .chain(AttributeValueChecker.createChainClosureChecker(s, i), IgniteForkJoinPool.commonPool())
                .chain(AttributeValueChecker.createChainOutClosureChecker(s, i))
                .chain(AttributeValueChecker.createChainOutClosureChecker(s, i), IgniteForkJoinPool.commonPool())
                .chainCompose(AttributeValueChecker.createChainComposeChecker(s, i))
                .chainCompose(AttributeValueChecker.createChainComposeChecker(s, i), IgniteForkJoinPool.commonPool());
        };

        createAttributeChecks(checks);

        try (Scope ignored = ThreadContext.withAttribute(STR_ATTR, "test").withAttribute(INT_ATTR, 5)) {
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

        BiConsumerX<String, Integer> checks = (s, i) -> {
            fut.thenCompose(AttributeValueChecker.createCompletableStageFactory(s, i))
                .thenComposeAsync(AttributeValueChecker.createCompletableStageFactory(s, i))
                .thenComposeAsync(AttributeValueChecker.createCompletableStageFactory(s, i), IgniteForkJoinPool.commonPool())
                .thenApply(AttributeValueChecker.createFunction(s, i))
                .thenApplyAsync(AttributeValueChecker.createFunction(s, i))
                .thenApplyAsync(AttributeValueChecker.createFunction(s, i), IgniteForkJoinPool.commonPool())
                .whenComplete(AttributeValueChecker.createBiConsumer(s, i))
                .whenCompleteAsync(AttributeValueChecker.createBiConsumer(s, i))
                .whenCompleteAsync(AttributeValueChecker.createBiConsumer(s, i), IgniteForkJoinPool.commonPool())
                .thenCombine(testCompletionStage, AttributeValueChecker.createBiFunction(s, i))
                .thenCombineAsync(testCompletionStage, AttributeValueChecker.createBiFunction(s, i))
                .thenCombineAsync(testCompletionStage, AttributeValueChecker.createBiFunction(s, i), IgniteForkJoinPool.commonPool())
                .applyToEither(testCompletionStage, AttributeValueChecker.createFunction(s, i))
                .applyToEitherAsync(testCompletionStage, AttributeValueChecker.createFunction(s, i))
                .applyToEitherAsync(testCompletionStage, AttributeValueChecker.createFunction(s, i), IgniteForkJoinPool.commonPool())
                .handle(AttributeValueChecker.createBiFunction(s, i))
                .handleAsync(AttributeValueChecker.createBiFunction(s, i))
                .handleAsync(AttributeValueChecker.createBiFunction(s, i), IgniteForkJoinPool.commonPool());

            fut.thenAccept(AttributeValueChecker.createConsumer(s, i));
            fut.thenAcceptAsync(AttributeValueChecker.createConsumer(s, i));
            fut.thenAcceptAsync(AttributeValueChecker.createConsumer(s, i), IgniteForkJoinPool.commonPool());

            fut.thenRun(new AttributeValueChecker(s, i));
            fut.thenRunAsync(new AttributeValueChecker(s, i));
            fut.thenRunAsync(new AttributeValueChecker(s, i), IgniteForkJoinPool.commonPool());

            fut.thenAcceptBoth(testCompletionStage, AttributeValueChecker.createBiConsumer(s, i));
            fut.thenAcceptBothAsync(testCompletionStage, AttributeValueChecker.createBiConsumer(s, i));
            fut.thenAcceptBothAsync(testCompletionStage, AttributeValueChecker.createBiConsumer(s, i), IgniteForkJoinPool.commonPool());

            fut.runAfterBoth(testCompletionStage, new AttributeValueChecker(s, i));
            fut.runAfterBothAsync(testCompletionStage, new AttributeValueChecker(s, i));
            fut.runAfterBothAsync(testCompletionStage, new AttributeValueChecker(s, i), IgniteForkJoinPool.commonPool());

            fut.acceptEither(testCompletionStage, AttributeValueChecker.createConsumer(s, i));
            fut.acceptEitherAsync(testCompletionStage, AttributeValueChecker.createConsumer(s, i));
            fut.acceptEitherAsync(testCompletionStage, AttributeValueChecker.createConsumer(s, i), IgniteForkJoinPool.commonPool());

            fut.runAfterEither(testCompletionStage, new AttributeValueChecker(s, i));
            fut.runAfterEitherAsync(testCompletionStage, new AttributeValueChecker(s, i));
            fut.runAfterEitherAsync(testCompletionStage, new AttributeValueChecker(s, i), IgniteForkJoinPool.commonPool());

            failedFut.exceptionally(AttributeValueChecker.createFunction(s, i));
        };

        createAttributeChecks(checks);

        try (Scope ignored = ThreadContext.withAttribute(STR_ATTR, "test").withAttribute(INT_ATTR, 5)) {
            checkAttributeValues("test", 5);

            fut.complete(0);
            failedFut.completeExceptionally(new IgniteException());
            testCompletionStage.complete(0);

            checkAttributeValues("test", 5);
        }

        AttributeValueChecker.assertAllCreatedChecksPassed();
    }

    /** */
    private void doThreadContextAwareExecutorServiceTest(ExecutorService pool) throws Exception {
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

        createAttributeChecks(asyncChecks);

        poolUnblockedLatch.countDown();

        createAttributeChecks(syncChecks);

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
    private void createAttributeChecks(BiConsumerX<String, Integer> checkGenerator) throws Exception {
        try (Scope ignored = ThreadContext.withAttribute(STR_ATTR, "test0").withAttribute(INT_ATTR, 1)) {
            checkGenerator.accept("test0", 1);
        }

        try (Scope ignored = ThreadContext.withAttribute(STR_ATTR, "test1").withAttribute(INT_ATTR, 2)) {
            checkGenerator.accept("test1", 2);
        }

        checkGenerator.accept(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    private static void checkAttributeValues(String strAttrVal, Integer intAttrVal) {
        assertEquals(intAttrVal, ThreadContext.get(INT_ATTR));
        assertEquals(strAttrVal, ThreadContext.get(STR_ATTR));
    }

    /** */
    private static class ThreadContextSnapshotChecker {
        /** */
        private final ThreadContextSnapshot snapshot;

        /** */
        private final String strAttrVal;

        /** */
        private final Integer intAttrVal;

        /** */
        private ThreadContextSnapshotChecker(ThreadContextSnapshot snapshot, String strAttrVal, Integer intAttrVal) {
            this.snapshot = snapshot;
            this.strAttrVal = strAttrVal;
            this.intAttrVal = intAttrVal;
        }

        /** */
        public void check() {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            // Checks that snapshot are restored correctly for attributes with both initialized and initial values.
            try (Scope ignored0 = ThreadContext.withAttribute(STR_ATTR, "test")) {
                checkAttributeValues("test", DFLT_INT_VAL);

                try (Scope ignored1 = ThreadContext.withSnapshot(snapshot)) {
                    checkAttributeValues(strAttrVal, intAttrVal);
                }

                checkAttributeValues("test", DFLT_INT_VAL);
            }

            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
        }

        /** */
        public static ThreadContextSnapshotChecker create() {
            return new ThreadContextSnapshotChecker(
                ThreadContext.createSnapshot(),
                ThreadContext.get(STR_ATTR),
                ThreadContext.get(INT_ATTR)
            );
        }
    }

    /** */
    private static class AttributeValueChecker extends CompletableFuture<Void> implements IgniteRunnable, Callable<Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        static final List<AttributeValueChecker> CHECKS = Collections.synchronizedList(new ArrayList<>());

        /** */
        private final String strAttrVal;

        /** */
        private final Integer intAttrVal;

        /** */
        public AttributeValueChecker(String strAttrVal, Integer intAttrVal) {
            this.strAttrVal = strAttrVal;
            this.intAttrVal = intAttrVal;

            CHECKS.add(this);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                checkAttributeValues(strAttrVal, intAttrVal);

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
            for (AttributeValueChecker check : CHECKS) {
                check.get(1000, MILLISECONDS);
            }
        }

        /** */
        static IgniteClosure<IgniteInternalFuture<Integer>, Integer> createChainClosureChecker(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return fut -> {
                checker.run();

                return 0;
            };
        }

        /** */
        static IgniteClosure<IgniteInternalFuture<Integer>, IgniteInternalFuture<Integer>> createChainComposeChecker(
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
        static IgniteInClosure<IgniteInternalFuture<Integer>> createListenInClosureChecker(String strAttrVal, int intAttrVal) {
            AttributeValueChecker checker = new AttributeValueChecker(strAttrVal, intAttrVal);

            return fut -> checker.run();
        }

        /** */
        static IgniteOutClosure<Integer> createChainOutClosureChecker(String strAttrVal, int intAttrVal) {
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
    private interface BiConsumerX<T, U> {
        /** */
        void accept(T t, U u) throws Exception;
    }
}
