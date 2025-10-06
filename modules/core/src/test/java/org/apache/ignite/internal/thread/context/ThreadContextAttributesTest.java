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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.thread.context.pool.ThreadContextAwareStripedExecutor;
import org.apache.ignite.internal.thread.context.pool.ThreadContextAwareStripedThreadPoolExecutor;
import org.apache.ignite.internal.thread.context.pool.ThreadContextAwareThreadPoolExecutor;
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
        ThreadContextAwareThreadPoolExecutor pool = deferShutdown(new ThreadContextAwareThreadPoolExecutor(
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
        ThreadContextAwareStripedThreadPoolExecutor pool = deferShutdown(new ThreadContextAwareStripedThreadPoolExecutor(
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
        ThreadContextAwareStripedExecutor pool = deferShutdown(new ThreadContextAwareStripedExecutor(
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
    private static class AttributeValueChecker extends CompletableFuture<Void> implements Runnable, Callable<Integer> {
        /** */
        static final List<AttributeValueChecker> CHECKS = new ArrayList<>();

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
    }

    /** */
    private interface BiConsumerX<T, U> {
        /** */
        void accept(T t, U u) throws Exception;
    }
}
