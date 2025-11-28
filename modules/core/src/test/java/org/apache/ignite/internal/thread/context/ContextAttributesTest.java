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
import org.apache.ignite.internal.thread.context.pool.ContextAwareStripedExecutor;
import org.apache.ignite.internal.thread.context.pool.ContextAwareStripedThreadPoolExecutor;
import org.apache.ignite.internal.thread.context.pool.ContextAwareThreadPoolExecutor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** */
public class ContextAttributesTest extends GridCommonAbstractTest {
    /** */
    private static final String DFLT_STR_VAL = "default";

    /** */
    private static final int DFLT_INT_VAL = -1;

    /** */
    private static final ContextAttribute<String> STR_ATTR = ContextAttribute.newInstance(DFLT_STR_VAL);

    /** */
    private static final ContextAttribute<Integer> INT_ATTR = ContextAttribute.newInstance(DFLT_INT_VAL);

    /** */
    private ExecutorService poolToShutdownAfterTest;

    /** */
    private int beforeTestAttrsHighId;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        AttributeValueChecker.CHECKS.clear();

        beforeTestAttrsHighId = ContextAttribute.highReservedId();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (poolToShutdownAfterTest != null)
            poolToShutdownAfterTest.shutdownNow();

        // Releases attribute IDs reserved during the test.
        ContextAttribute.ID_GEN.set(beforeTestAttrsHighId);
    }

    /** */
    @Test
    public void testNotAttachedAttribute() {
        // No opened scope.
        assertEquals(DFLT_STR_VAL, STR_ATTR.get());

        // Scope opened but testing attribute is not set.
        try (Scope ignored = Context.Builder.create().with(INT_ATTR, 0).build().attach()) {
            assertEquals(DFLT_STR_VAL, STR_ATTR.get());
        }
    }

    /** */
    @Test
    public void testAttachedAttribute() {
        try (Scope ignored = Context.Builder.create().with(STR_ATTR, "test").build().attach()) {
            assertEquals("test", STR_ATTR.get());
        }
    }

    /** */
    @Test
    public void testAttributeValueSearchUpScopeStack() {
        try (Scope ignored1 = Context.Builder.create().with(STR_ATTR, "test1").build().attach()) {
            try (Scope ignored2 = Context.Builder.create().with(INT_ATTR, 2).build().attach()) {
                checkAttributeValues("test1", 2);
            }
        }
    }

    /** */
    @Test
    public void testAttributeValueOverwrite() {
        try (Scope ignored = Context.Builder.create().with(STR_ATTR, "test1").with(INT_ATTR, 1).with(STR_ATTR, "test2").build().attach()) {
            checkAttributeValues("test2", 1);
        }
    }

    /** */
    @Test
    public void testConsequentScopes() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored1 = Context.Builder.create().with(STR_ATTR, "test1").with(INT_ATTR, 1).build().attach()) {
            checkAttributeValues("test1", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored2 = Context.Builder.create().with(INT_ATTR, 2).build().attach()) {
            checkAttributeValues(DFLT_STR_VAL, 2);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopes() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored1 = Context.Builder.create().with(INT_ATTR, 1).build().attach()) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            try (Scope ignored2 = Context.Builder.create().with(STR_ATTR, "test2").build().attach()) {
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

        try (Scope ignored1 = Context.Builder.create().with(INT_ATTR, 1).with(STR_ATTR, "test1").build().attach()) {
            checkAttributeValues("test1", 1);

            try (Scope ignored2 = Context.Builder.create().with(STR_ATTR, "test2").build().attach()) {
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

        try (Scope ignored1 = Context.Builder.create().with(INT_ATTR, null).with(STR_ATTR, null).build().attach()) {
            checkAttributeValues(null, null);

            try (Scope ignored2 = Context.Builder.create().with(STR_ATTR, "test2").build().attach()) {
                checkAttributeValues("test2", null);

                try (Scope ignored3 = Context.Builder.create().with(STR_ATTR, null).build().attach()) {
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
        try (Scope scope1 = Context.Builder.create().with(INT_ATTR, DFLT_INT_VAL).with(STR_ATTR, DFLT_STR_VAL).build().attach()) {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            assertTrue(scope1 == Scope.NOOP_SCOPE);

            try (Scope scope2 = Context.Builder.create().with(INT_ATTR, DFLT_INT_VAL).build().attach()) {
                checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

                assertTrue(scope2 == Scope.NOOP_SCOPE);
            }

            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
        }
    }

    /** */
    @Test
    public void testNestedScopeWithTheSameAttributeValue() {
        try (Scope ignored1 = Context.Builder.create().with(INT_ATTR, 1).build().attach()) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            try (Scope scope = Context.Builder.create().with(INT_ATTR, 1).build().attach()) {
                checkAttributeValues(DFLT_STR_VAL, 1);

                assertTrue(scope == Scope.NOOP_SCOPE);
            }

            checkAttributeValues(DFLT_STR_VAL, 1);
        }
    }

    /** */
    @Test
    public void testContextCacheReinitialization() {
        try (Scope ignored1 = Context.Builder.create().with(INT_ATTR, 1).build().attach()) {
            ContextAttribute<Object> attr = ContextAttribute.newInstance();

            assertNull(attr.get());

            try (Scope ignored2 = Context.Builder.create().with(attr, "test").build().attach()) {
                assertEquals("test", attr.get());
            }

            assertNull(attr.get());
        }
    }

    /** */
    @Test
    public void testMaximumAttributesInstanceCount() {
        int cnt = ContextAttribute.MAX_ATTR_CNT - ContextAttribute.highReservedId();

        List<ContextAttribute<Integer>> attrs = new ArrayList<>(cnt);

        attrs.add(ContextAttribute.newInstance());

        Context.Builder builder = Context.Builder.create().with(attrs.get(0), 0);

        for (int i = 1; i < cnt; i++) {
            attrs.add(ContextAttribute.newInstance());

            builder = builder.with(attrs.get(i), i);

        }

        try (Scope ignored = builder.build().attach()) {
            for (int i = 0; i < cnt; i++)
                assertTrue(i == attrs.get(i).get());
        }

        assertTrue(attrs.stream().allMatch(attr -> attr.get() == null));

        assertThrowsAnyCause(
            log,
            ContextAttribute::newInstance,
            AssertionError.class,
            "Exceeded maximum supported number of created Attributes instances"
        );
    }

    /** */
    @Test
    public void testEmptySnapshot() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        ContextSnapshot snapshot = ContextSnapshot.capture();

        try (Scope ignored = snapshot.restore()) {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testSnapshot() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        ContextSnapshot snapshot;

        try (Scope ignored = Context.Builder.create().with(INT_ATTR, 1).with(STR_ATTR, "test1").build().attach()) {
            checkAttributeValues("test1", 1);

            snapshot = ContextSnapshot.capture();
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored = snapshot.restore()) {
            checkAttributeValues("test1", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopeSnapshot() {
        ContextSnapshot snapshot;

        try (Scope ignored1 = Context.Builder.create().with(INT_ATTR, 1).with(STR_ATTR, "test1").build().attach()) {
            try (Scope ignored2 = Context.Builder.create().with(STR_ATTR, "test2").build().attach()) {
                checkAttributeValues("test2", 1);

                snapshot = ContextSnapshot.capture();
            }
        }

        try (Scope ignored = snapshot.restore()) {
            checkAttributeValues("test2", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopeInSnapshotScope() {
        ContextSnapshot snapshot0;

        try (Scope ignored = Context.Builder.create().with(INT_ATTR, 1).with(STR_ATTR, "test1").build().attach()) {
            checkAttributeValues("test1", 1);

            snapshot0 = ContextSnapshot.capture();
        }

        ContextSnapshot snapshot1;

        try (Scope ignored1 = snapshot0.restore()) {
            checkAttributeValues("test1", 1);

            try (Scope ignored2 = Context.Builder.create().with(INT_ATTR, 2).build().attach()) {
                checkAttributeValues("test1", 2);

                snapshot1 = ContextSnapshot.capture();
            }

            checkAttributeValues("test1", 1);
        }

        try (Scope ignored0 = snapshot1.restore()) {
            checkAttributeValues("test1", 2);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testSnapshotRestoreInExistingScope() {
        ContextSnapshot snapshot;

        try (Scope ignored = Context.Builder.create().with(STR_ATTR, "test1").build().attach()) {
            checkAttributeValues("test1", DFLT_INT_VAL);

            snapshot = ContextSnapshot.capture();
        }

        try (Scope ignored1 = Context.Builder.create().with(INT_ATTR, 1).build().attach()) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            // Note, snapshot restores the state of the entire context, including attributes that do not have a value set.
            try (Scope ignored2 = snapshot.restore()) {
                checkAttributeValues("test1", DFLT_INT_VAL);
            }

            checkAttributeValues(DFLT_STR_VAL, 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testScopedContextAwareThreadPool() throws Exception {
        ContextAwareThreadPoolExecutor pool = deferShutdown(new ContextAwareThreadPoolExecutor(
            "test",
            null,
            1,
            1,
            Long.MAX_VALUE,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            null));

        doScopedContextAwareExecutorServiceTest(pool);
    }

    /** */
    @Test
    public void testScopedContextAwareStripedThreadPoolExecutor() throws Exception {
        ContextAwareStripedThreadPoolExecutor pool = deferShutdown(new ContextAwareStripedThreadPoolExecutor(
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
    public void testScopedContextAwareStripedExecutor() throws Exception {
        ContextAwareStripedExecutor pool = deferShutdown(new ContextAwareStripedExecutor(
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
    private void doScopedContextAwareExecutorServiceTest(ExecutorService pool) throws Exception {
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
        try (Scope ignored = Context.Builder.create().with(STR_ATTR, "test1").with(INT_ATTR, 1).build().attach()) {
            checkGenerator.accept("test1", 1);
        }

        try (Scope ignored = Context.Builder.create().with(STR_ATTR, "test2").with(INT_ATTR, 2).build().attach()) {
            checkGenerator.accept("test2", 2);
        }

        checkGenerator.accept(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    private static void checkAttributeValues(String strAttrVal, Integer intAttrVal) {
        assertEquals(intAttrVal, INT_ATTR.get());
        assertEquals(strAttrVal, STR_ATTR.get());
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
