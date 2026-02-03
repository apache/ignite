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
import java.util.LinkedList;
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
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

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
    private int beforeTestReservedAttrIds;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        AttributeValueChecker.CHECKS.clear();

        beforeTestReservedAttrIds = ContextAttribute.ID_GEN.get();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (poolToShutdownAfterTest != null)
            poolToShutdownAfterTest.shutdownNow();

        // Releases attribute IDs reserved during the test.
        ContextAttribute.ID_GEN.set(beforeTestReservedAttrIds);
    }

    /** */
    @Test
    public void testNotAttachedAttribute() {
        // No opened scope.
        assertEquals(DFLT_STR_VAL, Context.get(STR_ATTR));

        // Scope opened but testing attribute is not set.
        try (Scope ignored = Context.set(INT_ATTR, 0)) {
            assertEquals(DFLT_STR_VAL, Context.get(STR_ATTR));
        }
    }

    /** */
    @Test
    public void testAttachedAttribute() {
        try (Scope ignored = Context.set(STR_ATTR, "test")) {
            assertEquals("test", Context.get(STR_ATTR));
        }
    }

    /** */
    @Test
    public void testAttributeValueSearchUpScopeStack() {
        try (Scope ignored1 = Context.set(STR_ATTR, "test1")) {
            try (Scope ignored2 = Context.set(INT_ATTR, 2)) {
                checkAttributeValues("test1", 2);
            }
        }
    }

    /** */
    @Test
    public void testAttributeValueOverwrite() {
        try (Scope ignored = Context.set(STR_ATTR, "test1", INT_ATTR, 1, STR_ATTR, "test2")) {
            checkAttributeValues("test2", 1);
        }
    }

    /** */
    @Test
    public void testConsequentScopes() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored1 = Context.set(STR_ATTR, "test1", INT_ATTR, 1)) {
            checkAttributeValues("test1", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored2 = Context.set(INT_ATTR, 2)) {
            checkAttributeValues(DFLT_STR_VAL, 2);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopes() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored1 = Context.set(INT_ATTR, 1)) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            try (Scope ignored2 = Context.set(STR_ATTR, "test2")) {
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

        try (Scope ignored1 = Context.set(INT_ATTR, 1, STR_ATTR, "test1")) {
            checkAttributeValues("test1", 1);

            try (Scope ignored2 = Context.set(STR_ATTR, "test2")) {
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

        try (Scope ignored1 = Context.set(INT_ATTR, null, STR_ATTR, null)) {
            checkAttributeValues(null, null);

            try (Scope ignored2 = Context.set(STR_ATTR, "test2")) {
                checkAttributeValues("test2", null);

                try (Scope ignored3 = Context.set(STR_ATTR, null)) {
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
        try (Scope scope1 = Context.set(INT_ATTR, DFLT_INT_VAL, STR_ATTR, DFLT_STR_VAL)) {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            assertTrue(scope1 == Scope.NOOP_SCOPE);

            try (Scope scope2 = Context.set(INT_ATTR, DFLT_INT_VAL)) {
                checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

                assertTrue(scope2 == Scope.NOOP_SCOPE);
            }

            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
        }
    }

    /** */
    @Test
    public void testNestedScopeWithTheSameAttributeValue() {
        try (Scope ignored1 = Context.set(INT_ATTR, 1)) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            try (Scope scope = Context.set(INT_ATTR, 1)) {
                checkAttributeValues(DFLT_STR_VAL, 1);

                assertTrue(scope == Scope.NOOP_SCOPE);
            }

            checkAttributeValues(DFLT_STR_VAL, 1);
        }
    }

    /** */
    @Test
    public void testRuntimeAttributeCreation() {
        try (Scope ignored1 = Context.set(INT_ATTR, 1)) {
            ContextAttribute<Object> attr = ContextAttribute.newInstance();

            assertNull(Context.get(attr));

            try (Scope ignored2 = Context.set(attr, "test")) {
                assertEquals("test", Context.get(attr));
            }

            assertNull(Context.get(attr));
        }
    }

    /** */
    @Test
    public void testMaximumAttributesInstanceCount() {
        int cnt = ContextAttribute.MAX_ATTR_CNT - ContextAttribute.ID_GEN.get();

        List<ContextAttribute<Integer>> attrs = new ArrayList<>(cnt);
        LinkedList<Scope> scopes = new LinkedList<>();

        for (int i = 0; i < cnt; i++) {
            attrs.add(ContextAttribute.newInstance());

            scopes.push(Context.set(attrs.get(i), i));
        }

        try {
            for (int i = 0; i < cnt; i++)
                assertTrue(i == Context.get(attrs.get(i)));
        }
        finally {
            scopes.forEach(Scope::close);
        }

        assertTrue(attrs.stream().allMatch(attr -> Context.get(attr) == null));

        assertThrowsAnyCause(
            log,
            ContextAttribute::newInstance,
            AssertionError.class,
            "Exceeded maximum supported number of created Attributes instances"
        );
    }

    /** */
    @Test
    public void testUnorderedScopeClosing() {
        Scope scope1 = Context.set(INT_ATTR, 0);

        try {
            try (Scope ignored = Context.set(STR_ATTR, "test")) {
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

        ContextSnapshot snapshot = Context.createSnapshot();

        try (Scope ignored = Context.restoreSnapshot(snapshot)) {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testSnapshot() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        ContextSnapshot snapshot;

        try (Scope ignored = Context.set(INT_ATTR, 1, STR_ATTR, "test1")) {
            checkAttributeValues("test1", 1);

            snapshot = Context.createSnapshot();
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored = Context.restoreSnapshot(snapshot)) {
            checkAttributeValues("test1", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopeSnapshot() {
        ContextSnapshot snapshot;

        try (Scope ignored1 = Context.set(INT_ATTR, 1, STR_ATTR, "test1")) {
            try (Scope ignored2 = Context.set(STR_ATTR, "test2")) {
                checkAttributeValues("test2", 1);

                snapshot = Context.createSnapshot();
            }
        }

        try (Scope ignored = Context.restoreSnapshot(snapshot)) {
            checkAttributeValues("test2", 1);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testNestedScopeInSnapshotScope() {
        ContextSnapshot snapshot0;

        try (Scope ignored = Context.set(INT_ATTR, 1, STR_ATTR, "test1")) {
            checkAttributeValues("test1", 1);

            snapshot0 = Context.createSnapshot();
        }

        ContextSnapshot snapshot1;

        try (Scope ignored1 = Context.restoreSnapshot(snapshot0)) {
            checkAttributeValues("test1", 1);

            try (Scope ignored2 = Context.set(INT_ATTR, 2)) {
                checkAttributeValues("test1", 2);

                snapshot1 = Context.createSnapshot();
            }

            checkAttributeValues("test1", 1);
        }

        try (Scope ignored0 = Context.restoreSnapshot(snapshot1)) {
            checkAttributeValues("test1", 2);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testSnapshotRestoreInExistingScope() {
        ContextSnapshot snapshot;

        try (Scope ignored = Context.set(STR_ATTR, "test1")) {
            checkAttributeValues("test1", DFLT_INT_VAL);

            snapshot = Context.createSnapshot();
        }

        try (Scope ignored1 = Context.set(INT_ATTR, 1)) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            // Note, snapshot restores the state of the entire context, including attributes that do not have a value set.
            try (Scope ignored2 = Context.restoreSnapshot(snapshot)) {
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

        ContextSnapshot snapshot;

        try (Scope ignored0 = Context.set(INT_ATTR, 1)) {
            checkAttributeValues(DFLT_STR_VAL, 1);

            snapshot = Context.createSnapshot();

            try (Scope ignored1 = Context.set(STR_ATTR, "test")) {
                checkAttributeValues("test", 1);

                try (Scope ignored = Context.restoreSnapshot(snapshot)) {
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
        ContextSnapshot snapshot;

        try (Scope ignored = Context.set(STR_ATTR, "test1")) {
            checkAttributeValues("test1", DFLT_INT_VAL);

            snapshot = Context.createSnapshot();
        }

        try (Scope snpScope = Context.restoreSnapshot(snapshot)) {
            try (Scope ignored1 = Context.set(INT_ATTR, 2)) {
                checkAttributeValues("test1", 2);

                assertThrowsWithCause(snpScope::close, AssertionError.class);
            }
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testContextAwareThreadPool() throws Exception {
        ContextAwareThreadPoolExecutor pool = deferShutdown(new ContextAwareThreadPoolExecutor(
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
    public void testContextAwareStripedExecutor() throws Exception {
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
        try (Scope ignored = Context.set(STR_ATTR, "test1", INT_ATTR, 1)) {
            checkGenerator.accept("test1", 1);
        }

        try (Scope ignored = Context.set(STR_ATTR, "test2", INT_ATTR, 2)) {
            checkGenerator.accept("test2", 2);
        }

        checkGenerator.accept(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    private static void checkAttributeValues(String strAttrVal, Integer intAttrVal) {
        assertEquals(intAttrVal, Context.get(INT_ATTR));
        assertEquals(strAttrVal, Context.get(STR_ATTR));
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
