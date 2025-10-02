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

package org.apache.ignite.thread.context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.thread.context.ThreadContext;
import org.apache.ignite.internal.thread.context.ThreadContextAttribute;
import org.apache.ignite.internal.thread.context.ThreadContextAttributeRegistry;
import org.apache.ignite.internal.thread.context.ThreadContextSnapshot;
import org.apache.ignite.internal.thread.context.pool.ThreadContextAwareThreadPoolExecutor;
import org.apache.ignite.testframework.GridTestUtils;
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
    private static final ThreadContextAttribute<String> STRING_ATTR = ThreadContextAttributeRegistry.instance().register();

    /** */
    private static final ThreadContextAttribute<Integer> INTEGER_ATTR = ThreadContextAttributeRegistry.instance().register(DFLT_INT_VAL);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testThreadScopeAttributes() {
        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, DFLT_STR_VAL).withAttribute(INTEGER_ATTR, DFLT_INT_VAL)) {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            try (Scope ignored1 = ThreadContext.withAttribute(STRING_ATTR, "test")) {
                checkAttributeValues("test", DFLT_INT_VAL);

                try (Scope ignored2 = ThreadContext.withAttribute(INTEGER_ATTR, 1)) {
                    checkAttributeValues("test", 1);
                }

                checkAttributeValues("test", DFLT_INT_VAL);
            }
        }
    }

    /** */
    @Test
    public void testScopeAttributeDuplication() {
        GridTestUtils.assertThrowsWithCause(() -> {
            try (Scope ignored = ThreadContext.withAttribute(STRING_ATTR, "0").withAttribute(STRING_ATTR, "1")) {
                // No-op.
            }
        }, UnsupportedOperationException.class);
    }

    /** */
    @Test
    public void testThreadContextSnapshot() {
        List<ThreadContextSnapshotChecker> checkers = new ArrayList<>();

        checkers.add(ThreadContextSnapshotChecker.create());

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, DFLT_STR_VAL).withAttribute(INTEGER_ATTR, DFLT_INT_VAL)) {
            checkers.add(ThreadContextSnapshotChecker.create());

            try (Scope ignored1 = ThreadContext.withAttribute(STRING_ATTR, "test")) {
                checkers.add(ThreadContextSnapshotChecker.create());

                try (Scope ignored2 = ThreadContext.withAttribute(INTEGER_ATTR, 1)) {
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
    public void testThreadContextAwareThreadPool() throws Exception {
        ThreadContextAwareThreadPoolExecutor pool = new ThreadContextAwareThreadPoolExecutor(
            "test",
            null,
            1,
            1,
            Long.MAX_VALUE,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            null);

        CountDownLatch latch = new CountDownLatch(1);

        pool.submit(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        List<Future<?>> futs = new ArrayList<>();

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, "test").withAttribute(INTEGER_ATTR, 1)) {
            futs.add(pool.submit(() -> checkAttributeValues("test", 1)));
            futs.add(pool.submit(() -> checkAttributeValues("test", 1), 0));
            futs.add(pool.submit(() -> checkAttributeValuesCallable("test", 1)));
        }

        futs.add(pool.submit(() -> checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL)));
        futs.add(pool.submit(() -> checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL), 0));
        futs.add(pool.submit(() -> checkAttributeValuesCallable(DFLT_STR_VAL, DFLT_INT_VAL)));

        latch.countDown();

        for (Future<?> fut : futs)
            fut.get();

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, "test").withAttribute(INTEGER_ATTR, 1)) {
            pool.invokeAny(Arrays.asList(() -> checkAttributeValuesCallable("test", 1)));
            pool.invokeAny(Arrays.asList(() -> checkAttributeValuesCallable("test", 1)), getTestTimeout(), MILLISECONDS);
            pool.invokeAll(Arrays.asList(() -> checkAttributeValuesCallable("test", 1)));
            pool.invokeAll(Arrays.asList(() -> checkAttributeValuesCallable("test", 1)), getTestTimeout(), MILLISECONDS);
        }

        pool.invokeAny(Arrays.asList(() -> checkAttributeValuesCallable(DFLT_STR_VAL, DFLT_INT_VAL)));
        pool.invokeAny(Arrays.asList(
            () -> checkAttributeValuesCallable(DFLT_STR_VAL, DFLT_INT_VAL)),
            getTestTimeout(),
            MILLISECONDS);
        pool.invokeAll(Arrays.asList(() -> checkAttributeValuesCallable(DFLT_STR_VAL, DFLT_INT_VAL)));
        pool.invokeAll(
            Arrays.asList(() -> checkAttributeValuesCallable(DFLT_STR_VAL, DFLT_INT_VAL)),
            getTestTimeout(),
            MILLISECONDS);
    }

    /** */
    private static class ThreadContextSnapshotChecker {
        /** */
        private final ThreadContextSnapshot snapshot;

        /** */
        private final Object strAttrVal;

        /** */
        private final Object intAttrVal;

        /** */
        private ThreadContextSnapshotChecker(ThreadContextSnapshot snapshot, Object strAttrVal, Object intAttrVal) {
            this.snapshot = snapshot;
            this.strAttrVal = strAttrVal;
            this.intAttrVal = intAttrVal;
        }

        /** */
        public void check() {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            try (Scope ignored3 = ThreadContext.withSnapshot(snapshot)) {
                assertEquals(strAttrVal, ThreadContext.get(STRING_ATTR));
                assertEquals(intAttrVal, ThreadContext.get(INTEGER_ATTR));
            }

            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
        }

        /** */
        public static ThreadContextSnapshotChecker create() {
            return new ThreadContextSnapshotChecker(
                ThreadContext.createSnapshot(),
                ThreadContext.get(STRING_ATTR),
                ThreadContext.get(INTEGER_ATTR)
            );
        }
    }

    /** */
    private static Integer checkAttributeValuesCallable(Object strAttrVal, Object intAttrVal) {
        checkAttributeValues(strAttrVal, intAttrVal);

        return 0;
    }

    /** */
    private static void checkAttributeValues(Object strAttrVal, Object intAttrVal) {
        assertEquals(intAttrVal, ThreadContext.get(INTEGER_ATTR));
        assertEquals(strAttrVal, ThreadContext.get(STRING_ATTR));
    }
}
