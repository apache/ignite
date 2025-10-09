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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
    }

    /** */
    @Test
    public void testScopeAttributeDuplication() {
        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

        try (Scope ignored = ThreadContext.withAttribute(INT_ATTR, 1).withAttribute(INT_ATTR, 2)) {
            checkAttributeValues(DFLT_STR_VAL, 2);
        }

        checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);
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

        CountDownLatch poolUnblockedLatch = blockPool(pool);

        String strAttrVal = "test";
        int intAttrVal = 1;

        List<Future<?>> futs = new ArrayList<>();

        try (Scope ignored0 = ThreadContext.withAttribute(STR_ATTR, strAttrVal).withAttribute(INT_ATTR, intAttrVal)) {
            futs.add(pool.submit(() -> checkAttributeValues(strAttrVal, intAttrVal)));
            futs.add(pool.submit(() -> checkAttributeValues(strAttrVal, intAttrVal), 0));
            futs.add(pool.submit(() -> attributeValuesCheckCallable(strAttrVal, intAttrVal)));
        }

        futs.add(pool.submit(() -> checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL)));
        futs.add(pool.submit(() -> checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL), 0));
        futs.add(pool.submit(() -> attributeValuesCheckCallable(DFLT_STR_VAL, DFLT_INT_VAL)));

        poolUnblockedLatch.countDown();

        for (Future<?> fut : futs)
            fut.get();

        try (Scope ignored0 = ThreadContext.withAttribute(STR_ATTR, strAttrVal).withAttribute(INT_ATTR, intAttrVal)) {
            pool.invokeAny(List.of(() -> attributeValuesCheckCallable(strAttrVal, intAttrVal)));
            pool.invokeAny(List.of(() -> attributeValuesCheckCallable(strAttrVal, intAttrVal)), getTestTimeout(), MILLISECONDS);
            pool.invokeAll(List.of(() -> attributeValuesCheckCallable(strAttrVal, intAttrVal)));
            pool.invokeAll(List.of(() -> attributeValuesCheckCallable(strAttrVal, intAttrVal)), getTestTimeout(), MILLISECONDS);
        }

        pool.invokeAny(List.of(() -> attributeValuesCheckCallable(DFLT_STR_VAL, DFLT_INT_VAL)));
        pool.invokeAny(
            List.of(() -> attributeValuesCheckCallable(DFLT_STR_VAL, DFLT_INT_VAL)),
            getTestTimeout(),
            MILLISECONDS);
        pool.invokeAll(List.of(() -> attributeValuesCheckCallable(DFLT_STR_VAL, DFLT_INT_VAL)));
        pool.invokeAll(
            List.of(() -> attributeValuesCheckCallable(DFLT_STR_VAL, DFLT_INT_VAL)),
            getTestTimeout(),
            MILLISECONDS);
    }

    /** */
    @Test
    public void testThreadContextAwareStripedThreadPoolExecutor() throws Exception {
        ThreadContextAwareStripedThreadPoolExecutor pool = new ThreadContextAwareStripedThreadPoolExecutor(
            2,
            getTestIgniteInstanceName(0),
            "",
            (t, e) -> log.error("", e),
            false,
            0
        );

        String strAttrVal = "test";
        int intAttrVal = 1;

        CountDownLatch latch = new CountDownLatch(2);

        try (Scope ignored0 = ThreadContext.withAttribute(STR_ATTR, strAttrVal).withAttribute(INT_ATTR, intAttrVal)) {
            pool.execute(() -> {
                checkAttributeValues(strAttrVal, intAttrVal);

                latch.countDown();

            }, 1);
        }

        pool.execute(() -> {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            latch.countDown();
        }, 1);

        assertTrue(latch.await(5000, MILLISECONDS));
    }

    /** */
    @Test
    public void testThreadContextAwareStripedExecutor() throws Exception {
        ThreadContextAwareStripedExecutor pool = new ThreadContextAwareStripedExecutor(
            2,
            getTestIgniteInstanceName(0),
            "",
            log,
            e -> {},
            false,
            null,
            getTestTimeout()
        );

        String strAttrVal = "test";
        int intAttrVal = 1;

        CountDownLatch latch = new CountDownLatch(4);

        try (Scope ignored0 = ThreadContext.withAttribute(STR_ATTR, strAttrVal).withAttribute(INT_ATTR, intAttrVal)) {
            pool.execute(() -> {
                checkAttributeValues(strAttrVal, intAttrVal);

                latch.countDown();

            });

            pool.execute(1, () -> {
                checkAttributeValues(strAttrVal, intAttrVal);

                latch.countDown();

            });
        }

        pool.execute(() -> {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            latch.countDown();
        });

        pool.execute(1, () -> {
            checkAttributeValues(DFLT_STR_VAL, DFLT_INT_VAL);

            latch.countDown();
        });

        assertTrue(latch.await(5000, MILLISECONDS));
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
    private static Integer attributeValuesCheckCallable(String strAttrVal, Integer intAttrVal) {
        checkAttributeValues(strAttrVal, intAttrVal);

        return 0;
    }

    /** */
    private static void checkAttributeValues(String strAttrVal, Integer intAttrVal) {
        assertEquals(intAttrVal, ThreadContext.get(INT_ATTR));
        assertEquals(strAttrVal, ThreadContext.get(STR_ATTR));
    }
}
