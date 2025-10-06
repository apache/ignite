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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.thread.context.pool.ThreadContextAwareThreadPoolExecutor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.context.concurrent.ThreadContextAwareCompletableFuture;
import org.junit.Test;

/** */
public class ThreadContextAttributesTest extends GridCommonAbstractTest {
    /** */
    private static final String DFLT_STIRNG_ATTR_VAL = "default";

    /** */
    private static final Integer DFLT_INTEGER_ATTR_VAL = 0;

    /** */
    private static final ThreadContextAttribute<String> STRING_ATTR = ThreadContextAttributeRegistry.instance().register(DFLT_STIRNG_ATTR_VAL);

    /** */
    private static final ThreadContextAttribute<Integer> INTEGER_ATTR = ThreadContextAttributeRegistry.instance().register(DFLT_INTEGER_ATTR_VAL);

    /** */
    @Test
    public void testBasic() {
        assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
        assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, DFLT_STIRNG_ATTR_VAL).withAttribute(INTEGER_ATTR, DFLT_INTEGER_ATTR_VAL)) {
            assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
            assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));

            try (Scope ignored1 = ThreadContext.withAttribute(STRING_ATTR, "hello")) {
                assertEquals("hello", ThreadContext.get(STRING_ATTR));
                assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));

                try (Scope ignored2 = ThreadContext.withAttribute(INTEGER_ATTR, 1)) {
                    assertEquals("hello", ThreadContext.get(STRING_ATTR));
                    assertEquals((Integer)1, ThreadContext.get(INTEGER_ATTR));
                }

                assertEquals("hello", ThreadContext.get(STRING_ATTR));
                assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
            }
        }

        assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
        assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
    }

    /** */
    @Test
    public void testDuplication() {
        GridTestUtils.assertThrowsWithCause(() -> {
            try (Scope ignored = ThreadContext.withAttribute(STRING_ATTR, "hello0").withAttribute(STRING_ATTR, "hello1")) {
                // No-op.
            }
        }, UnsupportedOperationException.class);

    }

    /** */
    @Test
    public void testDefer() {
        AtomicInteger cntr = new AtomicInteger();

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, "hello").defer(cntr::incrementAndGet).withAttribute(INTEGER_ATTR, 1)) {
            assertEquals("hello", ThreadContext.get(STRING_ATTR));
            assertEquals((Integer)1, ThreadContext.get(INTEGER_ATTR));
        }

        assertEquals(1, cntr.get());
    }

    /** */
    @Test
    public void testDeferDuplication() {
        AtomicInteger cntr = new AtomicInteger();

        GridTestUtils.assertThrowsWithCause(() -> {
            try (Scope ignored = ThreadContext.withAttribute(STRING_ATTR, "hello0").defer(cntr::incrementAndGet).defer(cntr::incrementAndGet)) {
                // No-op.
            }
        }, UnsupportedOperationException.class);
    }

    /** */
    @Test
    public void testSnapshot() {
        ThreadContextSnapshot snapshot0 = ThreadContext.createSnapshot();
        ThreadContextSnapshot snapshot1;
        ThreadContextSnapshot snapshot2;
        ThreadContextSnapshot snapshot3;
        ThreadContextSnapshot snapshot4;
        ThreadContextSnapshot snapshot5;
        ThreadContextSnapshot snapshot6;

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, DFLT_STIRNG_ATTR_VAL).withAttribute(INTEGER_ATTR, DFLT_INTEGER_ATTR_VAL)) {
            snapshot1 = ThreadContext.createSnapshot();

            try (Scope ignored1 = ThreadContext.withAttribute(STRING_ATTR, "hello")) {
                snapshot2 = ThreadContext.createSnapshot();

                try (Scope ignored2 = ThreadContext.withAttribute(INTEGER_ATTR, 1)) {
                    snapshot3 = ThreadContext.createSnapshot();
                }

                snapshot4 = ThreadContext.createSnapshot();
            }

            snapshot5 = ThreadContext.createSnapshot();
        }

        snapshot6 = ThreadContext.createSnapshot();

        try (Scope ignored0 = ThreadContext.withSnapshot(snapshot0)) {
            assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
            assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
        }

        try (Scope ignored1 = ThreadContext.withSnapshot(snapshot1)) {
            assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
            assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
        }

        try (Scope ignored2 = ThreadContext.withSnapshot(snapshot2)) {
            assertEquals("hello", ThreadContext.get(STRING_ATTR));
            assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
        }

        try (Scope ignored3 = ThreadContext.withSnapshot(snapshot3)) {
            assertEquals("hello", ThreadContext.get(STRING_ATTR));
            assertEquals((Integer)1, ThreadContext.get(INTEGER_ATTR));
        }

        try (Scope ignored4 = ThreadContext.withSnapshot(snapshot4)) {
            assertEquals("hello", ThreadContext.get(STRING_ATTR));
            assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
        }

        try (Scope ignored5 = ThreadContext.withSnapshot(snapshot5)) {
            assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
            assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
        }

        try (Scope ignored6 = ThreadContext.withSnapshot(snapshot6)) {
            assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
            assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
        }
    }

    /** */
    @Test
    public void testIgniteFutureContextPropagation() throws Exception {
        AtomicReference<Exception> ex = new AtomicReference<>();

        GridFutureAdapter<String> fut = new GridFutureAdapter<>();
        IgniteInternalFuture<String> chainFut;

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, "hello").withAttribute(INTEGER_ATTR, 1)) {
            fut.listen(() -> {
                assertEquals("hello", ThreadContext.get(STRING_ATTR));
                assertEquals((Integer)1, ThreadContext.get(INTEGER_ATTR));
            });
        }

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, "bob").withAttribute(INTEGER_ATTR, 2)) {
            chainFut = fut.chain(() -> {
                assertEquals("bob", ThreadContext.get(STRING_ATTR));
                assertEquals((Integer)2, ThreadContext.get(INTEGER_ATTR));

                return "2";
            }).chain(() -> {
                assertEquals("bob", ThreadContext.get(STRING_ATTR));
                assertEquals((Integer)2, ThreadContext.get(INTEGER_ATTR));

                return "2";
            });
        }

        new Thread(() -> {
            try {
                assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
                assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));

                fut.onDone("1");

                assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
                assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
            }
            catch (Exception e) {
                ex.set(e);
            }
        }).start();

        assertEquals("1", fut.get(getTestTimeout()));
        assertEquals("2", chainFut.get(getTestTimeout()));

        assertNull(ex.get());
    }

    /** */
    @Test
    public void testCopmpletableFutureContextPropagation() throws Exception {
        AtomicReference<Exception> ex = new AtomicReference<>();

        CompletableFuture<String> fut = new ThreadContextAwareCompletableFuture<>();
        CompletableFuture<Void> runFut;
        CompletableFuture<String> chainFut;

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, "hello").withAttribute(INTEGER_ATTR, 1)) {
            runFut = fut.thenRun(() -> {
                assertEquals("hello", ThreadContext.get(STRING_ATTR));
                assertEquals((Integer)1, ThreadContext.get(INTEGER_ATTR));
            }).thenRun(() -> {
                assertEquals("hello", ThreadContext.get(STRING_ATTR));
                assertEquals((Integer)1, ThreadContext.get(INTEGER_ATTR));
            });
        }

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, "bob").withAttribute(INTEGER_ATTR, 2)) {
            chainFut = fut.thenCompose(v -> {
                assertEquals("bob", ThreadContext.get(STRING_ATTR));
                assertEquals((Integer)2, ThreadContext.get(INTEGER_ATTR));

                return CompletableFuture.completedFuture("2") ;
            }).thenCompose(v -> {
                assertEquals("bob", ThreadContext.get(STRING_ATTR));
                assertEquals((Integer)2, ThreadContext.get(INTEGER_ATTR));

                return CompletableFuture.completedFuture("2") ;
            });
        }

        new Thread(() -> {
            try {
                assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
                assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));

                fut.complete("1");

                assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
                assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
            }
            catch (Exception e) {
                ex.set(e);
            }
        }).start();

        runFut.get();

        assertEquals("1", fut.get(getTestTimeout(), TimeUnit.MILLISECONDS));
        assertEquals("2", chainFut.get(getTestTimeout(), TimeUnit.MILLISECONDS));

        assertNull(ex.get());
    }

    /** */
    @Test
    public void testTreadPool() throws Exception {
        ThreadContextAwareThreadPoolExecutor pool = new ThreadContextAwareThreadPoolExecutor(
            "test",
            null,
            1,
            1,
            Long.MAX_VALUE,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            null);

        CountDownLatch latch = new CountDownLatch(2);

        pool.submit(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Future<?> fut0;

        try (Scope ignored0 = ThreadContext.withAttribute(STRING_ATTR, "hello").withAttribute(INTEGER_ATTR, 1)) {
            fut0 = pool.submit(() -> {
                assertEquals("hello", ThreadContext.get(STRING_ATTR));
                assertEquals((Integer)1, ThreadContext.get(INTEGER_ATTR));
            });

            latch.countDown();
        }

        Future<?>  fut1 = pool.submit(() -> {
            assertEquals(DFLT_STIRNG_ATTR_VAL, ThreadContext.get(STRING_ATTR));
            assertEquals(DFLT_INTEGER_ATTR_VAL, ThreadContext.get(INTEGER_ATTR));
        });

        latch.countDown();

        fut0.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        fut1.get(getTestTimeout(), TimeUnit.MILLISECONDS);
    }
}

