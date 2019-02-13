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

package org.apache.ignite.internal.processors.query.h2;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ThreadLocalObjectPoolSelfTest extends GridCommonAbstractTest {
    /** */
    private ThreadLocalObjectPool<Obj> pool = new ThreadLocalObjectPool<>(1, Obj::new, null, null);

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectIsReusedAfterRecycling() throws Exception {
        ThreadLocalObjectPool<Obj>.Reusable r1 = pool.borrow();

        Obj o1 = r1.object();

        r1.recycle();

        ThreadLocalObjectPool<Obj>.Reusable r2 = pool.borrow();

        assertSame(o1, r2.object());
        assertFalse(o1.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBorrowedObjectIsNotReturnedTwice() throws Exception {
        ThreadLocalObjectPool<Obj>.Reusable o1 = pool.borrow();
        ThreadLocalObjectPool<Obj>.Reusable o2 = pool.borrow();

        assertNotSame(o1.object(), o2.object());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectShouldBeClosedOnRecycleIfPoolIsFull() throws Exception {
        ThreadLocalObjectPool<Obj>.Reusable r1 = pool.borrow();
        ThreadLocalObjectPool<Obj>.Reusable r2 = pool.borrow();

        Obj o1 = r1.object();
        Obj o2 = r2.object();

        r1.recycle();
        r2.recycle();

        assertFalse(o1.isClosed());
        assertTrue(o2.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectShouldNotBeRecycledTwice() throws Exception {
        final ThreadLocalObjectPool<Obj>.Reusable r1 = pool.borrow();

        r1.recycle();

        GridTestUtils.assertThrows(log, () -> {
            r1.recycle();

            return null;
        }, AssertionError.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectShouldNotBeReturnedIfPoolIsFull() throws Exception {
        ThreadLocalObjectPool<Obj>.Reusable o1 = pool.borrow();
        ThreadLocalObjectPool<Obj>.Reusable o2 = pool.borrow();

        o1.recycle();

        assertEquals(1, pool.bagSize());

        o2.recycle();

        assertEquals(1, pool.bagSize());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectShouldReturnedToRecyclingThreadBag() throws Exception {
        ThreadLocalObjectPool<Obj>.Reusable o1 = pool.borrow();

        CompletableFuture.runAsync(() -> {
            o1.recycle();

            assertEquals(1, pool.bagSize());
        }).join();

        assertEquals(0, pool.bagSize());
    }

    /** */
    private static class Obj implements AutoCloseable {
        /** */
        private boolean closed = false;

        /** {@inheritDoc} */
        @Override public void close() {
            closed = true;
        }

        /**
         * @return {@code True} if closed.
         */
        public boolean isClosed() {
            return closed;
        }
    }
}
