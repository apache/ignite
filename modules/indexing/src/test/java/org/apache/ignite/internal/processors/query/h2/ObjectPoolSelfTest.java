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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class ObjectPoolSelfTest extends GridCommonAbstractTest {
    /** */
    private ObjectPool<Obj> pool = new ObjectPool<>(Obj::new, 1, null, null);

    /**
     * @throws Exception If failed.
     */
    public void testObjectIsReusedAfterRecycling() throws Exception {
        ObjectPool.Reusable<Obj> r1 = pool.borrow();

        Obj o1 = r1.object();

        r1.recycle();

        ObjectPool.Reusable<Obj> r2 = pool.borrow();

        Obj o2 = r2.object();

        assertSame(o1, o2);

        assertFalse(o1.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    public void testBorrowedObjectIsNotReturnedTwice() throws Exception {
        ObjectPool.Reusable<Obj> r1 = pool.borrow();
        ObjectPool.Reusable<Obj> r2 = pool.borrow();

        assertNotSame(r1.object(), r2.object());
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectShouldBeClosedOnRecycleIfPoolIsFull() throws Exception {
        ObjectPool.Reusable<Obj> r1 = pool.borrow();
        ObjectPool.Reusable<Obj> r2 = pool.borrow();

        Obj o2 = r2.object();

        r1.recycle();
        r2.recycle();

        assertNull(r1.object());
        assertNull(r2.object());

        assertTrue(o2.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectShouldNotBeReturnedIfPoolIsFull() throws Exception {
        ObjectPool.Reusable<Obj> r1 = pool.borrow();
        ObjectPool.Reusable<Obj> r2 = pool.borrow();

        r1.recycle();

        assertEquals(1, pool.bagSize());

        r2.recycle();

        assertEquals(1, pool.bagSize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectShouldReturnedToBag() throws Exception {
        ObjectPool.Reusable<Obj> r1 = pool.borrow();

        CompletableFuture.runAsync(() -> {
            r1.recycle();

            assertEquals(1, pool.bagSize());
        }).join();

        assertEquals(1, pool.bagSize());
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
