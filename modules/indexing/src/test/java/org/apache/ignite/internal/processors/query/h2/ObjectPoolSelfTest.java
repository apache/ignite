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

import org.apache.ignite.internal.processors.query.h2.ObjectPool.Reusable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class ObjectPoolSelfTest extends GridCommonAbstractTest {
    /** */
    private ObjectPool<Obj> pool = new ObjectPool<>(Obj::new, 1);

    /**
     * @throws Exception If failed.
     */
    public void testObjectIsReusedAfterRecycling() throws Exception {
        Reusable<Obj> o1 = pool.borrow();
        o1.recycle();
        Reusable<Obj> o2 = pool.borrow();

        assertSame(o1.object(), o2.object());
        assertFalse(o1.object().isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    public void testBorrowedObjectIsNotReturnedTwice() throws Exception {
        Reusable<Obj> o1 = pool.borrow();
        Reusable<Obj> o2 = pool.borrow();

        assertNotSame(o1.object(), o2.object());
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectShouldBeClosedOnRecycleIfPoolIsFull() throws Exception {
        Reusable<Obj> o1 = pool.borrow();
        Reusable<Obj> o2 = pool.borrow();
        o1.recycle();
        o2.recycle();

        assertTrue(o2.object().isClosed());
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