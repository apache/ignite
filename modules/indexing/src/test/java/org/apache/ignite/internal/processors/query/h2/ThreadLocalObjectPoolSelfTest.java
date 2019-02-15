/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.processors.query.h2.ThreadLocalObjectPool.Reusable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class ThreadLocalObjectPoolSelfTest extends GridCommonAbstractTest {
    /** */
    private ThreadLocalObjectPool<Obj> pool = new ThreadLocalObjectPool<>(Obj::new, 1);

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
    public void testBorrowedObjectIsNotReturnedTwice() throws Exception {
        Reusable<Obj> o1 = pool.borrow();
        Reusable<Obj> o2 = pool.borrow();

        assertNotSame(o1.object(), o2.object());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectShouldBeClosedOnRecycleIfPoolIsFull() throws Exception {
        Reusable<Obj> o1 = pool.borrow();
        Reusable<Obj> o2 = pool.borrow();
        o1.recycle();
        o2.recycle();

        assertTrue(o2.object().isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectShouldNotBeReturnedIfPoolIsFull() throws Exception {
        Reusable<Obj> o1 = pool.borrow();
        Reusable<Obj> o2 = pool.borrow();

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
        Reusable<Obj> o1 = pool.borrow();

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
