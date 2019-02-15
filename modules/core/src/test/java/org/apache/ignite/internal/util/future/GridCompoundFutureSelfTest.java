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

package org.apache.ignite.internal.util.future;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests compound future contracts.
 */
@RunWith(JUnit4.class)
public class GridCompoundFutureSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMarkInitialized() throws Exception {
        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>();

        for (int i = 0; i < 5; i++) {
            IgniteInternalFuture<Boolean> part = new GridFinishedFuture<>(true);

            fut.add(part);
        }

        assertFalse(fut.isDone());
        assertFalse(fut.isCancelled());

        fut.markInitialized();

        assertTrue(fut.isDone());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCompleteOnReducer() throws Exception {
        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

        List<GridFutureAdapter<Boolean>> futs = new ArrayList<>(5);

        for (int i = 0; i < 5; i++) {
            GridFutureAdapter<Boolean> part = new GridFutureAdapter<>();

            fut.add(part);

            futs.add(part);
        }

        fut.markInitialized();

        assertFalse(fut.isDone());
        assertFalse(fut.isCancelled());

        for (int i = 0; i < 3; i++) {
            futs.get(i).onDone(true);

            assertFalse(fut.isDone());
        }

        futs.get(3).onDone(false);

        assertTrue(fut.isDone());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCompleteOnException() throws Exception {
        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

        List<GridFutureAdapter<Boolean>> futs = new ArrayList<>(5);

        for (int i = 0; i < 5; i++) {
            GridFutureAdapter<Boolean> part = new GridFutureAdapter<>();

            fut.add(part);

            futs.add(part);
        }

        fut.markInitialized();

        assertFalse(fut.isDone());
        assertFalse(fut.isCancelled());

        for (int i = 0; i < 3; i++) {
            futs.get(i).onDone(true);

            assertFalse(fut.isDone());
        }

        futs.get(3).onDone(new IgniteCheckedException("Test message"));

        assertTrue(fut.isDone());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentCompletion() throws Exception {
        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

        final ConcurrentLinkedDeque<GridFutureAdapter<Boolean>> futs = new ConcurrentLinkedDeque<>();

        for (int i = 0; i < 1000; i++) {
            GridFutureAdapter<Boolean> part = new GridFutureAdapter<>();

            fut.add(part);

            futs.add(part);
        }

        fut.markInitialized();

        IgniteInternalFuture<?> complete = multithreadedAsync(new Runnable() {
            @Override public void run() {
                GridFutureAdapter<Boolean> part;

                while ((part = futs.poll()) != null)
                    part.onDone(true);
            }
        }, 20);

        complete.get();

        assertTrue(fut.isDone());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentRandomCompletion() throws Exception {
        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

        final ConcurrentLinkedDeque<GridFutureAdapter<Boolean>> futs = new ConcurrentLinkedDeque<>();

        for (int i = 0; i < 1000; i++) {
            GridFutureAdapter<Boolean> part = new GridFutureAdapter<>();

            fut.add(part);

            futs.add(part);
        }

        fut.markInitialized();

        IgniteInternalFuture<?> complete = multithreadedAsync(new Runnable() {
            @Override public void run() {
                GridFutureAdapter<Boolean> part;

                Random rnd = new Random();

                while ((part = futs.poll()) != null) {
                    int op = rnd.nextInt(10);

                    if (op < 8)
                        part.onDone(true);
                    else if (op == 8)
                        part.onDone(false);
                    else
                        part.onDone(new IgniteCheckedException("TestMessage"));
                }
            }
        }, 20);

        complete.get();

        assertTrue(fut.isDone());
    }
}
