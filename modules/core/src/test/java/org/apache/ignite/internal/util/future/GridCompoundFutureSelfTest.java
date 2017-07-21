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

package org.apache.ignite.internal.util.future;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * Tests compound future contracts.
 */
public class GridCompoundFutureSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
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
    public void testConcurrentCompletion() throws Exception {
        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

        final ConcurrentLinkedDeque8<GridFutureAdapter<Boolean>> futs =
            new ConcurrentLinkedDeque8<>();

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
    public void testConcurrentRandomCompletion() throws Exception {
        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

        final ConcurrentLinkedDeque8<GridFutureAdapter<Boolean>> futs =
            new ConcurrentLinkedDeque8<>();

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