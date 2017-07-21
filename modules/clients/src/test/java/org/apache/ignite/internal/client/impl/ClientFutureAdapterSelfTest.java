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

package org.apache.ignite.internal.client.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFuture;
import org.apache.ignite.internal.client.GridClientFutureTimeoutException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Grid client future implementation self test.
 */
public class ClientFutureAdapterSelfTest extends GridCommonAbstractTest {
    /**
     * Test finished futures.
     */
    public void testFinished() {
        GridClientFutureAdapter<Integer> fut = new GridClientFutureAdapter<>();

        assertFalse(fut.isDone());

        fut.onDone(0);

        assertTrue(fut.isDone());
        assertTrue(new GridClientFutureAdapter<>(0).isDone());
        assertTrue(new GridClientFutureAdapter<Integer>(new GridClientException("Test grid exception.")).isDone());
        assertTrue(new GridClientFutureAdapter<Integer>(new RuntimeException("Test runtime exception.")).isDone());
    }

    /**
     * Test chained futures behaviour.
     *
     * @throws org.apache.ignite.internal.client.GridClientException On any exception.
     */
    public void testChains() throws GridClientException {
        // Synchronous notifications.
        testChains(1, 100);
        testChains(10, 10);
        testChains(100, 1);
        testChains(1000, 0);
    }

    /**
     * Test chained future in certain conditions.
     *
     * @param chainSize Futures chain size.
     * @param waitDelay Delay to wait each future in the chain becomes done.
     * @throws GridClientException In case of any exception
     */
    private void testChains(int chainSize, long waitDelay) throws GridClientException {
        /* Base future to chain from. */
        GridClientFutureAdapter<Integer> fut = new GridClientFutureAdapter<>();

        /* Collection of chained futures: fut->chained[0]->chained[1]->...->chained[chainSize - 1] */
        List<GridClientFutureAdapter<Integer>> chained = new ArrayList<>();

        GridClientFutureAdapter<Integer> cur = fut;

        for (int i = 0; i < chainSize; i++) {
            cur = cur.chain(new GridClientFutureCallback<Integer, Integer>() {
                @Override public Integer onComplete(GridClientFuture<Integer> f) throws GridClientException {
                    assertTrue("Expects callback future is finished.", f.isDone());

                    return f.get() + 1;
                }
            });

            chained.add(cur);
        }

        long start;

        /* Validate not-finished futures in chain. */
        for (GridClientFuture<Integer> f : chained) {
            assertFalse(f.isDone());

            start = System.currentTimeMillis();

            try {
                f.get(waitDelay, TimeUnit.MILLISECONDS);

                fail("Expects chained future not finished yet.");
            }
            catch (GridClientFutureTimeoutException ignore) {
                /* No op: expects chained future not finished yet. */
            }

            assertTrue(System.currentTimeMillis() - start >= waitDelay);
        }

        /* Calculate 'count' chained futures time consumption. */
        start = System.currentTimeMillis();

        fut.onDone(0);
        assertEquals("Check chain-based increment value.", chainSize, chained.get(chainSize - 1).get().intValue());

        info("Time consumption for " + chainSize + " chained futures: " + (System.currentTimeMillis() - start));
    }
}