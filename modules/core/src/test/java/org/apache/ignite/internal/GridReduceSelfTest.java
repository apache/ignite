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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.util.typedef.R1;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test reduce with long operations.
 */
public class GridReduceSelfTest extends GridCommonAbstractTest {
    /** Number of nodes in the grid. */
    private static final int GRID_CNT = 3;

    /**
     * @throws Exception If failed.
     */
    public void testReduce() throws Exception {
        startGrids(GRID_CNT);

        try {
            Ignite ignite = grid(0);

            assert ignite.cluster().nodes().size() == GRID_CNT;

            List<ReducerTestClosure> closures = closures(ignite.cluster().nodes().size());

            Long res = compute(ignite.cluster().forLocal()).call(closures, new R1<Long, Long>() {
                private long sum;

                @Override public boolean collect(Long e) {
                    info("Got result from closure: " + e);

                    sum += e;

                    // Stop collecting on value 1.
                    return e != 1;
                }

                @Override public Long reduce() {
                    return sum;
                }
            });

            assertEquals((Long)1L, res);

            assertTrue(closures.get(0).isFinished);

            for (int i = 1; i < closures.size(); i++)
                assertFalse("Closure #" + i + " is not interrupted.", closures.get(i).isFinished);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReduceAsync() throws Exception {
        startGrids(GRID_CNT);

        try {
            Ignite ignite = grid(0);

            assert ignite.cluster().nodes().size() == GRID_CNT;

            List<ReducerTestClosure> closures = closures(ignite.cluster().nodes().size());

            IgniteCompute comp = compute(ignite.cluster().forLocal()).withAsync();

            comp.call(closures, new R1<Long, Long>() {
                private long sum;

                @Override public boolean collect(Long e) {
                    info("Got result from closure: " + e);

                    sum += e;

                    // Stop collecting on value 1.
                    return e != 1;
                }

                @Override public Long reduce() {
                    return sum;
                }
            });

            ComputeTaskFuture<Long> fut = comp.future();

            assertEquals((Long)1L, fut.get());

            assertTrue(closures.get(0).isFinished);

            for (int i = 1; i < closures.size(); i++)
                assertFalse("Closure #" + i + " is not interrupted.", closures.get(i).isFinished);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param size Number of closures.
     * @return Collection of closures.
     */
    private static List<ReducerTestClosure> closures(int size) {
        assert size > 1;

        List<ReducerTestClosure> cls = new ArrayList<>(size);

        cls.add(new ReducerTestClosure(true)); // Fast closure.

        for (int i = 1; i < size; i++)
            cls.add(new ReducerTestClosure(false)); // Normal closures.

        return cls;
    }

    /**
     * Closure for testing reducer.
     */
    @SuppressWarnings("PackageVisibleField")
    private static class ReducerTestClosure implements IgniteCallable<Long> {
        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Test flag to check the thread was interrupted. */
        volatile boolean isFinished;

        /** Fast or normal closure. */
        private boolean fast;

        /**
         * @param fast Fast or normal closure.
         */
        ReducerTestClosure(boolean fast) {
            this.fast = fast;
        }

        /** {@inheritDoc} */
        @Override public Long call() {
            try {
                try {
                    if (fast) {
                        Thread.sleep(500);

                        log.info("Returning 1 from fast closure.");

                        return 1L;
                    }
                    else {
                        Thread.sleep(5000);

                        log.info("Returning 2 from normal closure.");

                        return 2L;
                    }
                }
                finally {
                    isFinished = true;
                }
            }
            catch (InterruptedException ignore) {
                log.info("Returning 0 from interrupted closure.");

                return 0L;
            }
        }
    }
}