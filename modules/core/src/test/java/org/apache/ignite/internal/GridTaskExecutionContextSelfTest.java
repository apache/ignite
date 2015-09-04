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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.lang.GridAbsClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@code GridProjection.withXXX(..)} methods.
 */
public class GridTaskExecutionContextSelfTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicInteger CNT = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        CNT.set(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithName() throws Exception {
        IgniteCallable<String> f = new IgniteCallable<String>() {
            @TaskSessionResource
            private ComputeTaskSession ses;

            @Override public String call() {
                return ses.getTaskName();
            }
        };

        Ignite g = grid(0);

        assert "name1".equals(g.compute().withName("name1").call(f));
        assert "name2".equals(g.compute().withName("name2").call(f));
        assert f.getClass().getName().equals(g.compute().call(f));

        assert "name1".equals(g.compute().withName("name1").execute(new TestTask(false), null));
        assert "name2".equals(g.compute().withName("name2").execute(new TestTask(false), null));
        assert TestTask.class.getName().equals(g.compute().execute(new TestTask(false), null));
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithNoFailoverClosure() throws Exception {
        final IgniteRunnable r = new GridAbsClosureX() {
            @Override public void applyx() {
                CNT.incrementAndGet();

                throw new ComputeExecutionRejectedException("Expected error.");
            }
        };

        final Ignite g = grid(0);

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    g.compute().withNoFailover().run(r);

                    return null;
                }
            },
            ComputeExecutionRejectedException.class,
            "Expected error."
        );

        assertEquals(1, CNT.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithNoFailoverTask() throws Exception {
        final Ignite g = grid(0);

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    g.compute().withNoFailover().execute(new TestTask(true), null);

                    return null;
                }
            },
            ComputeExecutionRejectedException.class,
            "Expected error."
        );

        assertEquals(1, CNT.get());
    }

    /**
     * Test task that returns its name.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<Void, String> {
        /** */
        private final boolean fail;

        /**
         * @param fail Whether to fail.
         */
        private TestTask(boolean fail) {
            this.fail = fail;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) {
            return F.asSet(new ComputeJobAdapter() {
                @TaskSessionResource
                private ComputeTaskSession ses;

                @Override public Object execute() {
                    CNT.incrementAndGet();

                    if (fail)
                        throw new ComputeExecutionRejectedException("Expected error.");

                    return ses.getTaskName();
                }
            });
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<ComputeJobResult> results) {
            return F.first(results).getData();
        }
    }
}