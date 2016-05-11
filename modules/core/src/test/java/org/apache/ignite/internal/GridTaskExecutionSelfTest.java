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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.GridTestTask;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Task execution test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskExecutionSelfTest extends GridCommonAbstractTest {
    /** Grid instance. */
    private Ignite ignite;

    /** */
    public GridTaskExecutionSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid(1);

        startGrid(2);
        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid(1);
        stopGrid(2);
        stopGrid(3);

        ignite = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousExecute() throws Exception {
        IgniteCompute comp = ignite.compute().withAsync();

        assertNull(comp.execute(GridTestTask.class,  "testArg"));

        ComputeTaskFuture<?> fut = comp.future();

        assert fut != null;

        info("Task result: " + fut.get());
    }

    /**
     * Test for https://issues.apache.org/jira/browse/IGNITE-1384
     *
     * @throws Exception If failed.
     */
    public void testJobIdCollision() throws Exception {
        long locId = IgniteUuid.lastLocalId();

        ArrayList<IgniteFuture<Object>> futs = new ArrayList<>(2016);

        IgniteCompute compute = grid(1).compute(grid(1).cluster().forNodeId(grid(3).localNode().id())).withAsync();

        for (int i = 0; i < 1000; i++) {
            compute.call(new IgniteCallable<Object>() {
                @JobContextResource
                ComputeJobContext ctx;

                boolean held;

                @Override public Object call() throws Exception {
                    if (!held) {
                        ctx.holdcc(1000);

                        held = true;
                    }

                    return null;
                }
            });

            futs.add(compute.future());
        }

        info("Finished first loop.");

        AtomicLong idx = U.field(IgniteUuid.class, "cntGen");

        idx.set(locId);

        IgniteCompute compute1 = grid(2).compute(grid(2).cluster().forNodeId(grid(3).localNode().id())).withAsync();

        for (int i = 0; i < 100; i++) {
            compute1.call(new IgniteCallable<Object>() {
                @JobContextResource
                ComputeJobContext ctx;

                boolean held;

                @Override public Object call() throws Exception {
                    if (!held) {
                        ctx.holdcc(1000);

                        held = true;
                    }

                    return null;
                }
            });

            futs.add(compute1.future());
        }

        for (IgniteFuture<Object> fut : futs)
            fut.get();
    }
}
