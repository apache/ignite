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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.GridTestTask;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDeploymentException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

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

    /** */
    protected boolean peerClassLoadingEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(peerClassLoadingEnabled());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        startGrid(2);
        startGrid(3);
    }

    /**
     *  {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSynchronousExecute() throws Exception {
        ComputeTaskFuture<?> fut = ignite.compute().executeAsync(GridTestTask.class,  "testArg");

        assert fut != null;

        info("Task result: " + fut.get());
    }

    /**
     * Test for https://issues.apache.org/jira/browse/IGNITE-1384
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJobIdCollision() throws Exception {
        fail("Test refactoring is needed: https://issues.apache.org/jira/browse/IGNITE-4706");

        long locId = IgniteUuid.lastLocalId();

        ArrayList<IgniteFuture<Object>> futs = new ArrayList<>(2016);

        IgniteCompute compute = grid(1).compute(grid(1).cluster().forNodeId(grid(3).localNode().id()));

        for (int i = 0; i < 1000; i++) {
            futs.add(compute.callAsync(new IgniteCallable<Object>() {
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
            }));
        }

        info("Finished first loop.");

        AtomicLong idx = U.field(IgniteUuid.class, "cntGen");

        idx.set(locId);

        IgniteCompute compute1 = grid(2).compute(grid(2).cluster().forNodeId(grid(3).localNode().id()));

        for (int i = 0; i < 100; i++) {
            futs.add(compute1.callAsync(new IgniteCallable<Object>() {
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
            }));
        }

        for (IgniteFuture<Object> fut : futs)
            fut.get();
    }

    /**
     * Test execution of non-existing task by name IGNITE-4838.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteTaskWithInvalidName() throws Exception {
        try {
            ComputeTaskFuture<?> fut = ignite.compute().execute("invalid.task.name", null);

            fut.get();

            assert false : "Should never be reached due to exception thrown.";
        }
        catch (IgniteDeploymentException e) {
            info("Received correct exception: " + e);
        }
    }
}
