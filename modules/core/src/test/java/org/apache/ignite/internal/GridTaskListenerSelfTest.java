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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * This test checks that GridTaskListener is only called once per task.
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskListenerSelfTest extends GridCommonAbstractTest {
    /** */
    public GridTaskListenerSelfTest() {
        super(/*start grid*/true);
    }

    /**
     * Checks that GridTaskListener is only called once per task.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"BusyWait"})
    @Test
    public void testGridTaskListener() throws Exception {
        final AtomicInteger cnt = new AtomicInteger(0);

        IgniteInClosure<IgniteFuture<?>> lsnr = new CI1<IgniteFuture<?>>() {
            @Override public void apply(IgniteFuture<?> fut) {
                assert fut != null;

                cnt.incrementAndGet();
            }
        };

        Ignite ignite = G.ignite(getTestIgniteInstanceName());

        assert ignite != null;

        ignite.compute().localDeployTask(TestTask.class, TestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), TestTask.class.getName(), null);

        fut.listen(lsnr);

        fut.get();

        while (cnt.get() == 0) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                error("Got interrupted while sleep.", e);

                break;
            }
        }

        assert cnt.get() == 1 : "Unexpected GridTaskListener apply count [count=" + cnt.get() + ", expected=1]";
    }

    /** Test task. */
    private static class TestTask extends ComputeTaskSplitAdapter<Serializable, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Serializable arg) {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                jobs.add(new ComputeJobAdapter() {
                    @Override public Serializable execute() {
                        return 1;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
