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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Test task cancellation on grid stop.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal Self")
public class GridCancelOnGridStopSelfTest extends GridCommonAbstractTest {
    /** */
    private static CountDownLatch cnt;

    /** */
    private static boolean cancelCall;

    /** */
    public GridCancelOnGridStopSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCancelingJob() throws Exception {
        cancelCall = false;

        try (Ignite g = startGrid(1)) {
            cnt = new CountDownLatch(1);

            g.compute().executeAsync(CancelledTask.class, null);

            cnt.await();
        }

        assert cancelCall;
    }

    /**
     * Cancelled task.
     */
    private static final class CancelledTask extends ComputeTaskAdapter<String, Void> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable String arg) {
            for (ClusterNode node : subgrid) {
                if (node.id().equals(ignite.configuration().getNodeId())) {
                    return Collections.singletonMap(new ComputeJob() {
                        @Override public void cancel() {
                            cancelCall = true;
                        }

                        @Override public Serializable execute() {
                            cnt.countDown();

                            try {
                                Thread.sleep(Long.MAX_VALUE);
                            }
                            catch (InterruptedException e) {
                                throw new IgniteException(e);
                            }

                            return null;
                        }
                    }, node);
                }
            }

            throw new IgniteException("Local node not found");
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
