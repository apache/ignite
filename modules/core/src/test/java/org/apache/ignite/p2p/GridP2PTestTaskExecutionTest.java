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

package org.apache.ignite.p2p;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test executes GridP2PTestTask on the remote node.
 * Before running of test you MUST start at least one remote node.
 */
public final class GridP2PTestTaskExecutionTest extends GridCommonAbstractTest {
    /**
     * Method executes GridP2PTestTask.
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testGridP2PTestTask() throws IgniteCheckedException {
        try (Ignite g  = G.start()) {
            assert g != null;

            assert !g.cluster().forRemotes().nodes().isEmpty() : "Test requires at least 1 remote node.";

            /* Execute GridP2PTestTask. */
            ComputeTaskFuture<Integer> fut = executeAsync(g.compute(), GridP2PTestTask.class, 1);

            /* Wait for task completion. */
            Integer res = fut.get();

            X.println("Result of execution is: " + res);

            assert res > 0 : "Result of execution is: " + res + " for more information see GridP2PTestJob";
        }
    }
}
