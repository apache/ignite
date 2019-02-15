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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests instantiation of various task types (defined as private inner class, without default constructor, non-public
 * default constructor).
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskInstantiationSelfTest extends GridCommonAbstractTest {
    /**
     * Constructor.
     */
    public GridTaskInstantiationSelfTest() {
        super(true);
    }

    /**
     * @throws Exception If an error occurs.
     */
    @Test
    public void testTasksInstantiation() throws Exception {
        grid().compute().execute(PrivateClassTask.class, null);

        grid().compute().execute(NonPublicDefaultConstructorTask.class, null);

        try {
            grid().compute().execute(NoDefaultConstructorTask.class, null);

            assert false : "Exception should have been thrown.";
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * Test task defined as private inner class.
     */
    private static class PrivateClassTask extends ComputeTaskAdapter<String, Object> {
        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable String arg) {
            for (ClusterNode node : subgrid)
                if (node.id().equals(ignite.configuration().getNodeId()))
                    return Collections.singletonMap(new ComputeJobAdapter() {
                        @Override public Serializable execute() {
                            return null;
                        }
                    }, node);

            throw new IgniteException("Local node not found.");
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Test task defined with non-public default constructor.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class NonPublicDefaultConstructorTask extends PrivateClassTask {
        /**
         * No-op constructor.
         */
        private NonPublicDefaultConstructorTask() {
            // No-op.
        }
    }

    /**
     * Test task defined without default constructor.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class NoDefaultConstructorTask extends PrivateClassTask {
        /**
         * No-op constructor.
         *
         * @param param Some parameter.
         */
        @SuppressWarnings({"unused"})
        private NoDefaultConstructorTask(Object param) {
            // No-op.
        }
    }
}
