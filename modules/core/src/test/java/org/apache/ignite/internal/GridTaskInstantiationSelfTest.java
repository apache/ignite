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