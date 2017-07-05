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

package org.apache.ignite.p2p;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PRecursionTaskSelfTest extends GridCommonAbstractTest {
    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Process one test.
     *
     * @param depMode deployment mode.
     * @throws Exception if error occur.
     */
    private void processTest(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        try {
            Ignite ignite1 = startGrid(1);
            startGrid(2);

            long res = ignite1.compute().execute(FactorialTask.class, 3L);

            assert res == factorial(3);

            res = ignite1.compute().execute(FactorialTask.class, 3L);

            assert res == factorial(3);
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processTest(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processTest(DeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processTest(DeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processTest(DeploymentMode.SHARED);
    }

    /**
     * Calculates factorial.
     *
     * @param num Factorial to calculate.
     * @return Factorial for the number passed in.
     */
    private long factorial(long num) {
        assert num > 0;

        if (num == 1)
            return num;

        return num * factorial(num - 1);
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    public static class FactorialTask extends ComputeTaskAdapter<Long, Long> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Long arg) {
            assert arg > 1;

            Map<FactorialJob, ClusterNode> map = new HashMap<>();

            Iterator<ClusterNode> iter = subgrid.iterator();

            for (int i = 0; i < arg; i++) {
                // Recycle iterator.
                if (iter.hasNext() == false)
                    iter = subgrid.iterator();

                ClusterNode node = iter.next();

                map.put(new FactorialJob(arg - 1), node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Long reduce(List<ComputeJobResult> results) {
            long retVal = 0;

            for (ComputeJobResult res : results)
                retVal += (Long)res.getData();

            return retVal;
        }
    }

    /** */
    private static class FactorialJob extends ComputeJobAdapter {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Constructor.
         * @param arg Argument.
         */
        FactorialJob(Long arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Long execute() {
            Long arg = argument(0);

            assert arg != null;
            assert arg > 0;

            if (arg == 1)
                return 1L;

            ignite.compute().localDeployTask(FactorialTask.class, FactorialTask.class.getClassLoader());

            return ignite.compute().execute(FactorialTask.class, arg);
        }
    }
}
