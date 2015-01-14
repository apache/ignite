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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Task deployment tests.
 */
public class GridDeploymentMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int THREAD_CNT = 20;

    /** */
    private static final int EXEC_CNT = 30000;

    /**
     * @throws Exception If failed.
     */
    public void testDeploy() throws Exception {
        try {
            final Ignite ignite = startGrid(0);

            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

            final CyclicBarrier barrier = new CyclicBarrier(THREAD_CNT, new Runnable() {
                private int iterCnt;

                @Override public void run() {
                    try {
                        ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

                        assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) == null;

                        if (++iterCnt % 100 == 0)
                            info("Iterations count: " + iterCnt);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to undeploy task message.", e);

                        fail("See logs for details.");
                    }
                }
            });

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try {
                        for (int i = 0; i < EXEC_CNT; i++) {
                            barrier.await(2000, MILLISECONDS);

                            ignite.compute().localDeployTask(GridDeploymentTestTask.class,
                                GridDeploymentTestTask.class.getClassLoader());

                            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;
                        }
                    }
                    catch (Exception e) {
                        U.error(log, "Test failed.", e);

                        throw e;
                    }
                    finally {
                        info("Thread finished.");
                    }

                    return null;
                }
            }, THREAD_CNT, "grid-load-test-thread");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test task.
     */
    private static class GridDeploymentTestTask extends ComputeTaskAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws IgniteCheckedException {
            assert false;

            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }
}
