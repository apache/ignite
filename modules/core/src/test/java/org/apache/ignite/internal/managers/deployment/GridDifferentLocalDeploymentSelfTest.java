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

package org.apache.ignite.internal.managers.deployment;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Multiple local deployments. */
public class GridDifferentLocalDeploymentSelfTest extends GridCommonAbstractTest {
    /** Task name. */
    private static final String TASK_NAME1 = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /** Task name. */
    private static final String TASK_NAME2 = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath2";

    /** */
    private DeploymentMode depMode = DeploymentMode.PRIVATE;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testCheckTaskClassloaderCacheSharedMode() throws Exception {
        testCheckTaskClassloaderCache(DeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testCheckTaskClassloaderCachePrivateMode() throws Exception {
        testCheckTaskClassloaderCache(DeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testCheckTaskClassloaderCacheIsolatedMode() throws Exception {
        testCheckTaskClassloaderCache(DeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    @Test
    public void testCheckTaskClassloaderCacheContinuousMode() throws Exception {
        testCheckTaskClassloaderCache(DeploymentMode.CONTINUOUS);
    }

    /** */
    public void testCheckTaskClassloaderCache(DeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        IgniteEx server = startGrid(0);

        IgniteEx client = startClientGrid(1);

        ClassLoader clsLdr1 = getExternalClassLoader();

        ClassLoader clsLdr2 = getExternalClassLoader();

        Class<ComputeTask> taskCls11 = (Class<ComputeTask>) clsLdr1.loadClass(TASK_NAME1);
        Class<ComputeTask> taskCls12 = (Class<ComputeTask>) clsLdr2.loadClass(TASK_NAME1);
        Class<ComputeTask> taskCls21 = (Class<ComputeTask>) clsLdr2.loadClass(TASK_NAME2);

        IgniteInternalFuture f1 = GridTestUtils.runAsync(() -> {
            for (int i = 0; i < 10; ++i) {
                try {
                    client.compute().execute(taskCls11.newInstance(), server.localNode().id());
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        IgniteInternalFuture f2 = GridTestUtils.runAsync(() -> {
            for (int i = 0; i < 10; ++i) {
                try {
                    client.compute().execute(taskCls12.newInstance(), server.localNode().id());
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        f1.get(); f2.get();

        client.compute().execute(taskCls21.newInstance(), server.localNode().id());

        GridDeploymentManager deploymentMgr = client.context().deploy();

        GridDeploymentStore store = GridTestUtils.getFieldValue(deploymentMgr, "locStore");

        ConcurrentMap<String, Deque<GridDeployment>> cache = GridTestUtils.getFieldValue(store, "cache");

        assertEquals(2, cache.get(TASK_NAME1).size());

        deploymentMgr = server.context().deploy();

        GridDeploymentStore verStore = GridTestUtils.getFieldValue(deploymentMgr, "verStore");

        // deployments per userVer map.
        Map<String, List<Object>> varCache = GridTestUtils.getFieldValue(verStore, "cache");

        if (depMode == DeploymentMode.CONTINUOUS || depMode == DeploymentMode.SHARED) {
            for (List<Object> deps : varCache.values())
                assertEquals(2, deps.size());
        }
    }
}
