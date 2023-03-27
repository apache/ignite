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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDeploymentException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Checks count of tries to load class directly from class loader.
 */
@GridCommonTest(group = "P2P")
public class GridP2PCountTiesLoadClassDirectlyFromClassLoaderTest extends GridCommonAbstractTest {
    /** P2P class path property. */
    public static final String CLS_PATH_PROPERTY = "p2p.uri.cls";

    /** Compute task name. */
    private static String COMPUTE_TASK_NAME = "org.apache.ignite.tests.p2p.compute.ExternalCallable";

    /** Compute task name. */
    private static String COMPUTE_STEALING_TASK_NAME = "org.apache.ignite.tests.p2p.JobStealingTask";

    /** Deployment mode. */
    private DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDeploymentMode(depMode)
            .setPeerClassLoadingEnabled(true);
    }

    /**
     * @throws Exception if error occur.
     */
    public void executeP2PTask(DeploymentMode depMode) throws Exception {
        try {
            CountTriesClassLoader testCountLdr = new CountTriesClassLoader(Thread.currentThread()
                .getContextClassLoader());

            this.depMode = depMode;

            Thread.currentThread().setContextClassLoader(testCountLdr);

            String path = GridTestProperties.getProperty(CLS_PATH_PROPERTY);

            ClassLoader urlClsLdr = new URLClassLoader(new URL[] {new URL(path)}, testCountLdr);

            Ignite ignite = startGrids(2);

            ignite.compute(ignite.cluster().forRemotes()).call((IgniteCallable)urlClsLdr.loadClass(COMPUTE_TASK_NAME)
                .newInstance());

            int count = testCountLdr.count;

            ignite.compute(ignite.cluster().forRemotes()).call((IgniteCallable)urlClsLdr.loadClass(COMPUTE_TASK_NAME)
                .newInstance());
            ignite.compute(ignite.cluster().forRemotes()).call((IgniteCallable)urlClsLdr.loadClass(COMPUTE_TASK_NAME)
                .newInstance());
            ignite.compute(ignite.cluster().forRemotes()).call((IgniteCallable)urlClsLdr.loadClass(COMPUTE_TASK_NAME)
                .newInstance());

            assertEquals(count, testCountLdr.count);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if error occurs.
     */
    public void executeP2PTaskWithRestartMaster(DeploymentMode depMode) throws Exception {
        try {
            CountTriesClassLoader testCntLdr = new CountTriesClassLoader(Thread.currentThread()
                .getContextClassLoader());

            this.depMode = depMode;

            Thread.currentThread().setContextClassLoader(testCntLdr);

            String path = GridTestProperties.getProperty(CLS_PATH_PROPERTY);

            ClassLoader urlClsLdr = new URLClassLoader(new URL[] {new URL(path)}, testCntLdr);

            Ignite ignite = startGrids(2);

            Map<UUID, Integer> res = (Map<UUID, Integer>)ignite.compute(ignite.cluster().forRemotes()).execute(
                (ComputeTask<Integer, Object>)urlClsLdr.loadClass(COMPUTE_STEALING_TASK_NAME).newInstance(), 1);

            info("Result: " + res);

            int cnt = testCntLdr.count;

            ignite.compute(ignite.cluster().forRemotes()).execute(COMPUTE_STEALING_TASK_NAME, 2);
            ignite.compute(ignite.cluster().forRemotes()).execute(COMPUTE_STEALING_TASK_NAME, 3);
            ignite.compute(ignite.cluster().forRemotes()).execute(COMPUTE_STEALING_TASK_NAME, 4);

            assertEquals(cnt, testCntLdr.count);

            ignite.close();

            ignite = startGrid(0);

            try {
                ignite.compute().execute(COMPUTE_STEALING_TASK_NAME, 5);

                if (depMode != DeploymentMode.CONTINUOUS)
                    fail("Task should be undeployed.");
            }
            catch (IgniteDeploymentException e) {
                if (depMode != DeploymentMode.CONTINUOUS)
                    assertTrue(e.getMessage(), e.getMessage().contains("Unknown task name or failed to auto-deploy task"));
                else
                    fail(e.getMessage());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testRestartPrivateMode() throws Exception {
        executeP2PTaskWithRestartMaster(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testRestartIsolatedMode() throws Exception {
        executeP2PTaskWithRestartMaster(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testRestartSharedMode() throws Exception {
        executeP2PTaskWithRestartMaster(DeploymentMode.SHARED);
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testPrivateMode() throws Exception {
        executeP2PTask(DeploymentMode.PRIVATE);
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testIsolatedMode() throws Exception {
        executeP2PTask(DeploymentMode.ISOLATED);
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testContinuousMode() throws Exception {
        executeP2PTask(DeploymentMode.CONTINUOUS);
    }

    /**
     * @throws Exception if error occur.
     */
    @Test
    public void testSharedMode() throws Exception {
        executeP2PTask(DeploymentMode.SHARED);
    }

    /**
     * Test count class loader.
     */
    private static class CountTriesClassLoader extends ClassLoader {
        /** Count of tries. */
        int count = 0;

        /**
         * @param parent Parent class loader.
         */
        public CountTriesClassLoader(ClassLoader parent) {
            super(parent);
        }

        /** {@inheritDoc} */
        @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (COMPUTE_TASK_NAME.equals(name))
                U.dumpStack(log, "Try to load class: " + name + " " + ++count);

            return super.loadClass(name, resolve);
        }
    }
}
