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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class IgniteExplicitImplicitDeploymentSelfTest extends GridCommonAbstractTest {
    /** */
    public IgniteExplicitImplicitDeploymentSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(
            IgniteExplicitImplicitDeploymentSelfTest.class.getName(),
            GridDeploymentResourceTestTask.class.getName(),
            GridDeploymentResourceTestJob.class.getName()
        );

        cfg.setDeploymentMode(DeploymentMode.ISOLATED);

        return cfg;
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testImplicitDeployLocally() throws Exception {
        execImplicitDeployLocally(true, true, true);
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testImplicitDeployP2P() throws Exception {
        execImplicitDeployP2P(true, true, true);
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testExplicitDeployLocally() throws Exception {
        execExplicitDeployLocally(true, true, true);
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testExplicitDeployP2P() throws Exception {
        execExplicitDeployP2P(true, true, true);
    }

    /** Calls async compute execution with Class of the task.
     *
     * @param ignite Ignite server instance.
     * @param client Ignite client instance.
     * @param taskCls Class to compute.
     * @param expected Expected result.
     */
    private IgniteInternalFuture runAsyncByClass(
        final IgniteEx ignite,
        final IgniteEx client,
        Class<? extends ComputeTask<String, Integer>> taskCls,
        int expected
    ) {
        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            for (int i = 0; i < 10; ++i) {
                Integer res1 = ignite.compute().execute(taskCls, null);
                assertNotNull(res1);
                assertEquals("Invalid res1: ", expected, (int)res1);

                res1 = client.compute(ignite.compute().clusterGroup().forNodeId(ignite.localNode().id())).execute(taskCls, null);
                assertNotNull(res1);
                assertEquals("Invalid res1: ", expected, (int)res1);
            }
        });

        return f;
    }

    /** Calls async compute execution with class instance.
     * @param ignite Ignite server instance.
     * @param client Ignite client instance.
     * @param taskCls Instance to compute.
     * @param expected Expected result.
     */
    private IgniteInternalFuture runAsyncByInstance(
        final IgniteEx ignite,
        final IgniteEx client,
        ComputeTask<String, Integer> taskCls,
        int expected
    ) {
        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            for (int i = 0; i < 10; ++i) {
                Integer res1 = ignite.compute().execute(taskCls, null);
                assertNotNull(res1);
                assertEquals("Invalid res: ", expected, (int)res1);

                res1 = client.compute(ignite.compute().clusterGroup().forNodeId(ignite.localNode().id())).execute(taskCls, null);
                assertNotNull(res1);
                assertEquals("Invalid res: ", expected, (int)res1);
            }
        });

        return f;
    }

    /**
     * @param byCls If {@code true} than executes task by Class.
     * @param byTask If {@code true} than executes task instance.
     * @param byName If {@code true} than executes task by class name.
     * @throws Exception If test failed.
     */
    private void execExplicitDeployLocally(boolean byCls, boolean byTask, boolean byName) throws Exception {
        Ignite ignite;

        try {
            ignite = startGrid();

            // Explicit Deployment. Task execution should return 0.
            // Say resource class loader - different to task one.
            ClassLoader ldr1 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "1"),
                getClass().getClassLoader());

            // Assume that users task and job were loaded with this class loader
            ClassLoader ldr2 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "2"),
                getClass().getClassLoader(),
                GridDeploymentResourceTestTask.class.getName(),
                GridDeploymentResourceTestJob.class.getName()
            );

            info("Loader1: " + ldr1);
            info("Loader2: " + ldr2);

            Class<? extends ComputeTask<String, Integer>> taskCls = (Class<? extends ComputeTask<String, Integer>>)
                ldr2.loadClass(GridDeploymentResourceTestTask.class.getName());

            // Check auto-deploy. It should pick up resource class loader.
            if (byCls) {
                ignite.compute().localDeployTask(taskCls, ldr1);

                Integer res = ignite.compute().execute(taskCls, null);

                assert res != null;
                assert res == 2 : "Invalid response: " + res;
            }

            if (byTask) {
                ignite.compute().localDeployTask(taskCls, ldr1);

                Integer res = ignite.compute().execute(taskCls.newInstance(), null);

                assert res != null;
                assert res == 2 : "Invalid response: " + res;
            }

            if (byName) {
                ignite.compute().localDeployTask(taskCls, ldr1);

                Integer res = ignite.compute().execute(taskCls.getName(), null);

                assert res != null;
                assert res == 1 : "Invalid response: " + res;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param byCls If {@code true} than executes task by Class.
     * @param byTask If {@code true} than executes task instance.
     * @param byName If {@code true} than executes task by class name.
     * @throws Exception If test failed.
     */
    private void execImplicitDeployLocally(boolean byCls, boolean byTask, boolean byName) throws Exception {
        final IgniteEx ignite = startGrids(1);

        final IgniteEx client = startClientGrid(1);

        try {
            // First task class loader.
            ClassLoader ldr1 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "1"),
                getClass().getClassLoader(),
                GridDeploymentResourceTestTask.class.getName(),
                GridDeploymentResourceTestJob.class.getName()
            );

            // Second task class loader
            ClassLoader ldr2 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "2"),
                getClass().getClassLoader(),
                GridDeploymentResourceTestTask.class.getName(),
                GridDeploymentResourceTestJob.class.getName()
            );

            // The same name but different classes/ class loaders.
            Class<? extends ComputeTask<String, Integer>> taskCls1 = (Class<? extends ComputeTask<String, Integer>>)
                ldr1.loadClass(GridDeploymentResourceTestTask.class.getName());

            Class<? extends ComputeTask<String, Integer>> taskCls2 = (Class<? extends ComputeTask<String, Integer>>)
                ldr2.loadClass(GridDeploymentResourceTestTask.class.getName());

            if (byCls) {
                IgniteInternalFuture f1 = runAsyncByClass(ignite, client, taskCls1, 1);

                IgniteInternalFuture f2 = runAsyncByClass(ignite, client, taskCls2, 2);

                f1.get();
                f2.get();
            }

            if (byTask) {
                final ComputeTask<String, Integer> tc1 = taskCls1.newInstance();
                final ComputeTask<String, Integer> tc2 = taskCls2.newInstance();

                IgniteInternalFuture f1 = runAsyncByInstance(ignite, client, tc1, 1);

                IgniteInternalFuture f2 = runAsyncByInstance(ignite, client, tc2, 2);

                f1.get();
                f2.get();
            }

            if (byName) {
                ignite.compute().localDeployTask(taskCls1, ldr1);

                Integer res1 = ignite.compute().execute(taskCls1.getName(), null);

                assert res1 != null;

                assert res1 == 1 : "Invalid res1: " + res1;

                ignite.compute().localDeployTask(taskCls2, ldr2);

                Integer res2 = ignite.compute().execute(taskCls2.getName(), null);

                assert res2 != null;

                assert res2 == 2 : "Invalid res2: " + res2;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param byCls If {@code true} than executes task by Class.
     * @param byTask If {@code true} than executes task instance.
     * @param byName If {@code true} than executes task by class name.
     * @throws Exception If test failed.
     */
    private void execExplicitDeployP2P(boolean byCls, boolean byTask, boolean byName) throws Exception {
        try {
            IgniteEx ignite = startGrids(2);

            ClassLoader ldr1 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "1"),
                getClass().getClassLoader(),
                IgniteExplicitImplicitDeploymentSelfTest.class.getName(),
                GridDeploymentResourceTestTask.class.getName(),
                GridDeploymentResourceTestJob.class.getName()
            );

            ClassLoader ldr2 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "2"),
                getClass().getClassLoader(),
                IgniteExplicitImplicitDeploymentSelfTest.class.getName(),
                GridDeploymentResourceTestTask.class.getName(),
                GridDeploymentResourceTestJob.class.getName()
            );

            Class<? extends ComputeTask<String, Integer>> taskCls = (Class<? extends ComputeTask<String, Integer>>)
                ldr2.loadClass(GridDeploymentResourceTestTask.class.getName());

            if (byCls) {
                ignite.compute().localDeployTask(taskCls, ldr1);

                // Even though the task is deployed with resource class loader,
                // when we execute it, it will be redeployed with task class-loader.
                Integer res = ignite.compute().execute(taskCls, null);

                assert res != null;
                assert res == 2 : "Invalid response: " + res;
            }

            if (byTask) {
                ignite.compute().localDeployTask(taskCls, ldr1);

                // Even though the task is deployed with resource class loader,
                // when we execute it, it will be redeployed with task class-loader.
                Integer res = ignite.compute().execute(taskCls.newInstance(), null);

                assert res != null;
                assert res == 2 : "Invalid response: " + res;
            }

            if (byName) {
                ignite.compute().localDeployTask(taskCls, ldr1);

                // Even though the task is deployed with resource class loader,
                // when we execute it, it will be redeployed with task class-loader.
                Integer res = ignite.compute().execute(taskCls.getName(), null);

                assert res != null;
                assert res == 1 : "Invalid response: " + res;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param byCls If {@code true} than executes task by Class.
     * @param byTask If {@code true} than executes task instance.
     * @param byName If {@code true} than executes task by class name.
     * @throws Exception If test failed.
     */
    private void execImplicitDeployP2P(boolean byCls, boolean byTask, boolean byName) throws Exception {
        try {
            IgniteEx ignite = startGrids(1);

            final IgniteEx client = startClientGrid(1);

            ClassLoader ldr1 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "1"),
                getClass().getClassLoader(),
                GridDeploymentResourceTestTask.class.getName(),
                GridDeploymentResourceTestJob.class.getName()
            );

            ClassLoader ldr2 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "2"),
                getClass().getClassLoader(),
                GridDeploymentResourceTestTask.class.getName(),
                GridDeploymentResourceTestJob.class.getName()
            );

            Class<? extends ComputeTask<String, Integer>> taskCls1 = (Class<? extends ComputeTask<String, Integer>>)
                ldr1.loadClass(GridDeploymentResourceTestTask.class.getName());

            Class<? extends ComputeTask<String, Integer>> taskCls2 = (Class<? extends ComputeTask<String, Integer>>)
                ldr2.loadClass(GridDeploymentResourceTestTask.class.getName());

            if (byCls) {
                IgniteInternalFuture f1 = runAsyncByClass(ignite, client, taskCls1, 1);

                IgniteInternalFuture f2 = runAsyncByClass(ignite, client, taskCls2, 2);

                f1.get();
                f2.get();
            }

            if (byTask) {
                final ComputeTask<String, Integer> tc1 = taskCls1.newInstance();
                final ComputeTask<String, Integer> tc2 = taskCls2.newInstance();

                IgniteInternalFuture f1 = runAsyncByInstance(ignite, client, tc1, 1);

                IgniteInternalFuture f2 = runAsyncByInstance(ignite, client, tc2, 2);

                f1.get();
                f2.get();
            }

            if (byName) {
                ignite.compute().localDeployTask(taskCls1, ldr1);
                Integer res1 = ignite.compute().execute(taskCls1.getName(), null);

                assert res1 != null;
                assertEquals("Invalid res1: ", 1, (int)res1);

                ignite.compute().localDeployTask(taskCls2, ldr2);
                Integer res2 = ignite.compute().execute(taskCls2.getName(), null);

                assert res2 != null;
                assertEquals("Invalid res2: ", 2, (int)res2);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * We use custom name to avoid auto-deployment in the same VM.
     */
    @SuppressWarnings({"PublicInnerClass"})
    @ComputeTaskName("GridDeploymentResourceTestTask")
    public static class GridDeploymentResourceTestTask extends ComputeTaskAdapter<String, Integer> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            boolean ignoreLocNode = false;

            UUID locId = ignite.configuration().getNodeId();

            if (subgrid.size() == 1)
                assert subgrid.get(0).id().equals(locId) : "Wrong node id.";
            else
                ignoreLocNode = true;

            for (ClusterNode node : subgrid) {
                // Ignore local node.
                if (ignoreLocNode && node.id().equals(locId))
                    continue;

                map.put(new GridDeploymentResourceTestJob(), node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            return results.get(0).getData();
        }
    }

    /**
     * Simple job for this test.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class GridDeploymentResourceTestJob extends ComputeJobAdapter {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            if (log.isInfoEnabled())
                log.info("Executing grid job: " + this);

            try {
                ClassLoader ldr = Thread.currentThread().getContextClassLoader();

                if (log.isInfoEnabled())
                    log.info("Loader (inside job): " + ldr);

                InputStream in = ldr.getResourceAsStream("testResource");

                if (in != null) {
                    Reader reader = new InputStreamReader(in);

                    try {
                        char res = (char)reader.read();

                        return Integer.parseInt(Character.toString(res));
                    }
                    finally {
                        U.close(in, null);
                    }
                }

                return null;
            }
            catch (IOException e) {
                throw new IgniteException("Failed to execute job.", e);
            }
        }
    }
}
