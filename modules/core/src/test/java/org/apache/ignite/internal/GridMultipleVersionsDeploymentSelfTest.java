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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_TASK_UNDEPLOYED;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class GridMultipleVersionsDeploymentSelfTest extends GridCommonAbstractTest {
    /** Excluded classes. */
    private static final String[] EXCLUDE_CLASSES = new String[] {
        GridDeploymentTestTask.class.getName(),
        GridDeploymentTestJob.class.getName()
    };

    /** */
    public GridMultipleVersionsDeploymentSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(GridDeploymentTestJob.class.getName(),
            GridDeploymentTestTask.class.getName());

        // Following tests makes sense in ISOLATED modes (they redeploy tasks
        // and don't change task version. The different tasks with the same version from the same node
        // executed in parallel - this does not work in share mode.)
        cfg.setDeploymentMode(DeploymentMode.ISOLATED);

        cfg.setPeerClassLoadingLocalClassPathExclude(
            "org.apache.ignite.internal.GridMultipleVersionsDeploymentSelfTest*");

        return cfg;
    }

    /**
     * @param ignite Grid.
     * @param taskName Task name.
     * @return {@code true} if task has been deployed on passed grid.
     */
    private boolean checkDeployed(Ignite ignite, String taskName) {
        Map<String, Class<? extends ComputeTask<?, ?>>> locTasks = ignite.compute().localTasks();

        if (log().isInfoEnabled())
            log().info("Local tasks found: " + locTasks);

        return locTasks.get(taskName) != null;
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    public void testMultipleVersionsLocalDeploy() throws Exception {
        try {
            Ignite ignite = startGrid(1);

            ClassLoader ldr1 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "1"),
                getClass().getClassLoader(),
                EXCLUDE_CLASSES);

            ClassLoader ldr2 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "2"),
                getClass().getClassLoader(),
                EXCLUDE_CLASSES
            );

            Class<? extends ComputeTask<?, ?>> taskCls1 = (Class<? extends ComputeTask<?, ?>>)ldr1.
                loadClass(GridDeploymentTestTask.class.getName());

            Class<? extends ComputeTask<?, ?>> taskCls2 = (Class<? extends ComputeTask<?, ?>>)ldr2.
                loadClass(GridDeploymentTestTask.class.getName());

            ignite.compute().localDeployTask(taskCls1, ldr1);

            // Task will wait for the signal.
            ComputeTaskFuture fut = executeAsync(ignite.compute(), "GridDeploymentTestTask", null);

            // We should wait here when to be sure that job has been started.
            // Since we loader task/job classes with different class loaders we cannot
            // use any kind of mutex because of the illegal state exception.
            // We have to use timer here. DO NOT CHANGE 2 seconds. This should be enough
            // on Bamboo.
            Thread.sleep(2000);

            assert checkDeployed(ignite, "GridDeploymentTestTask");

            // Deploy new one - this should move first task to the obsolete list.
            ignite.compute().localDeployTask(taskCls2, ldr2);

            boolean deployed = checkDeployed(ignite, "GridDeploymentTestTask");

            Object res = fut.get();

            ignite.compute().undeployTask("GridDeploymentTestTask");

            // New one should be deployed.
            assert deployed;

            // Wait for the execution.
            assert res.equals(1);
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    public void testMultipleVersionsP2PDeploy() throws Exception {
        try {
            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            final CountDownLatch latch = new CountDownLatch(2);

            g2.events().localListen(
                new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        info("Received event: " + evt);

                        latch.countDown();

                        return true;
                    }
                }, EVT_TASK_UNDEPLOYED
            );

            ClassLoader ldr1 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "1"),
                getClass().getClassLoader(),
                EXCLUDE_CLASSES);

            ClassLoader ldr2 = new GridTestClassLoader(
                Collections.singletonMap("testResource", "2"),
                getClass().getClassLoader(),
                EXCLUDE_CLASSES);

            Class<? extends ComputeTask<?, ?>> taskCls1 = (Class<? extends ComputeTask<?, ?>>)ldr1.
                loadClass(GridDeploymentTestTask.class.getName());

            Class<? extends ComputeTask<?, ?>> taskCls2 = (Class<? extends ComputeTask<?, ?>>)ldr2.
                loadClass(GridDeploymentTestTask.class.getName());

            g1.compute().localDeployTask(taskCls1, ldr1);

            // Task will wait for the signal.
            ComputeTaskFuture fut1 = executeAsync(g1.compute(), "GridDeploymentTestTask", null);

            assert checkDeployed(g1, "GridDeploymentTestTask");

            // We should wait here when to be sure that job has been started.
            // Since we loader task/job classes with different class loaders we cannot
            // use any kind of mutex because of the illegal state exception.
            // We have to use timer here. DO NOT CHANGE 2 seconds here.
            Thread.sleep(2000);

            // Deploy new one - this should move first task to the obsolete list.
            g1.compute().localDeployTask(taskCls2, ldr2);

            // Task will wait for the signal.
            ComputeTaskFuture fut2 = executeAsync(g1.compute(), "GridDeploymentTestTask", null);

            boolean deployed = checkDeployed(g1, "GridDeploymentTestTask");

            Object res1 = fut1.get();
            Object res2 = fut2.get();

            g1.compute().undeployTask("GridDeploymentTestTask");

            // New one should be deployed.
            assert deployed;

            // Wait for the execution.
            assert res1.equals(1);
            assert res2.equals(2);

            stopGrid(1);

            assert latch.await(3000, MILLISECONDS);

            assert !checkDeployed(g2, "GridDeploymentTestTask");
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * Task that maps {@link GridDeploymentTestJob} either on local node
     * or on remote nodes if there are any. Never on both.
     */
    @SuppressWarnings({"PublicInnerClass"})
    @ComputeTaskName(value="GridDeploymentTestTask")
    public static class GridDeploymentTestTask extends ComputeTaskAdapter<Object, Object> {
        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            boolean ignoreLocNode = false;

            assert ignite != null;

            UUID locNodeId = ignite.configuration().getNodeId();

            assert locNodeId != null;

            if (subgrid.size() == 1)
                assert subgrid.get(0).id().equals(locNodeId) : "Wrong node id.";
            else
                ignoreLocNode = true;

            for (ClusterNode node : subgrid) {
                // Ignore local node.
                if (ignoreLocNode && node.id().equals(locNodeId))
                    continue;

                map.put(new GridDeploymentTestJob(), node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            return results.get(0).getData();
        }
    }

    /**
     * Simple job class that requests resource with name "testResource"
     * and expects "0" value.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class GridDeploymentTestJob extends ComputeJobAdapter {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Integer execute() {
            try {
                if (log.isInfoEnabled())
                    log.info("GridDeploymentTestJob job started");

                // Again there is no way to get access to any
                // mutex of the test class because of the different class loaders.
                // we have to wait.
                Thread.sleep(3000);

                // Here we should request some resources. New task
                // has already been deployed and old one should be still available.
                int res = getClass().getClassLoader().getResourceAsStream("testResource").read();

                return res - 48;
            }
            catch (IOException | InterruptedException e) {
                throw new IgniteException("Failed to execute job.", e);
            }
        }
    }
}