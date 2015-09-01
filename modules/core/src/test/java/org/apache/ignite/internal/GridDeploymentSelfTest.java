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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.events.EventType.EVT_TASK_DEPLOYED;
import static org.apache.ignite.events.EventType.EVT_TASK_UNDEPLOYED;

/**
 * Task deployment tests.
 */
@SuppressWarnings("unchecked")
@GridCommonTest(group = "Kernal Self")
public class GridDeploymentSelfTest extends GridCommonAbstractTest {
    /** */
    private TestDeploymentSpi depSpi;

    /** */
    private boolean p2pEnabled = true;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        depSpi = new TestDeploymentSpi();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        depSpi = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentSpi(depSpi = new TestDeploymentSpi());
        cfg.setPeerClassLoadingEnabled(p2pEnabled);

        // Disable cache since it can deploy some classes during start process.
        cfg.setCacheConfiguration();

        return cfg;
    }

    /** */
    public GridDeploymentSelfTest() {
        super(/*start grid*/false);
    }

    /**
     * @param ignite Grid.
     * @param taskName Task name.
     * @return {@code True} if task is not deployed.
     */
    private boolean checkUndeployed(Ignite ignite, String taskName) {
        return ignite.compute().localTasks().get(taskName) == null;
    }

    /**
     * @param ignite Grid.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void stopGrid(Ignite ignite) {
        try {
            if (ignite != null)
                stopGrid(ignite.name());
        }
        catch (Throwable e) {
            error("Got error when stopping grid.", e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeploy() throws Exception {
        Ignite ignite = startGrid(getTestGridName());

        try {
            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert checkUndeployed(ignite, GridDeploymentTestTask.class.getName());
        }
        finally {
            stopGrid(ignite);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgnoreDeploymentSpi() throws Exception {
        // If peer class loading is disabled and local deployment SPI
        // is configured, SPI should be ignored.
        p2pEnabled = false;

        Ignite ignite = startGrid(getTestGridName());

        try {
            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 0 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 0 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();
        }
        finally {
            stopGrid(ignite);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRedeploy() throws Exception {
        Ignite ignite = startGrid(getTestGridName());

        try {
            // Added to work with P2P.
            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            // Check auto-deploy.
            ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridDeploymentTestTask.class.getName(), null);

            fut.get();

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Check 2nd execute.
            fut = executeAsync(ignite.compute(), GridDeploymentTestTask.class.getName(), null);

            fut.get();

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Redeploy, should be NO-OP for the same task.
            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            // Check 2nd execute.
            fut = executeAsync(ignite.compute(), GridDeploymentTestTask.class.getName(), null);

            fut.get();

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            // Check undeploy.
            ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) == null;

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            // Added to work with P2P
            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            // Check auto-deploy.
            executeAsync(ignite.compute(), GridDeploymentTestTask.class.getName(), null);

            assert depSpi.getRegisterCount() == 2;
            assert depSpi.getUnregisterCount() == 1;

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            ignite.compute().localDeployTask(GridDeploymentTestTask1.class,
                GridDeploymentTestTask1.class.getClassLoader());

            try {
                ignite.compute().localDeployTask(GridDeploymentTestTask2.class,
                    GridDeploymentTestTask2.class.getClassLoader());

                assert false : "Should not be able to deploy 2 task with same task name";
            }
            catch (IgniteException e) {
                info("Received expected grid exception: " + e);
            }

            assert depSpi.getRegisterCount() == 3 : "Invalid register count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid unregister count: " + depSpi.getUnregisterCount();

            assert ignite.compute().localTasks().get("GridDeploymentTestTask") != null;

            Class<? extends ComputeTask<?, ?>> cls = ignite.compute().localTasks().get("GridDeploymentTestTask");

            assert cls.getName().equals(GridDeploymentTestTask1.class.getName());
        }
        finally {
            stopGrid(ignite);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"BusyWait"})
    public void testDeployOnTwoNodes() throws Exception {
        Ignite ignite1 = startGrid(getTestGridName() + '1');
        Ignite ignite2 = startGrid(getTestGridName() + '2');

        try {
            assert !ignite1.cluster().forRemotes().nodes().isEmpty() : ignite1.cluster().forRemotes();
            assert !ignite2.cluster().forRemotes().nodes().isEmpty() : ignite2.cluster().forRemotes();

            ignite1.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());
            ignite2.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert ignite1.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;
            assert ignite2.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            ignite1.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert checkUndeployed(ignite1, GridDeploymentTestTask.class.getName());

            int cnt = 0;

            boolean taskUndeployed = false;

            while (cnt++ < 10 && !taskUndeployed) {
                taskUndeployed = checkUndeployed(ignite2, GridDeploymentTestTask.class.getName());

                if (!taskUndeployed)
                    Thread.sleep(500);
            }

            // Undeploy on one node should undeploy explicitly deployed
            // tasks on the others
            assert taskUndeployed;
        }
        finally {
            stopGrid(ignite1);
            stopGrid(ignite2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployEvents() throws Exception {
        Ignite ignite = startGrid(getTestGridName());

        try {
            DeploymentEventListener evtLsnr = new DeploymentEventListener();

            ignite.events().localListen(evtLsnr, EVT_TASK_DEPLOYED, EVT_TASK_UNDEPLOYED);

            // Should generate 1st deployment event.
            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Should generate 1st un-deployment event.
            ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert checkUndeployed(ignite, GridDeploymentTestTask.class.getName());

            // Should generate 2nd deployment event.
            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 2 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Should generate 2nd un-deployment event.
            ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 2 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 2 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert checkUndeployed(ignite, GridDeploymentTestTask.class.getName());

            // Should generate 3rd deployment event.
            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 3 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 2 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Should generate 3rd un-deployment event.
            ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 3 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 3 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert checkUndeployed(ignite, GridDeploymentTestTask.class.getName());

            assert evtLsnr.getDeployCount() == 3 : "Invalid number of deployment events" + evtLsnr.getDeployCount();
            assert evtLsnr.getUndeployCount() == 3 : "Invalid number of un-deployment events" + evtLsnr.getDeployCount();
        }
        finally {
            stopGrid(ignite);
        }
    }

    /**
     * Test deployable task.
     */
    private static class GridDeploymentTestTask extends ComputeTaskAdapter<Object, Object> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            for (ClusterNode node : subgrid) {
                map.put(new ComputeJobAdapter() {
                    @Override public Serializable execute() {
                        if (log.isInfoEnabled())
                            log.info("Executing grid job: " + this);

                        return null;
                    }
                }, node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Test deployable named task.
     */
    @ComputeTaskName(value = "GridDeploymentTestTask")
    private static class GridDeploymentTestTask1 extends ComputeTaskAdapter<Object, Object> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            for (ClusterNode node : subgrid) {
                map.put(new ComputeJobAdapter() {
                    @Override public Serializable execute() {
                        log.info("Executing grid job: " + this);

                        return null;
                    }
                }, node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Test deployable named task.
     */
    @ComputeTaskName(value = "GridDeploymentTestTask")
    private static class GridDeploymentTestTask2 extends ComputeTaskAdapter<Object, Object> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            for (ClusterNode node : subgrid) {
                map.put(new ComputeJobAdapter() {
                    @Override public Serializable execute() {
                        if (log.isInfoEnabled())
                            log.info("Executing grid job: " + this);

                        return null;
                    }
                }, node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     *
     * Test deployment spi.
     */
    private static class TestDeploymentSpi extends LocalDeploymentSpi {
        /** */
        private volatile int deployCnt;

        /** */
        private volatile int undeployCnt;

        /** {@inheritDoc} */
        @Override public boolean register(ClassLoader ldr, Class rsrc) throws IgniteSpiException {
            if (super.register(ldr, rsrc)) {
                deployCnt++;

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean unregister(String rsrcName) {
            undeployCnt++;

            return super.unregister(rsrcName);
        }

        /**
         * @return Deploy count.
         */
        public int getRegisterCount() {
            return deployCnt;
        }

        /**
         * @return Undeploy count.
         */
        public int getUnregisterCount() {
            return undeployCnt;
        }
    }

    /**
     * Deployment listener.
     */
    private static class DeploymentEventListener implements IgnitePredicate<Event> {
        /** */
        private int depCnt;

        /** */
        private int undepCnt;

        /**
         * Gonna process task deployment events only.
         *
         * @param evt local grid event.
         */
        @Override public boolean apply(Event evt) {
            if (evt.type() == EVT_TASK_DEPLOYED)
                depCnt++;
            else if (evt.type() == EVT_TASK_UNDEPLOYED)
                undepCnt++;

            return true;
        }

        /**
         * @return Deploy count.
         */
        public int getDeployCount() {
            return depCnt;
        }

        /**
         * @return Undeploy count.
         */
        public int getUndeployCount() {
            return undepCnt;
        }
    }
}