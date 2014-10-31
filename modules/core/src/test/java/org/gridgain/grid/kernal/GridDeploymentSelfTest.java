/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.deployment.local.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

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
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

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
     * @param grid Grid.
     * @param taskName Task name.
     * @return {@code True} if task is not deployed.
     */
    private boolean checkUndeployed(Grid grid, String taskName) {
        return grid.compute().localTasks().get(taskName) == null;
    }

    /**
     * @param grid Grid.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void stopGrid(Grid grid) {
        try {
            if (grid != null)
                stopGrid(grid.name());
        }
        catch (Throwable e) {
            error("Got error when stopping grid.", e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeploy() throws Exception {
        Grid grid = startGrid(getTestGridName());

        try {
            grid.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            grid.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert checkUndeployed(grid, GridDeploymentTestTask.class.getName());
        }
        finally {
            stopGrid(grid);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgnoreDeploymentSpi() throws Exception {
        // If peer class loading is disabled and local deployment SPI
        // is configured, SPI should be ignored.
        p2pEnabled = false;

        Grid grid = startGrid(getTestGridName());

        try {
            grid.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 0 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            grid.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 0 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();
        }
        finally {
            stopGrid(grid);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRedeploy() throws Exception {
        Grid grid = startGrid(getTestGridName());

        try {
            // Added to work with P2P.
            grid.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            // Check auto-deploy.
            GridComputeTaskFuture<?> fut = executeAsync(grid.compute(), GridDeploymentTestTask.class.getName(), null);

            fut.get();

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Check 2nd execute.
            fut = executeAsync(grid.compute(), GridDeploymentTestTask.class.getName(), null);

            fut.get();

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Redeploy, should be NO-OP for the same task.
            grid.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            // Check 2nd execute.
            fut = executeAsync(grid.compute(), GridDeploymentTestTask.class.getName(), null);

            fut.get();

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            // Check undeploy.
            grid.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) == null;

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            // Added to work with P2P
            grid.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            // Check auto-deploy.
            executeAsync(grid.compute(), GridDeploymentTestTask.class.getName(), null);

            assert depSpi.getRegisterCount() == 2;
            assert depSpi.getUnregisterCount() == 1;

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            grid.compute().localDeployTask(GridDeploymentTestTask1.class,
                GridDeploymentTestTask1.class.getClassLoader());

            try {
                grid.compute().localDeployTask(GridDeploymentTestTask2.class,
                    GridDeploymentTestTask2.class.getClassLoader());

                assert false : "Should not be able to deploy 2 task with same task name";
            }
            catch (GridException e) {
                info("Received expected grid exception: " + e);
            }

            assert depSpi.getRegisterCount() == 3 : "Invalid register count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid unregister count: " + depSpi.getUnregisterCount();

            assert grid.compute().localTasks().get("GridDeploymentTestTask") != null;

            Class<? extends GridComputeTask<?, ?>> cls = grid.compute().localTasks().get("GridDeploymentTestTask");

            assert cls.getName().equals(GridDeploymentTestTask1.class.getName());
        }
        finally {
            stopGrid(grid);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"BusyWait"})
    public void testDeployOnTwoNodes() throws Exception {
        Grid grid1 = startGrid(getTestGridName() + '1');
        Grid grid2 = startGrid(getTestGridName() + '2');

        try {
            assert !grid1.cluster().forRemotes().nodes().isEmpty() : grid1.cluster().forRemotes();
            assert !grid2.cluster().forRemotes().nodes().isEmpty() : grid2.cluster().forRemotes();

            grid1.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());
            grid2.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert grid1.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;
            assert grid2.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            grid1.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert checkUndeployed(grid1, GridDeploymentTestTask.class.getName());

            int cnt = 0;

            boolean taskUndeployed = false;

            while (cnt++ < 10 && !taskUndeployed) {
                taskUndeployed = checkUndeployed(grid2, GridDeploymentTestTask.class.getName());

                if (!taskUndeployed)
                    Thread.sleep(500);
            }

            // Undeploy on one node should undeploy explicitly deployed
            // tasks on the others
            assert taskUndeployed;
        }
        finally {
            stopGrid(grid1);
            stopGrid(grid2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployEvents() throws Exception {
        Grid grid = startGrid(getTestGridName());

        try {
            DeploymentEventListener evtLsnr = new DeploymentEventListener();

            grid.events().localListen(evtLsnr, EVT_TASK_DEPLOYED, EVT_TASK_UNDEPLOYED);

            // Should generate 1st deployment event.
            grid.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 0 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Should generate 1st un-deployment event.
            grid.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 1 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert checkUndeployed(grid, GridDeploymentTestTask.class.getName());

            // Should generate 2nd deployment event.
            grid.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 2 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 1 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Should generate 2nd un-deployment event.
            grid.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 2 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 2 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert checkUndeployed(grid, GridDeploymentTestTask.class.getName());

            // Should generate 3rd deployment event.
            grid.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert depSpi.getRegisterCount() == 3 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 2 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert grid.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            // Should generate 3rd un-deployment event.
            grid.compute().undeployTask(GridDeploymentTestTask.class.getName());

            assert depSpi.getRegisterCount() == 3 : "Invalid deploy count: " + depSpi.getRegisterCount();
            assert depSpi.getUnregisterCount() == 3 : "Invalid undeploy count: " + depSpi.getUnregisterCount();

            assert checkUndeployed(grid, GridDeploymentTestTask.class.getName());

            assert evtLsnr.getDeployCount() == 3 : "Invalid number of deployment events" + evtLsnr.getDeployCount();
            assert evtLsnr.getUndeployCount() == 3 : "Invalid number of un-deployment events" + evtLsnr.getDeployCount();
        }
        finally {
            stopGrid(grid);
        }
    }

    /**
     * Test deployable task.
     */
    private static class GridDeploymentTestTask extends GridComputeTaskAdapter<Object, Object> {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Object arg)
            throws GridException {
            Map<GridComputeJobAdapter, GridNode> map = new HashMap<>(subgrid.size());

            for (GridNode node : subgrid) {
                map.put(new GridComputeJobAdapter() {
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
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     * Test deployable named task.
     */
    @GridComputeTaskName(value = "GridDeploymentTestTask")
    private static class GridDeploymentTestTask1 extends GridComputeTaskAdapter<Object, Object> {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Object arg) throws GridException {
            Map<GridComputeJobAdapter, GridNode> map = new HashMap<>(subgrid.size());

            for (GridNode node : subgrid) {
                map.put(new GridComputeJobAdapter() {
                    @Override public Serializable execute() {
                        log.info("Executing grid job: " + this);

                        return null;
                    }
                }, node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     * Test deployable named task.
     */
    @GridComputeTaskName(value = "GridDeploymentTestTask")
    private static class GridDeploymentTestTask2 extends GridComputeTaskAdapter<Object, Object> {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Object arg) throws GridException {
            Map<GridComputeJobAdapter, GridNode> map = new HashMap<>(subgrid.size());

            for (GridNode node : subgrid) {
                map.put(new GridComputeJobAdapter() {
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
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     *
     * Test deployment spi.
     */
    private static class TestDeploymentSpi extends GridLocalDeploymentSpi {
        /** */
        private volatile int deployCnt;

        /** */
        private volatile int undeployCnt;

        /** {@inheritDoc} */
        @Override public boolean register(ClassLoader ldr, Class rsrc) throws GridSpiException {
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
    private static class DeploymentEventListener implements GridPredicate<GridEvent> {
        /** */
        private int depCnt;

        /** */
        private int undepCnt;

        /**
         * Gonna process task deployment events only.
         *
         * @param evt local grid event.
         */
        @Override public boolean apply(GridEvent evt) {
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
