/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.IgniteEventType.*;

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
        cfg.setDeploymentMode(IgniteDeploymentMode.ISOLATED);

        cfg.setPeerClassLoadingLocalClassPathExclude(
            "org.gridgain.grid.kernal.GridMultipleVersionsDeploymentSelfTest*");

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
                new IgnitePredicate<IgniteEvent>() {
                    @Override public boolean apply(IgniteEvent evt) {
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
        /** */
        @IgniteLocalNodeIdResource
        private UUID locNodeId;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws GridException {
            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            boolean ignoreLocNode = false;

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
        @Override public Integer reduce(List<ComputeJobResult> results) throws GridException {
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
        @IgniteLoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Integer execute() throws GridException {
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
                throw new GridException("Failed to execute job.", e);
            }
        }
    }
}
