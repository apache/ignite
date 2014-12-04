/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PRemoteClassLoadersSelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Override P2P configuration to exclude Task and Job classes
        cfg.setPeerClassLoadingLocalClassPathExclude(
            GridP2PTestTask.class.getName(),
            GridP2PTestTask1.class.getName(),
            GridP2PTestJob.class.getName(),
            GridP2PRemoteClassLoadersSelfTest.class.getName()
        );

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed..
     */
    @SuppressWarnings("unchecked")
    private void processTestSameRemoteClassLoader(GridDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            GridP2PTestStaticVariable.staticVar = 0;

            Ignite ignite1 = startGrid(1);
            startGrid(2);

            waitForRemoteNodes(ignite1, 1);

            ClassLoader tstClsLdr =
                new GridTestClassLoader(
                    Collections.<String, String>emptyMap(), getClass().getClassLoader(),
                    GridP2PTestTask.class.getName(), GridP2PTestTask1.class.getName(), GridP2PTestJob.class.getName());

            Class<? extends ComputeTask<?, ?>> task1 =
                (Class<? extends ComputeTask<?, ?>>) tstClsLdr.loadClass(GridP2PTestTask.class.getName());

            Class<? extends ComputeTask<?, ?>> task2 =
                (Class<? extends ComputeTask<?, ?>>) tstClsLdr.loadClass(GridP2PTestTask1.class.getName());

            Object res1 = ignite1.compute().execute(task1.newInstance(), null);

            Object res2 = ignite1.compute().execute(task2.newInstance(), null);

            info("Check results.");

            // One remote p2p class loader
            assert res1 != null : "res1 != null";
            assert res1 instanceof Long : "res1 instanceof Long != true";
            assert res1.equals(0L): "Expected 0, got " + res1;

            // The same remote p2p class loader.
            assert res2 != null : "res2 != null";
            assert res2 instanceof Long : "res2 instanceof Long != true";
            assert res2.equals(1L) : "Expected 1 got " + res2;

            info("Tests passed.");
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * @param depMode deployment mode.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void processTestDifferentRemoteClassLoader(GridDeploymentMode depMode) throws Exception {
        try {
            this.depMode = depMode;

            GridP2PTestStaticVariable.staticVar = 0;

            Ignite ignite1 = startGrid(1);
            startGrid(2);

            waitForRemoteNodes(ignite1, 1);

            ClassLoader tstClsLdr1 =
                new GridTestClassLoader(
                    Collections.EMPTY_MAP, getClass().getClassLoader(),
                    GridP2PTestTask.class.getName(), GridP2PTestJob.class.getName()
                );

            ClassLoader tstClsLdr2 =
                new GridTestClassLoader(
                    Collections.EMPTY_MAP, getClass().getClassLoader(),
                    GridP2PTestTask1.class.getName(), GridP2PTestJob.class.getName());

            Class<? extends ComputeTask<?, ?>> task1 =
                (Class<? extends ComputeTask<?, ?>>) tstClsLdr1.loadClass(GridP2PTestTask.class.getName());

            Class<? extends ComputeTask<?, ?>> task2 =
                (Class<? extends ComputeTask<?, ?>>) tstClsLdr2.loadClass(GridP2PTestTask1.class.getName());

            Object res1 = ignite1.compute().execute(task1.newInstance(), null);

            Object res2 = ignite1.compute().execute(task2.newInstance(), null);

            info("Check results.");

            // One remote p2p class loader
            assert res1 != null : "res1 != null";
            assert res1 instanceof Long : "res1 instanceof Long != true";
            assert res1.equals(0L): "Invalid res2 value: " + res1;

            // Another remote p2p class loader.
            assert res2 != null : "res2 == null";
            assert res2 instanceof Long : "res2 instanceof Long != true";
            assert res2.equals(0L) : "Invalid res2 value: " + res2;

            info("Tests passed.");
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
    public void testSameClassLoaderPrivateMode() throws Exception {
        processTestSameRemoteClassLoader(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSameClassLoaderIsolatedMode() throws Exception {
        processTestSameRemoteClassLoader(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testDifferentClassLoaderPrivateMode() throws Exception {
        processTestDifferentRemoteClassLoader(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testDifferentClassLoaderIsolatedMode() throws Exception {
        processTestDifferentRemoteClassLoader(GridDeploymentMode.ISOLATED);
    }

    /**
     * Static variable holder class.
     */
    public static final class GridP2PTestStaticVariable {
        /** */
        @SuppressWarnings({"PublicField"})
        public static long staticVar;

        /**
         * Enforces singleton.
         */
        private GridP2PTestStaticVariable() {
            // No-op.
        }
    }

    /**
     * P2P test job.
     */
    public static class GridP2PTestJob extends ComputeJobAdapter {
        /**
         * @param arg Argument.
         */
        public GridP2PTestJob(String arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws GridException {
            // Return next value.
            return GridP2PTestStaticVariable.staticVar++;
        }
    }

    /**
     * P2P test task.
     */
    public static class GridP2PTestTask extends ComputeTaskAdapter<Serializable, Object> {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        @IgniteLocalNodeIdResource
        private UUID nodeId;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Serializable arg)
            throws GridException {
            Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());

            for (ClusterNode node : subgrid) {
                if (!node.id().equals(nodeId))
                    map.put(new GridP2PTestJob(null) , node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assert results.size() == 1;

            ComputeJobResult res = results.get(0);

            if (log.isInfoEnabled())
                log.info("Got job result for aggregation: " + res);

            if (res.getException() != null)
                throw res.getException();

            return res.getData();
        }
    }

    /**
     * P2p test task.
     */
    public static class GridP2PTestTask1 extends GridP2PTestTask {
        // No-op.
    }
}
