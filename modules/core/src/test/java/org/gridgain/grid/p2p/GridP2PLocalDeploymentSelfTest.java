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
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Test to make sure that if job executes on the same node, it reuses the same class loader as task.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ObjectEquality"})
@GridCommonTest(group = "P2P")
public class GridP2PLocalDeploymentSelfTest extends GridCommonAbstractTest {
    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private GridDeploymentMode depMode;

    /** */
    private static UserResource jobRsrc;

    /** */
    private static UserResource taskRsrc;

    /** */
    private static ClassLoader jobLdr;

    /** */
    private static ClassLoader taskLdr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Process one test.
     * @param depMode deployment mode.
     * @throws Exception if error occur.
     */
    private void processSharedModeTest(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ignite1.compute().execute(TestTask.class, ignite2.cluster().localNode().id());

            assert jobRsrc != taskRsrc;

            UserResource saveTaskRsrc = taskRsrc;
            UserResource saveJobRsrc = jobRsrc;

            ClassLoader saveTaskLdr = taskLdr;
            ClassLoader saveJobLdr = jobLdr;

            ignite2.compute().execute(TestTask.class, ignite1.cluster().localNode().id());

            assert saveJobRsrc == taskRsrc;
            assert saveTaskRsrc == jobRsrc;

            assert saveTaskLdr == jobLdr;
            assert saveJobLdr == taskLdr;
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception if error occur.
     */
    @SuppressWarnings({"unchecked"})
    public void testLocalDeployment() throws Exception {
        depMode = GridDeploymentMode.PRIVATE;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);
            Ignite ignite3 = startGrid(3);

            ClassLoader ldr1 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());
            ClassLoader ldr2 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());
            ClassLoader ldr3 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());

            Class taskCls = ldr1.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");

            ignite1.compute().execute(taskCls, ignite1.cluster().localNode().id());

            taskCls = ldr2.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");

            int[] res1 = (int[]) ignite2.compute().execute(taskCls, ignite1.cluster().localNode().id());

            taskCls = ldr3.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");

            int[] res2 = (int[]) ignite3.compute().execute(taskCls, ignite1.cluster().localNode().id());

            assert res1[0] != res2[0]; // Resources are not same.
            assert res1[1] != res2[1]; // Class loaders are not same.
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }

    /**
     * Process one test.
     * @param depMode deployment mode.
     * @throws Exception if error occur.
     */
    @SuppressWarnings({"unchecked"})
    private void processIsolatedModeTest(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ClassLoader ldr1 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());
            ClassLoader ldr2 = new URLClassLoader(
                new URL[] {new URL ( GridTestProperties.getProperty("p2p.uri.cls")) }, getClass().getClassLoader());

            Class task1 = ldr1.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");
            Class task2 = ldr2.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");

            int[] res1 = (int[]) ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            int[] res2 = (int[]) ignite2.compute().execute(task2, ignite1.cluster().localNode().id());

            assert res1[1] != res2[1]; // Class loaders are not same.
            assert res1[0] != res2[0]; // Resources are not same.

            assert res1[1] != System.identityHashCode(ldr1);
            assert res2[1] != System.identityHashCode(ldr2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processIsolatedModeTest(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processIsolatedModeTest(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processSharedModeTest(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processSharedModeTest(GridDeploymentMode.SHARED);
    }

    /**
     * Simple resource.
     */
    public static class UserResource {
        // No-op.
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    public static class TestTask extends GridComputeTaskAdapter<UUID, Serializable> {
        /** User resource. */
        @GridUserResource private transient UserResource rsrc;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, ClusterNode> map(final List<ClusterNode> subgrid, UUID arg)
            throws GridException {

            taskRsrc = rsrc;

            taskLdr = getClass().getClassLoader();

            for (ClusterNode node : subgrid) {
                if (node.id().equals(arg))
                    return Collections.singletonMap(new TestJob(arg), node);
            }

            throw new GridException("Failed to find target node: " + arg);
        }

        /** {@inheritDoc} */
        @Override public int[] reduce(List<GridComputeJobResult> results) throws GridException {
            assert results.size() == 1;

            assert taskRsrc == rsrc;
            assert taskLdr == getClass().getClassLoader();

            return null;
        }

        /**
         * Simple job class.
         */
        public static class TestJob extends GridComputeJobAdapter {
            /** User resource. */
            @GridUserResource private transient UserResource rsrc;

            /** Local node ID. */
            @GridLocalNodeIdResource private UUID locNodeId;

            /**
             * @param nodeId Node ID for node this job is supposed to execute on.
             */
            public TestJob(UUID nodeId) { super(nodeId); }

            /** {@inheritDoc} */
            @Override public Serializable execute() throws GridException {
                assert locNodeId.equals(argument(0)) == true;

                jobRsrc = rsrc;

                jobLdr = getClass().getClassLoader();

                return null;
            }
        }
    }
}
