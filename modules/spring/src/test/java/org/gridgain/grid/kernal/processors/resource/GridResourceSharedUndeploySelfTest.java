/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.springframework.context.support.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.resource.GridAbstractUserResource.*;
import static org.gridgain.grid.kernal.processors.resource.GridResourceTestUtils.*;

/**
 * Tests resources injection for the tasks executed in shared class loader undeploy mode.
 */
@GridCommonTest(group = "Resource Self")
public class GridResourceSharedUndeploySelfTest extends GridCommonAbstractTest {
    /** */
    private static Object task1Rsrc1;

    /** */
    private static Object task1Rsrc2;

    /** */
    private static Object task1Rsrc3;

    /** */
    private static Object task1Rsrc4;

    /** */
    private static Object task2Rsrc1;

    /** */
    private static Object task2Rsrc2;

    /** */
    private static Object task2Rsrc3;

    /** */
    private static Object task2Rsrc4;

    /** */
    public GridResourceSharedUndeploySelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        task1Rsrc1 = null;
        task1Rsrc2 = null;
        task1Rsrc3 = null;
        task1Rsrc4 = null;

        task2Rsrc1 = null;
        task2Rsrc2 = null;
        task2Rsrc3 = null;
        task2Rsrc4 = null;

        resetResourceCounters();
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(GridDeploymentMode.SHARED);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameTaskLocally() throws Exception {
        Ignite ignite = startGrid(0, new GridSpringResourceContextImpl(new GenericApplicationContext()));

        try {
            // Execute the same task twice.
            // 1 resource created locally
            ignite.compute().execute(SharedResourceTask1.class, null);
            ignite.compute().execute(SharedResourceTask1.class, null);

            checkUsageCount(createClss, UserResource1.class, 2);
            checkUsageCount(createClss, UserResource2.class, 2);

            checkUsageCount(deployClss, UserResource1.class, 2);
            checkUsageCount(deployClss, UserResource2.class, 2);
        }
        finally {
            GridTestUtils.close(ignite, log());
        }

        checkUsageCount(undeployClss, UserResource1.class, 2);
        checkUsageCount(undeployClss, UserResource2.class, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentTaskLocally() throws Exception {
        Ignite ignite = startGrid(0, new GridSpringResourceContextImpl(new GenericApplicationContext()));

        try {
            // Execute the tasks with the same class loaders.
            // 1 resource created locally
            ignite.compute().execute(SharedResourceTask1.class, null);
            ignite.compute().execute(SharedResourceTask2.class, null);

            checkUsageCount(createClss, UserResource1.class, 2);
            checkUsageCount(createClss, UserResource2.class, 2);

            checkUsageCount(deployClss, UserResource1.class, 2);
            checkUsageCount(deployClss, UserResource2.class, 2);
        }
        finally {
            GridTestUtils.close(ignite, log());
        }

        checkUsageCount(undeployClss, UserResource1.class, 2);
        checkUsageCount(undeployClss, UserResource2.class, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDifferentTaskNameLocally() throws Exception {
        Ignite ignite = startGrid(0, new GridSpringResourceContextImpl(new GenericApplicationContext()));

        // Versions are different - should not share
        // 2 resource created locally
        try {
            ignite.compute().execute(SharedResourceTask1Version1.class, null);

            try {
                ignite.compute().execute(SharedResourceTask1Version2.class, null);

                assert false : "SharedResourceTask4 should not be allowed to deploy.";
            }
            catch (GridException e) {
                info("Received expected exception: " + e);
            }
        }
        finally {
            GridTestUtils.close(ignite, log());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    public void testDifferentTasks() throws Exception {
        Ignite ignite1 = null;
        Ignite ignite2 = null;

        try {
            ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            // Execute different tasks.
            ignite1.compute().execute(SharedResourceTask1.class, null);
            ignite1.compute().execute(SharedResourceTask2.class, null);

            // In ISOLATED_CLASSLOADER mode tasks should have the class
            // loaders because they have the same CL locally and thus the same
            // resources.
            // So 1 resource locally and 1 remotely
            assert task1Rsrc1 == task2Rsrc1;
            assert task1Rsrc2 == task2Rsrc2;
            assert task1Rsrc3 == task2Rsrc3;
            assert task1Rsrc4 == task2Rsrc4;

            checkUsageCount(createClss, UserResource1.class, 4);
            checkUsageCount(createClss, UserResource2.class, 4);

            checkUsageCount(deployClss, UserResource1.class, 4);
            checkUsageCount(deployClss, UserResource2.class, 4);
        }
        finally {
            GridTestUtils.close(ignite1, log());
            GridTestUtils.close(ignite2, log());
        }

        checkUsageCount(undeployClss, UserResource1.class, 4);
        checkUsageCount(undeployClss, UserResource2.class, 4);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    public void testUndeployedTask() throws Exception {
        Ignite ignite1 = null;
        Ignite ignite2 = null;

        try {
            ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            // Execute tasks.
            ignite1.compute().execute(SharedResourceTask1.class, null);
            ignite1.compute().execute(SharedResourceTask2.class, null);

            ignite1.compute().undeployTask(SharedResourceTask1.class.getName());

            // Wait until resources get undeployed remotely
            // because undeploy is asynchronous apply.
            Thread.sleep(3000);

            // 1 local and 1 remote resource instances
            checkUsageCount(createClss, UserResource1.class, 4);
            checkUsageCount(deployClss, UserResource1.class, 4);
            checkUsageCount(createClss, UserResource2.class, 4);
            checkUsageCount(deployClss, UserResource2.class, 4);
            checkUsageCount(undeployClss, UserResource1.class, 4);
            checkUsageCount(undeployClss, UserResource2.class, 4);

            ignite1.compute().undeployTask(SharedResourceTask2.class.getName());

            // Wait until resources get undeployed remotely
            // because undeploy is asynchronous apply.
            Thread.sleep(3000);

            // We undeployed last task for this class loader and resources.
            // All resources should be undeployed.
            checkUsageCount(undeployClss, UserResource1.class, 4);
            checkUsageCount(undeployClss, UserResource2.class, 4);

            // Execute the same tasks.
            ignite1.compute().execute(SharedResourceTask1.class, null);
            ignite1.compute().execute(SharedResourceTask2.class, null);

            // 2 new resources.
            checkUsageCount(createClss, UserResource1.class, 8);
            checkUsageCount(deployClss, UserResource1.class, 8);
            checkUsageCount(createClss, UserResource2.class, 8);
            checkUsageCount(deployClss, UserResource2.class, 8);
        }
        finally {
            GridTestUtils.close(ignite1, log());
            GridTestUtils.close(ignite2, log());
        }

        checkUsageCount(undeployClss, UserResource1.class, 8);
        checkUsageCount(undeployClss, UserResource2.class, 8);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testRedeployedTask() throws Exception {
        Ignite ignite = startGrid(0, new GridSpringResourceContextImpl(new GenericApplicationContext()));

        try {
            // Execute same task with different class loaders. Second execution should redeploy first one.
            ignite.compute().execute(SharedResourceTask1.class, null);

            checkUsageCount(createClss, UserResource1.class, 2);
            checkUsageCount(createClss, UserResource2.class, 2);

            checkUsageCount(deployClss, UserResource1.class, 2);
            checkUsageCount(deployClss, UserResource2.class, 2);

            // Change class loader of the task. So it's just implicit redeploy.
            ClassLoader tstClsLdr = new GridTestClassLoader(null, getClass().getClassLoader(),
                SharedResourceTask1.class.getName(),
                GridResourceSharedUndeploySelfTest.SharedResourceTask1.GridSharedJob1.class.getName(),
                GridResourceSharedUndeploySelfTest.class.getName());

            Class<? extends ComputeTask<Object, Object>> taskCls =
                (Class<? extends ComputeTask<Object, Object>>)tstClsLdr.loadClass(
                    SharedResourceTask1.class.getName());

            ignite.compute().execute(taskCls, null);

            // Old resources should be undeployed at this point.
            checkUsageCount(undeployClss, UserResource1.class, 2);
            checkUsageCount(undeployClss, UserResource2.class, 2);

            // We should detect redeployment and create new resources.
            checkUsageCount(createClss, UserResource1.class, 4);
            checkUsageCount(createClss, UserResource2.class, 4);

            checkUsageCount(deployClss, UserResource1.class, 4);
            checkUsageCount(deployClss, UserResource2.class, 4);
        }
        finally {
            GridTestUtils.close(ignite, log());
        }

        checkUsageCount(undeployClss, UserResource1.class, 4);
        checkUsageCount(undeployClss, UserResource2.class, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameTaskFromTwoNodesUndeploy() throws Exception {
        Ignite ignite1 = null;
        Ignite ignite2 = null;
        Ignite ignite3 = null;

        try {
            ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            ignite3 = startGrid(3, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            ignite1.compute().execute(SharedResourceTask1.class, null);
            ignite2.compute().execute(SharedResourceTask1.class, null);

            checkUsageCount(createClss, UserResource1.class, 6);
            checkUsageCount(deployClss, UserResource1.class, 6);
            checkUsageCount(createClss, UserResource2.class, 6);
            checkUsageCount(deployClss, UserResource2.class, 6);

            checkUsageCount(undeployClss, UserResource1.class, 0);
            checkUsageCount(undeployClss, UserResource2.class, 0);

            ignite1.compute().undeployTask(SharedResourceTask1.class.getName());

            // Wait until resources get undeployed remotely
            // because undeploy is asynchronous apply.
            Thread.sleep(3000);

            checkUsageCount(undeployClss, UserResource1.class, 6);
            checkUsageCount(undeployClss, UserResource2.class, 6);

            ignite2.compute().undeployTask(SharedResourceTask1.class.getName());

            // Wait until resources get undeployed remotely
            // because undeploy is asynchronous apply.
            Thread.sleep(3000);

            // All Tasks from originating nodes were undeployed. All resources should be cleaned up.
            checkUsageCount(undeployClss, UserResource1.class, 6);
            checkUsageCount(undeployClss, UserResource2.class, 6);
        }
        finally {
            GridTestUtils.close(ignite1, log());
            GridTestUtils.close(ignite2, log());
            GridTestUtils.close(ignite3, log());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSameTaskFromTwoNodesLeft() throws Exception {
        Ignite ignite1 = null;
        Ignite ignite2 = null;
        Ignite ignite3 = null;

        try {
            ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            ignite3 = startGrid(3, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            ignite1.compute().execute(SharedResourceTask1.class, null);
            ignite2.compute().execute(SharedResourceTask1.class, null);

            checkUsageCount(createClss, UserResource1.class, 6);
            checkUsageCount(deployClss, UserResource1.class, 6);
            checkUsageCount(createClss, UserResource2.class, 6);
            checkUsageCount(deployClss, UserResource2.class, 6);

            checkUsageCount(undeployClss, UserResource1.class, 0);
            checkUsageCount(undeployClss, UserResource2.class, 0);

            GridTestUtils.close(ignite1, log());

            // Wait until other nodes get notified
            // this grid1 left.
            Thread.sleep(1000);

            // Undeployment happened only on Grid1.
            checkUsageCount(undeployClss, UserResource1.class, 2);
            checkUsageCount(undeployClss, UserResource2.class, 2);

            GridTestUtils.close(ignite2, log());

            // Wait until resources get undeployed remotely
            // because undeploy is asynchronous apply.
            Thread.sleep(1000);

            // Grid1 and Grid2
            checkUsageCount(undeployClss, UserResource1.class, 4);
            checkUsageCount(undeployClss, UserResource2.class, 4);
        }
        finally {
            GridTestUtils.close(ignite1, log());
            GridTestUtils.close(ignite2, log());
            GridTestUtils.close(ignite3, log());
        }
    }

    /** */
    public static class UserResource1 extends GridAbstractUserResource {
        // No-op.
    }

    /** */
    public static class UserResource2 extends GridAbstractUserResource {
        // No-op.
    }

    /** */
    @ComputeTaskName("SharedResourceTask1")
    public static class SharedResourceTask1Version1 extends SharedResourceTask1 {
        // No-op.
    }

    /** */
    @ComputeTaskName("SharedResourceTask1")
    public static class SharedResourceTask1Version2 extends SharedResourceTask1 {
        // No-op.
    }

    /** */
    public static class SharedResourceTask1 extends ComputeTaskSplitAdapter<Object, Object> {
        /** User resource.  */
        @IgniteUserResource(resourceClass = UserResource1.class)
        private transient GridAbstractUserResource rsrc1;

        /** User resource. */
        @IgniteUserResource
        private transient UserResource2 rsrc2;

        /** User resource.  */
        @IgniteUserResource(resourceClass = UserResource1.class, resourceName = "rsrc3")
        private transient GridAbstractUserResource rsrc3;

        /** User resource. */
        @IgniteUserResource(resourceName = "rsrc4")
        private transient UserResource2 rsrc4;

        /** */
        @IgniteLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<ComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
            assert rsrc1 != null;
            assert rsrc2 != null;
            assert rsrc3 != null;
            assert rsrc4 != null;
            assert log != null;

            log.info("Injected shared resource1 into task: " + rsrc1);
            log.info("Injected shared resource2 into task: " + rsrc2);
            log.info("Injected shared resource3 into task: " + rsrc3);
            log.info("Injected shared resource4 into task: " + rsrc4);
            log.info("Injected log resource into task: " + log);

            task1Rsrc1 = rsrc1;
            task1Rsrc2 = rsrc2;
            task1Rsrc3 = rsrc3;
            task1Rsrc4 = rsrc4;

            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++)
                jobs.add(new GridSharedJob1());

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assert rsrc1 != null;
            assert rsrc2 != null;
            assert rsrc3 != null;
            assert rsrc4 != null;
            assert log != null;

            // Nothing to reduce.
            return null;
        }

        /**
         * Job class for the 1st task. To avoid illegal
         * access when loading class with different class loader.
         */
        public final class GridSharedJob1 extends ComputeJobAdapter {
            /** User resource. */
            @IgniteUserResource(resourceClass = UserResource1.class)
            private transient GridAbstractUserResource rsrc5;

            /** Global resource. */
            @IgniteUserResource
            private transient UserResource2 rsrc6;

            /** User resource. */
            @IgniteUserResource(resourceClass = UserResource1.class, resourceName = "rsrc3")
            private transient GridAbstractUserResource rsrc7;

            /** Global resource. */
            @IgniteUserResource(resourceName = "rsrc4")
            private transient UserResource2 rsrc8;

            /** {@inheritDoc} */
            @SuppressWarnings({"ObjectEquality"})
            @Override public Serializable execute() {
                assert rsrc1 != null;
                assert rsrc2 != null;
                assert rsrc3 != null;
                assert rsrc4 != null;
                assert log != null;

                assert rsrc5 != null;
                assert rsrc6 != null;
                assert rsrc7 != null;
                assert rsrc8 != null;

                // Make sure that neither task nor global scope got
                // created more than once.
                assert rsrc1 == rsrc5;
                assert rsrc2 == rsrc6;
                assert rsrc3 == rsrc7;
                assert rsrc4 == rsrc8;

                log.info("Injected shared resource1 into job: " + rsrc1);
                log.info("Injected shared resource2 into job: " + rsrc2);
                log.info("Injected shared resource3 into job: " + rsrc3);
                log.info("Injected shared resource4 into job: " + rsrc4);
                log.info("Injected shared resource5 into job: " + rsrc5);
                log.info("Injected shared resource6 into job: " + rsrc6);
                log.info("Injected shared resource7 into job: " + rsrc7);
                log.info("Injected shared resource8 into job: " + rsrc8);
                log.info("Injected log resource into job: " + log);

                return null;
            }
        }
    }

    /** */
    public static class SharedResourceTask2 extends ComputeTaskSplitAdapter<Object, Object> {
        /** User resource.  */
        @IgniteUserResource(resourceClass = UserResource1.class)
        private transient GridAbstractUserResource rsrc1;

        /** User resource. */
        @IgniteUserResource
        private transient UserResource2 rsrc2;

        /** User resource.  */
        @IgniteUserResource(resourceClass = UserResource1.class, resourceName = "rsrc3")
        private transient GridAbstractUserResource rsrc3;

        /** User resource. */
        @IgniteUserResource(resourceName = "rsrc4")
        private transient UserResource2 rsrc4;

        /** */
        @IgniteLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<ComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
            assert rsrc1 != null;
            assert rsrc2 != null;
            assert rsrc3 != null;
            assert rsrc4 != null;
            assert log != null;

            log.info("Injected shared resource1 into task: " + rsrc1);
            log.info("Injected shared resource2 into task: " + rsrc2);
            log.info("Injected shared resource3 into task: " + rsrc3);
            log.info("Injected shared resource4 into task: " + rsrc4);
            log.info("Injected log resource into task: " + log);

            task2Rsrc1 = rsrc1;
            task2Rsrc2 = rsrc2;
            task2Rsrc3 = rsrc3;
            task2Rsrc4 = rsrc4;

            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    /** User resource. */
                    @IgniteUserResource(resourceClass = UserResource1.class)
                    private transient GridAbstractUserResource rsrc5;

                    /** User resource */
                    @IgniteUserResource
                    private transient UserResource2 rsrc6;

                    /** User resource. */
                    @IgniteUserResource(resourceClass = UserResource1.class, resourceName = "rsrc3")
                    private transient GridAbstractUserResource rsrc7;

                    /** User resource */
                    @IgniteUserResource(resourceName = "rsrc4")
                    private transient UserResource2 rsrc8;

                    /** {@inheritDoc} */
                    @SuppressWarnings({"ObjectEquality"})
                    @Override public Serializable execute() {
                        assert rsrc1 != null;
                        assert rsrc2 != null;
                        assert rsrc3 != null;
                        assert rsrc4 != null;
                        assert log != null;

                        assert rsrc5 != null;
                        assert rsrc6 != null;
                        assert rsrc7 != null;
                        assert rsrc8 != null;

                        // Make sure that neither task nor global scope got
                        // created more than once.
                        assert rsrc1 == rsrc5;
                        assert rsrc2 == rsrc6;
                        assert rsrc3 == rsrc7;
                        assert rsrc4 == rsrc8;

                        log.info("Injected shared resource1 into job: " + rsrc1);
                        log.info("Injected shared resource2 into job: " + rsrc2);
                        log.info("Injected shared resource3 into job: " + rsrc3);
                        log.info("Injected shared resource4 into job: " + rsrc4);
                        log.info("Injected shared resource5 into job: " + rsrc5);
                        log.info("Injected shared resource6 into job: " + rsrc6);
                        log.info("Injected shared resource7 into job: " + rsrc7);
                        log.info("Injected shared resource8 into job: " + rsrc8);
                        log.info("Injected log resource into job: " + log);

                        return null;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assert rsrc1 != null;
            assert rsrc2 != null;
            assert rsrc3 != null;
            assert rsrc4 != null;
            assert log != null;

            // Nothing to reduce.
            return null;
        }
    }

}
