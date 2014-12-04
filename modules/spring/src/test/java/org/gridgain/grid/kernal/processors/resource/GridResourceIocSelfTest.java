/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.deployment.uri.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;
import org.springframework.context.support.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.processors.resource.GridAbstractUserResource.*;
import static org.gridgain.grid.kernal.processors.resource.GridResourceTestUtils.*;

/**
 * Tests for {@link GridResourceIoc} class.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Resource Self")
public class GridResourceIocSelfTest extends GridCommonAbstractTest {
    /** Name of user resource class from gar file. */
    private static final String TEST_USER_RSRC = "org.gridgain.grid.tests.p2p.GridTestUserResource";

    /** Name of task from gar file. */
    private static final String TEST_EXT_TASK = "org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1";

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

    /** Path to GAR file. */
    private String garFile;

    /**
     * Constructor.
     */
    public GridResourceIocSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName,
        GridTestResources rsrcs) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName, rsrcs);

        cfg.setDeploymentMode(depMode);

        cfg.setCacheConfiguration();
        cfg.setIncludeProperties();
        cfg.setPeerClassLoadingLocalClassPathExclude("org.gridgain.grid.kernal.processors.resource.*");

        if (garFile != null) {
            GridUriDeploymentSpi depSpi = new GridUriDeploymentSpi();

            depSpi.setUriList(Collections.singletonList(garFile));

            cfg.setDeploymentSpi(depSpi);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        resetResourceCounters();
    }

    /**
     * Executes test task on one node and explicitly undeploy it.
     * Checks ioc for both deployed and undeployed states.
     *
     * @throws Exception If failed.
     */
    private void processTestWithUndeploy() throws Exception {
        try {
            Ignite ignite = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            GridResourceIoc ioc = ((GridKernal) ignite).context().resource().getResourceIoc();

            ignite.compute().execute(TestTask.class, null);

            checkUsageCount(createClss, UserResource1.class, 1);
            checkUsageCount(deployClss, UserResource1.class, 1);

            assert ioc.isCached(UserResource1.class);

            ignite.compute().undeployTask(TestTask.class.getName());

            assert !ioc.isCached(UserResource1.class);

            checkUsageCount(undeployClss, UserResource1.class, 1);
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * Executes test task on two nodes and then stop master node.
     * Checks ioc before master node stop and after.
     *
     * @throws Exception If failed.
     */
    private void processTestWithNodeStop() throws Exception {
        try {
            Ignite ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            Ignite ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            final CountDownLatch latch = new CountDownLatch(2);

            ignite2.events().localListen(
                new IgnitePredicate<GridEvent>() {
                    @Override public boolean apply(GridEvent evt) {
                        info("Received event: " + evt);

                        latch.countDown();

                        return true;
                    }
                }, EVT_TASK_UNDEPLOYED
            );

            GridResourceIoc ioc = ((GridKernal) ignite2).context().resource().getResourceIoc();

            ignite1.compute().execute(TestTask.class, null);

            checkUsageCount(createClss, UserResource1.class, 1);
            checkUsageCount(deployClss, UserResource1.class, 1);

            assert ioc.isCached(UserResource1.class);

            stopGrid(1);

            Thread.sleep(2000);

            assert !ioc.isCached(UserResource1.class);

            checkUsageCount(undeployClss, UserResource1.class, 1);

            assert latch.await(5000, MILLISECONDS);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * Executes tasks from deployed gar file and checks ioc cache on deployed/undeployed events.
     *
     * @throws Exception If error occur.
     */
    private void processTestGarUndeployed() throws Exception {
        String garDir = "modules/extdata/p2p/deploy";
        String garFileName = "p2p.gar";

        File origGarPath = U.resolveGridGainPath(garDir + '/' + garFileName);

        File tmpPath = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        if (!tmpPath.mkdir())
            throw new IOException("Can not create temp directory");

        try {
            File newGarFile = new File(tmpPath, garFileName);

            U.copy(origGarPath, newGarFile, false);

            assert newGarFile.exists();

            try {
                garFile = "file:///" + tmpPath.getAbsolutePath();

                try {
                    Ignite ignite = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));

                    int[] res = ignite.compute().execute(TEST_EXT_TASK, ignite.cluster().localNode().id());

                    assert res.length == 2;

                    GridResourceIoc ioc = ((GridKernal) ignite).context().resource().
                        getResourceIoc();

                    assert ioc.isCached(TEST_USER_RSRC);

                    if (!newGarFile.delete())
                        throw new IOException("Can not delete temp gar file");

                    newGarFile = null;

                    Thread.sleep(GridUriDeploymentSpi.DFLT_DISK_SCAN_FREQUENCY + 1000);

                    // Cache must contains no GridTestUserResource.
                    assert !ioc.isCached(TEST_USER_RSRC);

                    try {
                        ignite.compute().execute(TEST_EXT_TASK,
                            ignite.cluster().localNode().id());

                        assert false : "Task must be undeployed";
                    }
                    catch (GridException e) {
                        info("Caught expected exception: " + e);
                    }
                }
                finally {
                    stopGrid(1);
                }
            }
            finally {
                if (newGarFile != null && !newGarFile.delete())
                    error("Can not delete temp gar file");
            }
        }
        finally {
            if (!tmpPath.delete())
                error("Can not delete temp directory");
        }
    }

    /**
     * Simple task.
     */
    @SuppressWarnings({"PublicInnerClass"})
    @GridComputeTaskName("TestTask")
    public static class TestTask extends GridComputeTaskSplitAdapter<Object, Void> {
        /** User resource. */
        @GridUserResource
        private transient UserResource1 rsrc1;

        /** {@inheritDoc} */
        @Override protected Collection<GridComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
            assert rsrc1 != null;

            Collection<GridComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new GridComputeJobAdapter() {
                    /** User resource */
                    @GridUserResource
                    private transient UserResource1 rsrc2;

                    @Override public Serializable execute() {
                        assert rsrc2 != null;

                        return null;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class UserResource1 extends GridAbstractUserResource {
        // No-op.
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class UserResourceTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @GridUserResource(resourceClass = UserResource1.class)
        private transient Object rsrc1;

        /** {@inheritDoc} */
        @Nullable
        @Override protected Collection<GridComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
            assert rsrc1 != null;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assert rsrc1 != null;

            // Nothing to reduce.
            return null;
        }

        /**
         * @return Resource of anonymous class.
         */
        public UserResource2 createAnonymousResource() {
            return new UserResource2() {
                // No-op.
            };
        }

        /** */
        private static class UserResource2 extends GridAbstractUserResource {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIoc() throws Exception {
        GridResourceIoc ioc = new GridResourceIoc();

        UserResourceTask task = new UserResourceTask();

        GridDeployment dep = new GridTestDeployment(GridDeploymentMode.PRIVATE, task.getClass().getClassLoader(),
            IgniteUuid.randomUuid(), "", task.getClass().getName(), false);

        dep.addDeployedClass(task.getClass());

        UserResourceTask.UserResource2 rsrc;

        // Nullable caching key.
        ioc.inject(rsrc = task.createAnonymousResource(), GridUserResource.class,
            new GridResourceBasicInjector<>(null), dep, task.getClass());

        info("Injected resources.");

        ioc.onUndeployed(UserResourceTask.class.getClassLoader());

        info("Cleanup resources.");

        for (Class<?> cls = UserResourceTask.class; !cls.equals(Object.class); cls = cls.getSuperclass())
            assert !ioc.isCached(cls) : "Class must be removed from cache: " + cls.getName();

        for (Class<?> cls = rsrc.getClass(); !cls.equals(Object.class); cls = cls.getSuperclass())
            assert !ioc.isCached(cls) : "Class must be removed from cache: " + cls.getName();
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployPrivateMode() throws Exception {
        depMode = GridDeploymentMode.PRIVATE;

        processTestWithUndeploy();
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployIsolatedMode() throws Exception {
        depMode = GridDeploymentMode.ISOLATED;

        processTestWithUndeploy();
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeployContinuousMode() throws Exception {
        depMode = GridDeploymentMode.CONTINUOUS;

        processTestWithUndeploy();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testUndeploySharedMode() throws Exception {
        depMode = GridDeploymentMode.SHARED;

        processTestWithUndeploy();
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testStopNodePrivateMode() throws Exception {
        depMode = GridDeploymentMode.PRIVATE;

        processTestWithNodeStop();
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testStopNodeIsolatedMode() throws Exception {
        depMode = GridDeploymentMode.ISOLATED;

        processTestWithNodeStop();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testStopNodeSharedMode() throws Exception {
        depMode = GridDeploymentMode.SHARED;

        processTestWithNodeStop();
    }

    /**
     * Test GridDeploymentMode.PRIVATE mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarPrivateMode() throws Exception {
        depMode = GridDeploymentMode.PRIVATE;

        processTestGarUndeployed();
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarIsolatedMode() throws Exception {
        depMode = GridDeploymentMode.ISOLATED;

        processTestGarUndeployed();
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarContinuousMode() throws Exception {
        depMode = GridDeploymentMode.CONTINUOUS;

        processTestGarUndeployed();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarSharedMode() throws Exception {
        depMode = GridDeploymentMode.SHARED;

        processTestGarUndeployed();
    }
}
