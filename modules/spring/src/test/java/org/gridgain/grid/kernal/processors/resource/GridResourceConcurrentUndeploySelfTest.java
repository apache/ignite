/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;
import org.springframework.context.support.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.kernal.processors.resource.GridResourceTestUtils.*;

/**
 * Tests concurrent undeployment of resources.
 */
@SuppressWarnings( {"PublicInnerClass", "BusyWait"})
@GridCommonTest(group = "Resource Self")
public class GridResourceConcurrentUndeploySelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private GridDeploymentMode depMode;

    /** Semaphore. */
    private static CountDownLatch cnt;

    /** Node executing all tasks. */
    private static UUID nodeToExec;

    /** Undeploy count. */
    private static int undeployCnt;

    /** Name of user resource class. */
    private static final String TEST_USER_RESOURCE = "org.gridgain.grid.tests.p2p.GridTestUserResource";

    /** */
    private static ClassLoader saveTask1Ldr;

    /** */
    private static ClassLoader saveTask2Ldr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Wait for specified event came.
     *
     * @param ignite grid for waiting.
     * @param type type of event.
     * @throws InterruptedException if thread was interrupted.
     */
    private void waitForEvent(Ignite ignite, int type) throws InterruptedException {
        Collection<IgniteEvent> evts;

        do {
            evts = ignite.events().localQuery(F.<IgniteEvent>alwaysTrue(), type);

            Thread.sleep(500);
        }
        while (!Thread.currentThread().isInterrupted() && evts.isEmpty());

        info("Events: " + evts);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        GridAbstractUserResource.resetResourceCounters();

        nodeToExec = null;

        cnt = null;

        undeployCnt = 0;
    }

    /**
     * @throws Exception if error occur.
     */
    public void testNodeLeftInSharedMode() throws Exception {
        depMode = GridDeploymentMode.SHARED;

        try {
            Ignite ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            Ignite ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            Ignite ignite3 = startGrid(3, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            nodeToExec = ignite2.cluster().localNode().id();

            cnt = new CountDownLatch(2);

            ComputeTaskFuture<?> res = executeAsync(ignite1.compute(), UserResourceTask1.class, true);

            executeAsync(ignite3.compute(), UserResourceTask2.class, false).get();

            cnt.await();

            assert saveTask1Ldr == saveTask2Ldr;

            G.stop(getTestGridName(3), true);

            undeployCnt++;

            Thread.sleep(500);

            checkUsageCount(GridAbstractUserResource.undeployClss, UserResource2.class, 0);

            checkUsageCount(GridAbstractUserResource.undeployClss, UserResource.class, 1);

            GridResourceIoc ioc = ((GridKernal) ignite2).context().resource().getResourceIoc();

            assert ioc.isCached(UserResource.class);

            info("Waiting for task to complete...");

            res.cancel();
        }
        finally {
            G.stop(getTestGridName(1), true);
            G.stop(getTestGridName(2), true);
            G.stop(getTestGridName(3), true);
        }
    }

    /**
     * @param mode deployment mode.
     * @throws Exception if error occur.
     */
    private void processTestLocalNode(GridDeploymentMode mode) throws Exception {
        depMode = mode;

        try {
            Ignite ignite = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            nodeToExec = ignite.cluster().localNode().id();

            cnt = new CountDownLatch(1);

            ComputeTaskFuture<?> res = executeAsync(ignite.compute(), UserResourceTask1.class, true);

            cnt.await();

            ignite.compute().undeployTask(UserResourceTask1.class.getName());

            GridResourceIoc ioc = ((GridKernal) ignite).context().resource().getResourceIoc();

            assert ioc.isCached(UserResource.class);

            res.cancel();

            info("Received task result.");

            waitForEvent(ignite, EVT_TASK_UNDEPLOYED);

            assert !ioc.isCached(UserResource.class);
        }
        finally {
            G.stop(getTestGridName(1), true);
        }
    }

    /**
     * @param mode deployment mode.
     * @throws Exception if error occur.
     */
    private void processTestRemoteNode(GridDeploymentMode mode) throws Exception {
        depMode = mode;

        try {
            Ignite ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            Ignite ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            nodeToExec = ignite2.cluster().localNode().id();

            cnt = new CountDownLatch(1);

            ComputeTaskFuture<?> res = executeAsync(ignite1.compute(), UserResourceTask1.class, true);

            cnt.await();

            ignite1.compute().undeployTask(UserResourceTask1.class.getName());

            undeployCnt++;

            Thread.sleep(1000);

            GridResourceIoc ioc = ((GridKernal) ignite2).context().resource().getResourceIoc();

            assert ioc.isCached(UserResource.class);

            res.cancel();

            waitForEvent(ignite2, EVT_TASK_UNDEPLOYED);

            assert !ioc.isCached(UserResource.class);
        }
        finally {
            G.stop(getTestGridName(1), true);
            G.stop(getTestGridName(2), true);
        }
    }

    /**
     * @param mode deployment mode.
     * @throws Exception if error occur.
     */
    @SuppressWarnings({"unchecked"})
    private void processTestRemoteNodeP2P(GridDeploymentMode mode) throws Exception {
        depMode = mode;

        try {
            Ignite ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            Ignite ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            ClassLoader ldr = getExternalClassLoader();

            Class task1 = ldr.loadClass("org.gridgain.grid.tests.p2p.GridP2PTestTaskExternalPath1");

            ComputeTaskFuture res =
                executeAsync(ignite1.compute(), task1, new Object[] {ignite2.cluster().localNode().id(), true});

            waitForEvent(ignite2, EVT_JOB_STARTED);

            ignite1.compute().undeployTask(task1.getName());

            Thread.sleep(500);

            GridResourceIoc ioc = ((GridKernal) ignite2).context().resource().getResourceIoc();

            assert ioc.isCached(TEST_USER_RESOURCE);

            res.cancel();

            waitForEvent(ignite2, EVT_TASK_UNDEPLOYED);

            assert !ioc.isCached(TEST_USER_RESOURCE);
        }
        finally {
            G.stop(getTestGridName(1), true);
            G.stop(getTestGridName(2), true);
        }
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testLocalNodePrivateMode() throws Exception {
        processTestLocalNode(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testLocalNodeIsolatedMode() throws Exception {
        processTestLocalNode(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testLocalNodeContinuousMode() throws Exception {
        processTestLocalNode(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testLocalNodeSharedMode() throws Exception {
        processTestLocalNode(GridDeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testRemoteNodePrivateMode() throws Exception {
        processTestRemoteNode(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testRemoteNodeIsolatedMode() throws Exception {
        processTestRemoteNode(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testRemoteNodeContinuousMode() throws Exception {
        processTestRemoteNode(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testRemoteNodeSharedMode() throws Exception {
        processTestRemoteNode(GridDeploymentMode.SHARED);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testP2PRemoteNodePrivateMode() throws Exception {
        processTestRemoteNodeP2P(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testP2PRemoteNodeIsolatedMode() throws Exception {
        processTestRemoteNodeP2P(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testP2PRemoteNodeContinuousMode() throws Exception {
        processTestRemoteNodeP2P(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testP2PRemoteNodeSharedMode() throws Exception {
        processTestRemoteNodeP2P(GridDeploymentMode.SHARED);
    }

    /**
     * Simple resource.
     */
    public static class UserResource extends GridAbstractUserResource {
        // No-op.
    }

    /**
     * Simple resource.
     */
    public static class UserResource2 extends GridAbstractUserResource {
        // No-op.
    }

    /** */
    public static class UserResourceTask2 extends UserResourceTask1 {
        /** {@inheritDoc} */
        @Override protected void saveLdr(ClassLoader ldr) {
            saveTask2Ldr = ldr;
        }
    }

    /** */
    public static class UserResourceTask1 extends ComputeTaskAdapter<Boolean, Object> {
        /** */
        @IgniteUserResource
        private transient UserResource rsrcTask;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Boolean arg) throws GridException {
            assert rsrcTask != null;

            for (ClusterNode node : subgrid) {
                if (node.id().equals(nodeToExec)) {
                    return Collections.singletonMap(new ComputeJobAdapter(arg) {
                        /** */
                        @SuppressWarnings("unused")
                        @IgniteUserResource
                        private transient UserResource2 rsrc2;

                        /** {@inheritDoc} */
                        @SuppressWarnings({"ObjectEquality"})
                        @Override public Serializable execute() {
                            saveLdr(getClass().getClassLoader());

                            if (cnt != null)
                                cnt.countDown();

                            Boolean arg = argument(0);

                            if (arg != null && arg) {
                                try {
                                    Thread.sleep(Long.MAX_VALUE);
                                }
                                catch (InterruptedException ignore) {
                                    // Task has been canceled.
                                }
                            }

                            checkUsageCount(GridAbstractUserResource.undeployClss, UserResource.class, undeployCnt);

                            return null;
                        }
                    }, node);
                }
            }

            throw new GridException("Node not found");
        }

        /**
         * @param ldr Class loader to save.
         */
        protected void saveLdr(ClassLoader ldr) {
            saveTask1Ldr = ldr;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assert rsrcTask != null;

            return null;
        }
    }
}
