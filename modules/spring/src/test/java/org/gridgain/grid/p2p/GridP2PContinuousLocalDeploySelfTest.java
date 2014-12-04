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
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;
import org.springframework.context.support.*;

import java.io.*;
import java.util.*;

/**
 * The test do the following:
 *
 * First test:
 * 1. Start 3 nodes: N1, N2, N3
 * 2. execute task1 from N1 on N2.
 * 3. execute task2 from N3 on N2.
 * 4. Make sure that task1 and task2 share class loader on N2
 *    (of course assuming that they share class loader on their originating nodes).
 * 5. Make sure that user resources are created once and shared thereafter.
 *
 * Second Test:
 * 1. Start 3 nodes in SHARED_DEPLOY mode: N1, N2, N3
 * 2. execute task1 from N1 on N2.
 * 3. Stop N1.
 * 3. execute task2 from N3 on N2.
 * 4. Make sure that task1 and task2 share class loader on N2 (of course assuming that they share class loader on their originating nodes).
 * 5. Make sure that user resources are created once and shared thereafter.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "PublicInnerClass"})
@GridCommonTest(group = "P2P")
public class GridP2PContinuousLocalDeploySelfTest extends GridCommonAbstractTest {
    /** */
    private static UUID node2Id;

    /** */
    private static ClassLoader clsLdr1;

    /** */
    private static ClassLoader clsLdr2;

    /** */
    public GridP2PContinuousLocalDeploySelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(GridDeploymentMode.CONTINUOUS);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        GridP2PAbstractUserResource.resetResourceCounters();
    }

    /**
     * @throws Exception if error occur
     */
    public void testContinuousMode() throws Exception {
        try {
            Ignite ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            Ignite ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            Ignite ignite3 = startGrid(3, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            node2Id = ignite2.cluster().localNode().id();

            ignite1.compute().execute(SharedResourceTask1.class, null);
            ignite1.compute().execute(SharedResourceTask2.class, null);

            // 2 instances: one instance of resource for task and one instance for
            // job, because job execute on remote node.
            GridP2PAbstractUserResource.checkCreateCount(UserResource1.class, 2);
            GridP2PAbstractUserResource.checkCreateCount(UserResource2.class, 2);

            assertEquals(clsLdr1, clsLdr2);

            ignite3.compute().execute(SharedResourceTask1.class, null);
            ignite3.compute().execute(SharedResourceTask2.class, null);

            // 3 instances: one instance for each nodes.
            GridP2PAbstractUserResource.checkCreateCount(UserResource1.class, 3);
            GridP2PAbstractUserResource.checkCreateCount(UserResource2.class, 3);

            assertEquals(clsLdr1, clsLdr2);
        }
        finally {
            stopGrid(3);
            stopGrid(2);
            stopGrid(1);
        }

        GridP2PAbstractUserResource.checkUndeployCount(UserResource1.class, 3);
        GridP2PAbstractUserResource.checkUndeployCount(UserResource2.class, 3);
    }

    /**
     * @throws Exception if error occur
     */
    public void testContinuousModeNodeRestart() throws Exception {
        try {
            Ignite ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            Ignite ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            Ignite ignite3 = startGrid(3, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            node2Id = ignite2.cluster().localNode().id();

            ignite1.compute().execute(SharedResourceTask1.class, null);
            ignite1.compute().execute(SharedResourceTask2.class, null);

            GridP2PAbstractUserResource.checkCreateCount(UserResource1.class, 2);
            GridP2PAbstractUserResource.checkCreateCount(UserResource2.class, 2);

            assertEquals(clsLdr1, clsLdr2);

            stopGrid(1);

            Thread.sleep(2000);

            GridP2PAbstractUserResource.checkUndeployCount(UserResource1.class, 1);
            GridP2PAbstractUserResource.checkUndeployCount(UserResource2.class, 1);

            ignite3.compute().execute(SharedResourceTask1.class, null);
            ignite3.compute().execute(SharedResourceTask2.class, null);

            // 3 instances: one instance for each nodes.
            GridP2PAbstractUserResource.checkCreateCount(UserResource1.class, 3);
            GridP2PAbstractUserResource.checkCreateCount(UserResource2.class, 3);

            assertEquals(clsLdr1, clsLdr2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }

        GridP2PAbstractUserResource.checkUndeployCount(UserResource1.class, 3);
        GridP2PAbstractUserResource.checkUndeployCount(UserResource2.class, 3);
    }

    /** */
    public static class UserResource1 extends GridP2PAbstractUserResource {
        // No-op.
    }

    /** */
    public static class UserResource2 extends GridP2PAbstractUserResource {
        // No-op.
    }


    /**
     * First task.
     */
    public static class SharedResourceTask1 extends GridComputeTaskAdapter<Object, Object> {
        /** Logger. */
        @GridLoggerResource
        private GridLogger log;

        /** User resource.  */
        @GridUserResource(resourceClass = UserResource1.class)
        private transient GridP2PAbstractUserResource rsrc1;

        /** User resource. */
        @GridUserResource
        private transient UserResource2 rsrc2;

        /** Grid instance. */
        @GridInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) throws GridException {
            log.info("Injected resource1: " + rsrc1);
            log.info("Injected resource1: " + rsrc2);

            assert rsrc1 != null;
            assert rsrc2 != null;

            return Collections.<ComputeJob, ClusterNode>singletonMap(
                new GridSharedJob1(), ignite.cluster().node(node2Id));
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            // Nothing to reduce.
            return null;
        }
    }

    /**
     * Job class for the 1st task.
     */
    public static final class GridSharedJob1 extends ComputeJobAdapter {
        /**
         * User resource.
         */
        @GridUserResource(resourceClass = UserResource1.class)
        private transient GridP2PAbstractUserResource rsrc3;

        /**
         * Global resource.
         */
        @GridUserResource
        private transient UserResource2 rsrc4;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @SuppressWarnings({"ObjectEquality"})
        @Override public Serializable execute() {
            log.info("Injected shared resource1 into job: " + rsrc3);
            log.info("Injected shared resource2 into job: " + rsrc4);
            log.info("Injected log resource into job: " + log);

            clsLdr1 = getClass().getClassLoader();

            return null;
        }
    }

    /**
     * Second task.
     */
    public static class SharedResourceTask2 extends GridComputeTaskAdapter<Object, Object> {
        /** Logger. */
        @GridLoggerResource
        private GridLogger log;

        /** User resource.  */
        @GridUserResource(resourceClass = UserResource1.class)
        private transient GridP2PAbstractUserResource rsrc1;

        /** User resource. */
        @GridUserResource
        private transient UserResource2 rsrc2;

        /** Grid instance. */
        @GridInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) throws GridException {
            log.info("Injected resource1: " + rsrc1);
            log.info("Injected resource1: " + rsrc2);

            assert rsrc1 != null;
            assert rsrc2 != null;

            return Collections.<ComputeJob, ClusterNode>singletonMap(
                new GridSharedJob2(), ignite.cluster().node(node2Id));
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            // Nothing to reduce.
            return null;
        }
    }

    /**
     * Job class for the 2st task.
     */
    public static final class GridSharedJob2 extends ComputeJobAdapter {
        /** User resource. */
        @GridUserResource(resourceClass = UserResource1.class)
        private transient GridP2PAbstractUserResource rsrc3;

        /** Global resource. */
        @GridUserResource
        private transient UserResource2 rsrc4;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @SuppressWarnings({"ObjectEquality"})
        @Override public Serializable execute() {
            log.info("Injected shared resource1 into job: " + rsrc3);
            log.info("Injected shared resource2 into job: " + rsrc4);
            log.info("Injected log resource into job: " + log);

            clsLdr2 = getClass().getClassLoader();

            return null;
        }
    }
}
