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
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.springframework.context.*;
import org.springframework.context.support.*;

import javax.management.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.processors.resource.GridAbstractUserResource.*;
import static org.gridgain.grid.kernal.processors.resource.GridResourceTestUtils.*;

/**
 *
 */
public class GridResourceMethodOverrideInjectionSelfTest extends GridCommonAbstractTest {
    /** */
    public GridResourceMethodOverrideInjectionSelfTest() {
        super(/*start grid*/false);
    }

    /**
     * @throws Exception in the case of failures.
     */
    public void testMethodResourceOverride() throws Exception {
        Ignite ignite1 = null;
        Ignite ignite2 = null;

        try {
            ignite1 = startGrid(1, new GridSpringResourceContextImpl(new GenericApplicationContext()));
            ignite2 = startGrid(2, new GridSpringResourceContextImpl(new GenericApplicationContext()));

            ignite1.compute().execute(MethodResourceOverrideTask.class, null);

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

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class UserResource1 extends GridAbstractUserResource {
        // No-op.
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class UserResource2 extends GridAbstractUserResource {
        // No-op.
    }

    /**
     *
     */
    private abstract static class AbstractResourceTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        protected transient GridAbstractUserResource rsrc1;

        /** */
        protected transient UserResource2 rsrc2;

        /** */
        protected transient GridAbstractUserResource rsrc3;

        /** */
        protected transient UserResource2 rsrc4;

        /** */
        protected GridLogger log;

        /** */
        protected Ignite ignite;

        /** */
        protected UUID nodeId;

        /** */
        protected MBeanServer mbeanSrv;

        /** */
        protected ExecutorService exec;

        /** */
        protected String ggHome;

        /** */
        protected String gridName;

        /** */
        protected ApplicationContext springCtx;

        /** */
        protected ComputeTaskSession ses;

        /** */
        protected ComputeJobContext jobCtx;

        /** */
        protected transient ComputeTaskContinuousMapper mapper;

        /**
         * @param rsrc1 User defined resource.
         */
        @GridUserResource(resourceClass = UserResource1.class)
        @SuppressWarnings("unused")
        public void setResource1(GridAbstractUserResource rsrc1) {
            this.rsrc1 = rsrc1;
        }

        /**
         * @param rsrc2 User defined resource.
         */
        @GridUserResource
        @SuppressWarnings("unused")
        protected void setResource2(UserResource2 rsrc2) {
            this.rsrc2 = rsrc2;
        }

        /**
         * @param rsrc3 User defined resource.
         */
        @GridUserResource(resourceClass = UserResource1.class, resourceName = "rsrc3")
        @SuppressWarnings("unused")
        public void setResource3(GridAbstractUserResource rsrc3) {
            this.rsrc3 = rsrc3;
        }

        /**
         * @param rsrc4 User defined resource.
         */
        @GridUserResource(resourceName = "rsrc4")
        @SuppressWarnings("unused")
        public void setResource4(UserResource2 rsrc4) {
            this.rsrc4 = rsrc4;
        }

        /**
         * @param log GridLogger.
         */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @GridLoggerResource private void setLog(GridLogger log) { this.log = log; }

        /**
         * @param ignite Grid.
         */
        @IgniteInstanceResource
        @SuppressWarnings("unused")
        void setIgnite(Ignite ignite) { this.ignite = ignite; }

        /**
         * @param nodeId UUID.
         */
        @GridLocalNodeIdResource
        public void setNodeId(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * @param mbeanSrv MBeanServer.
         */
        @GridMBeanServerResource
        @SuppressWarnings("unused")
        protected void setMbeanServer(MBeanServer mbeanSrv) { this.mbeanSrv = mbeanSrv; }

        /**
         * @param exec ExecutorService.
         */
        @IgniteExecutorServiceResource
        @SuppressWarnings("unused")
        public void setExecutor(ExecutorService exec) {
            this.exec = exec;
        }

        /**
         * @param ggHome GridGain Home.
         */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @IgniteHomeResource
        private void setGridGainHome(String ggHome) { this.ggHome = ggHome; }

        /**
         * @param gridName Grid name.
         */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @IgniteHomeResource
        private void setGridName(String gridName) { this.gridName = gridName; }

        /**
         * @param springCtx Spring Application Context.
         */
        @GridSpringApplicationContextResource
        @SuppressWarnings("unused")
        void setSpringContext(ApplicationContext springCtx) { this.springCtx = springCtx; }

        /**
         * @param ses GridComputeTaskSession.
         */
        @GridTaskSessionResource
        @SuppressWarnings("unused")
        public void setSession(ComputeTaskSession ses) {
            this.ses = ses;
        }

        /**
         * @param jobCtx Job context.
         */
        @GridJobContextResource
        @SuppressWarnings("unused")
        public void setJobContext(ComputeJobContext jobCtx) {
            this.jobCtx = jobCtx;
        }

        /**
         * @param mapper continuous mapper.
         */
        @GridTaskContinuousMapperResource
        @SuppressWarnings("unused")
        public void setMapper(ComputeTaskContinuousMapper mapper) {
            this.mapper = mapper;
        }
    }

    /**
     *
     */
    private static class MethodResourceOverrideTask extends AbstractResourceTask {
        /**
         * @param rsrc1 User resource.
         */
        @GridUserResource(resourceClass = UserResource1.class)
        @Override public void setResource1(GridAbstractUserResource rsrc1) {
            this.rsrc1 = rsrc1;
        }

        /**
         * @param rsrc2 UserResource2
         */
        @GridUserResource
        @Override public void setResource2(UserResource2 rsrc2) {
            this.rsrc2 = rsrc2;
        }

        /**
         * @param rsrc3 The grid resource.
         */
        @GridUserResource(resourceClass = UserResource1.class, resourceName = "rsrc3")
        @Override public void setResource3(GridAbstractUserResource rsrc3) {
            this.rsrc3 = rsrc3;
        }

        /**
         * @param rsrc4 The grid resource.
         */
        @GridUserResource(resourceName = "rsrc4")
        @Override public void setResource4(UserResource2 rsrc4) {
            this.rsrc4 = rsrc4;
        }

        /**
         * @param log GridLogger. The grid logger resource.
         */
        @SuppressWarnings({"MethodOverridesPrivateMethodOfSuperclass", "unused"})
        @GridLoggerResource
        public void setLog(GridLogger log) {
            this.log = log;
        }

        /**
         * @param ignite Grid.
         */
        @IgniteInstanceResource
        @Override public void setIgnite(Ignite ignite) {
            this.ignite = ignite;
        }

        /**
         * @param jobCtx Job context.
         */
        @GridJobContextResource
        @Override public void setJobContext(ComputeJobContext jobCtx) {
            this.jobCtx = jobCtx;

            log.info("-->setJobContext identity: " + System.identityHashCode(this));
        }

        /** {@inheritDoc} */
        @Override protected Collection<ComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
            assert rsrc1 != null;
            assert rsrc2 != null;
            assert rsrc3 != null;
            assert rsrc4 != null;
            assert log != null;
            assert ignite != null;
            assert nodeId != null;
            assert mbeanSrv != null;
            assert exec != null;
            assert ggHome != null;
            assert gridName != null;
            assert springCtx != null;
            assert ses != null;
            assert mapper != null;

            // Job context belongs to job, not task.
            assert jobCtx == null;
            log.info("-->Identity in the split: " + System.identityHashCode(this));

            log.info("Injected shared resource1 into task: " + rsrc1);
            log.info("Injected shared resource2 into task: " + rsrc2);
            log.info("Injected shared resource3 into task: " + rsrc3);
            log.info("Injected shared resource4 into task: " + rsrc4);
            log.info("Injected log resource into task: " + log);
            log.info("Injected grid resource into task: " + ignite);
            log.info("Injected nodeId resource into task: " + nodeId);
            log.info("Injected mbean server resource into task: " + mbeanSrv);
            log.info("Injected executor service resource into task: " + exec);
            log.info("Injected gridgain home resource into task: " + ggHome);
            log.info("Injected grid name resource into task: " + gridName);
            log.info("Injected spring context resource into task: " + springCtx);
            log.info("Injected session resource into task: " + ses);
            log.info("Injected continuous mapper: " + mapper);

            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    /** */
                    private transient GridAbstractUserResource rsrc5;

                    /** */
                    private transient UserResource2 rsrc6;

                    /** */
                    private transient GridAbstractUserResource rsrc7;

                    /** */
                    private transient UserResource2 rsrc8;

                    /** */
                    private ComputeJobContext jobCtx;

                    /**
                     * @param rsrc5 User resource.
                     */
                    @SuppressWarnings("unused")
                    @GridUserResource(resourceClass = UserResource1.class)
                    public void setResource5(GridAbstractUserResource rsrc5) { this.rsrc5 = rsrc5; }

                    /**
                     * @param rsrc6 User resource.
                     */
                    @SuppressWarnings("unused")
                    @GridUserResource
                    public void setResource6(UserResource2 rsrc6) { this.rsrc6 = rsrc6; }

                    /**
                     * @param rsrc7 User resource.
                     */
                    @SuppressWarnings("unused")
                    @GridUserResource(resourceClass = UserResource1.class, resourceName = "rsrc3")
                    public void setResource7(GridAbstractUserResource rsrc7) { this.rsrc7 = rsrc7; }

                    /**
                     * @param rsrc8 User resource.
                     */
                    @SuppressWarnings("unused")
                    @GridUserResource(resourceName = "rsrc4")
                    public void setResource8(UserResource2 rsrc8) { this.rsrc8 = rsrc8; }

                    /**
                     * @param jobCtx Job context.
                     */
                    @SuppressWarnings("unused")
                    @GridJobContextResource
                    public void setJobContext(ComputeJobContext jobCtx) { this.jobCtx = jobCtx; }

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        assert rsrc1 != null;
                        assert rsrc2 != null;
                        assert rsrc3 != null;
                        assert rsrc4 != null;
                        assert log != null;
                        assert ignite != null;
                        assert nodeId != null;
                        assert mbeanSrv != null;
                        assert exec != null;
                        assert ggHome != null;
                        assert gridName != null;
                        assert springCtx != null;
                        assert ses != null;
                        assert jobCtx != null;

                        assert rsrc5 != null;
                        assert rsrc6 != null;
                        assert rsrc7 != null;
                        assert rsrc8 != null;

                        //Job context is job resource, not task resource.
                        assert MethodResourceOverrideTask.this.jobCtx == null;
                        assert jobCtx != null;

                        log.info("Injected shared resource1 into job: " + rsrc1);
                        log.info("Injected shared resource2 into job: " + rsrc2);
                        log.info("Injected shared resource3 into job: " + rsrc3);
                        log.info("Injected shared resource4 into job: " + rsrc4);
                        log.info("Injected shared resource5 into job: " + rsrc5);
                        log.info("Injected shared resource6 into job: " + rsrc6);
                        log.info("Injected shared resource7 into job: " + rsrc7);
                        log.info("Injected shared resource8 into job: " + rsrc8);
                        log.info("Injected log resource into job: " + log);
                        log.info("Injected grid resource into job: " + ignite);
                        log.info("Injected nodeId resource into job: " + nodeId);
                        log.info("Injected mbean server resource into job: " + mbeanSrv);
                        log.info("Injected executor service resource into job: " + exec);
                        log.info("Injected gridgain home resource into job: " + ggHome);
                        log.info("Injected grid grid name resource into job: " + gridName);
                        log.info("Injected spring context resource into job: " + springCtx);
                        log.info("Injected session resource into job: " + ses);
                        log.info("Injected job context resource into job: " + jobCtx);

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
            assert ignite != null;
            assert nodeId != null;
            assert mbeanSrv != null;
            assert exec != null;
            assert ggHome != null;
            assert gridName != null;
            assert springCtx != null;
            assert ses != null;

            // Job context belongs to job, not task.
            assert jobCtx == null;

            log.info("-->Identity in reduce: " + System.identityHashCode(this));

            // Nothing to reduce.
            return null;
        }
    }
}
