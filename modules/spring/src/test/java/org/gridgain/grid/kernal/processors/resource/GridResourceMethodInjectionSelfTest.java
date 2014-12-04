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
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.springframework.beans.factory.support.*;
import org.springframework.context.*;
import org.springframework.context.support.*;

import javax.management.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.processors.resource.GridAbstractUserResource.*;
import static org.gridgain.grid.kernal.processors.resource.GridResourceTestUtils.*;

/**
 * Tests task resource injection.
 */
@GridCommonTest(group = "Resource Self")
@SuppressWarnings({"PublicInnerClass"})
public class GridResourceMethodInjectionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String SPRING_BEAN_RSRC_NAME = "test-bean";

    /** */
    public GridResourceMethodInjectionSelfTest() {
        super(/*start grid*/false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMethodInjection() throws Exception {
        Ignite ignite1 = null;
        Ignite ignite2 = null;
        try {
            GenericApplicationContext ctx = new GenericApplicationContext();

            RootBeanDefinition bf = new RootBeanDefinition();

            bf.setBeanClass(UserSpringBean.class);

            ctx.registerBeanDefinition(SPRING_BEAN_RSRC_NAME, bf);

            ctx.refresh();

            ignite1 = startGrid(1, new GridSpringResourceContextImpl(ctx));
            ignite2 = startGrid(2, new GridSpringResourceContextImpl(ctx));

            ignite1.compute().execute(UserResourceTask.class, null);

            checkUsageCount(createClss, UserResource1.class, 4);
            checkUsageCount(createClss, UserResource2.class, 4);
            checkUsageCount(createClss, UserResource3.class, 4);
            checkUsageCount(createClss, UserResource4.class, 4);
            checkUsageCount(createClss, UserResource5.class, 4);

            checkUsageCount(deployClss, UserResource1.class, 4);
            checkUsageCount(deployClss, UserResource2.class, 4);
            checkUsageCount(deployClss, UserResource3.class, 4);
            checkUsageCount(deployClss, UserResource4.class, 4);
        }
        finally {
            GridTestUtils.close(ignite1, log());
            GridTestUtils.close(ignite2, log());
        }
        checkUsageCount(deployClss, UserResource5.class, 8);

        checkUsageCount(undeployClss, UserResource1.class, 4);
        checkUsageCount(undeployClss, UserResource2.class, 4);
        checkUsageCount(undeployClss, UserResource3.class, 4);
        checkUsageCount(undeployClss, UserResource4.class, 4);
        checkUsageCount(undeployClss, UserResource5.class, 8);
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
    public static class UserResource3 extends GridAbstractUserResource {
        // No-op.
    }

    /** */
    public static class UserResource4 extends GridAbstractUserResource {
        // No-op.
    }

    /** */
    public static class UserResource5 extends GridAbstractUserResource {
        /** */
        private UserSpringBean springBean;

        /**
         * @param springBean Bean provided from Spring context.
         */
        @GridSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        @SuppressWarnings("unused")
        public void setSpringBean(UserSpringBean springBean) {
            this.springBean = springBean;
        }

        /**
         * Method must be called.
         * Parent GridAbstractUserResource#deploy() with the same annotation
         * must be called too.
         */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @GridUserResourceOnDeployed private void resourceDeploy() {
            addUsage(deployClss);

            assert springBean != null;
        }

        /**
         * Method must be called.
         * Parent GridAbstractUserResource#undeploy() with the same annotation
         * must be called too.
         */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @GridUserResourceOnUndeployed private void resourceUndeploy() {
            addUsage(undeployClss);

            assert springBean != null;
        }
    }

    /** */
    public static class UserSpringBean implements Serializable {
        // No-op.
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    public static class NonTransientUserResourceTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @GridUserResource(resourceClass = UserResource1.class)
        private GridAbstractUserResource rsrc;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws GridException {
            // Never reached.
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            // Never reached.
            assert false;

            return null;
        }
    }

    /** */
    public static class UserResourceTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        private transient GridAbstractUserResource rsrc1;

        /** */
        private transient UserResource2 rsrc2;

        /** */
        private transient GridAbstractUserResource rsrc3;

        /** */
        private transient UserResource2 rsrc4;

        /** */
        private GridLogger log;

        /** */
        private Ignite ignite;

        /** */
        private UUID nodeId;

        /** */
        private MBeanServer mbeanSrv;

        /** */
        private ExecutorService exec;

        /** */
        private String ggHome;

        /** */
        private String gridName;

        /** */
        private String locHost;

        /** */
        private GridMarshaller marshaller;

        /** */
        private ApplicationContext springCtx;

        /** */
        private GridComputeTaskSession ses;

        /** */
        private ComputeLoadBalancer balancer;

        /** */
        private ComputeJobContext jobCtx;

        /** */
        private UserSpringBean springBean;

        /** */
        private transient GridComputeTaskContinuousMapper mapper;

        /**
         * @param rsrc1 Resource 1.
         */
        @GridUserResource(resourceClass = UserResource1.class)
        public void setResource1(GridAbstractUserResource rsrc1) {
            this.rsrc1 = rsrc1;
        }

        /**
         * @param rsrc2 Resource 2.
         */
        @GridUserResource
        public void setResource2(UserResource2 rsrc2) {
            this.rsrc2 = rsrc2;
        }

        /**
         * @param rsrc3 Resource 3.
         */
        @GridUserResource(resourceClass = UserResource1.class, resourceName = "rsrc3")
        public void setResource3(GridAbstractUserResource rsrc3) {
            this.rsrc3 = rsrc3;
        }

        /**
         * @param rsrc4 Resource 4.
         */
        @GridUserResource(resourceName = "rsrc4")
        public void setResource4(UserResource2 rsrc4) {
            this.rsrc4 = rsrc4;
        }

        /**
         * @param log Logger.
         */
        @GridLoggerResource
        public void setLog(GridLogger log) {
            this.log = log;
        }

        /**
         * @param ignite Grid.
         */
        @GridInstanceResource
        public void setIgnite(Ignite ignite) {
            this.ignite = ignite;
        }

        /**
         * @param nodeId Node ID.
         */
        @GridLocalNodeIdResource
        public void setNodeId(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * @param mbeanSrv MBean server.
         */
        @GridMBeanServerResource
        public void setMbeanServer(MBeanServer mbeanSrv) {
            this.mbeanSrv = mbeanSrv;
        }

        /**
         * @param exec Executor.
         */
        @GridExecutorServiceResource
        public void setExecutor(ExecutorService exec) {
            this.exec = exec;
        }

        /**
         * @param ggHome GridGain home.
         */
        @GridHomeResource
        public void setGridgainHome(String ggHome) {
            this.ggHome = ggHome;
        }

        /**
         * @param gridName Grid name.
         */
        @GridNameResource
        public void setGridName(String gridName) {
            this.gridName = gridName;
        }

        /**
         * @param marshaller Marshaller.
         */
        @GridMarshallerResource
        public void setMarshaller(GridMarshaller marshaller) {
            this.marshaller = marshaller;
        }

        /**
         * @param springCtx Spring context.
         */
        @GridSpringApplicationContextResource
        public void setSpringContext(ApplicationContext springCtx) {
            this.springCtx = springCtx;
        }

        /**
         * @param ses Task session.
         */
        @GridTaskSessionResource
        public void setSession(GridComputeTaskSession ses) {
            this.ses = ses;
        }

        /**
         * @param locHost Local host.
         */
        @GridLocalHostResource
        public void setLocalHost(String locHost) {
            this.locHost = locHost;
        }

        /**
         * @param balancer Load balancer.
         */
        @GridLoadBalancerResource
        public void setBalancer(ComputeLoadBalancer balancer) {
            this.balancer = balancer;
        }

        /**
         * @param jobCtx Job context.
         */
        @GridJobContextResource
        public void setJobContext(ComputeJobContext jobCtx) {
            this.jobCtx = jobCtx;
        }

        /**
         * @param springBean Bean provided from Spring context.
         */
        @GridSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        public void setSpringBean(UserSpringBean springBean) {
            this.springBean = springBean;
        }

        /**
         * @param mapper Task Continuous Mapper.
         */
        @GridTaskContinuousMapperResource
        public void setMapper(GridComputeTaskContinuousMapper mapper) {
            this.mapper = mapper;
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
            assert locHost != null;
            assert marshaller != null;
            assert springCtx != null;
            assert ses != null;
            assert balancer != null;
            assert springBean != null;
            assert mapper != null;

            // Job context belongs to job, not to task.
            assert jobCtx == null;

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
            log.info("Injected local host resource into task: " + locHost);
            log.info("Injected marshaller resource into task: " + marshaller);
            log.info("Injected spring context resource into task: " + springCtx);
            log.info("Injected session resource into task: " + ses);
            log.info("Injected load balancer into task: " + balancer);
            log.info("Injected spring bean resource into task: " + springBean);
            log.info("Injected continuous mapper: " + mapper);

            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    /** */
                    private transient GridAbstractUserResource rsrc5;

                    /** */
                    private transient UserResource4 rsrc6;

                    /** */
                    private transient UserResource5 rsrc7;

                    /** */
                    private transient GridAbstractUserResource rsrc8;

                    /** */
                    private transient UserResource4 rsrc9;

                    /** */
                    private transient UserResource5 rsrc10;

                    /** */
                    private UserSpringBean springBean2;

                    /**
                     * @param rsrc5 Resource 5.
                     */
                    @GridUserResource(resourceClass = UserResource3.class)
                    public void setResource5(GridAbstractUserResource rsrc5) {
                        this.rsrc5 = rsrc5;
                    }

                    /**
                     * @param rsrc6 Resource 6.
                     */
                    @GridUserResource
                    public void setResource6(UserResource4 rsrc6) {
                        this.rsrc6 = rsrc6;
                    }

                    /**
                     * @param rsrc7 Resource 7.
                     */
                    @GridUserResource
                    public void setResource7(UserResource5 rsrc7) {
                        this.rsrc7 = rsrc7;
                    }

                    /**
                     * @param rsrc8 Resource 8.
                     */
                    @GridUserResource(resourceClass = UserResource3.class, resourceName = "rsrc8")
                    public void setResource8(GridAbstractUserResource rsrc8) {
                        this.rsrc8 = rsrc8;
                    }

                    /**
                     * @param rsrc9 Resource 9.
                     */
                    @GridUserResource(resourceName = "rsrc9")
                    public void setResource9(UserResource4 rsrc9) {
                        this.rsrc9 = rsrc9;
                    }

                    /**
                     * @param rsrc10 Resource 10.
                     */
                    @GridUserResource(resourceName = "rsrc10")
                    public void setResource10(UserResource5 rsrc10) {
                        this.rsrc10 = rsrc10;
                    }

                    /** */
                    @GridJobContextResource
                    private ComputeJobContext jobCtx;

                    /**
                     * @param springBean2 Bean provided from Spring context.
                     */
                    @GridSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
                    public void setSpringBean2(UserSpringBean springBean2) {
                        this.springBean2 = springBean2;
                    }

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
                        assert locHost != null;
                        assert marshaller != null;
                        assert springCtx != null;
                        assert ses != null;
                        assert jobCtx != null;
                        assert springBean != null;
                        assert springBean2 != null;

                        assert rsrc5 != null;
                        assert rsrc6 != null;
                        assert rsrc7 != null;
                        assert rsrc8 != null;
                        assert rsrc9 != null;
                        assert rsrc10 != null;

                        log.info("Injected shared resource1 into job: " + rsrc1);
                        log.info("Injected shared resource2 into job: " + rsrc2);
                        log.info("Injected shared resource3 into job: " + rsrc3);
                        log.info("Injected shared resource4 into job: " + rsrc4);
                        log.info("Injected shared resource5 into job: " + rsrc5);
                        log.info("Injected shared resource6 into job: " + rsrc6);
                        log.info("Injected shared resource7 into job: " + rsrc7);
                        log.info("Injected shared resource8 into job: " + rsrc8);
                        log.info("Injected shared resource9 into job: " + rsrc9);
                        log.info("Injected shared resource10 into job: " + rsrc10);
                        log.info("Injected log resource into job: " + log);
                        log.info("Injected grid resource into job: " + ignite);
                        log.info("Injected nodeId resource into job: " + nodeId);
                        log.info("Injected local Host resource into job: " + locHost);
                        log.info("Injected mbean server resource into job: " + mbeanSrv);
                        log.info("Injected executor service resource into job: " + exec);
                        log.info("Injected gridgain home resource into job: " + ggHome);
                        log.info("Injected grid name resource into job: " + ggHome);
                        log.info("Injected marshaller resource into job: " + marshaller);
                        log.info("Injected spring context resource into job: " + springCtx);
                        log.info("Injected session resource into job: " + ses);
                        log.info("Injected job context resource into job: " + jobCtx);
                        log.info("Injected spring bean2 resource into job: " + springBean2);

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
            assert locHost != null;
            assert mbeanSrv != null;
            assert exec != null;
            assert ggHome != null;
            assert marshaller != null;
            assert springCtx != null;
            assert ses != null;
            assert balancer != null;
            assert springBean != null;

            // Job context is job resource, not task resource.
            assert jobCtx == null;

            // Nothing to reduce.
            return null;
        }
    }
}
