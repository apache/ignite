/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.lang.*;
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
public class GridResourceFieldInjectionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String SPRING_BEAN_RSRC_NAME = "test-bean";

    /** */
    public GridResourceFieldInjectionSelfTest() {
        super(/*start grid*/false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldInjection() throws Exception {
        Grid grid1 = null;
        Grid grid2 = null;

        try {
            GenericApplicationContext ctx = new GenericApplicationContext();

            RootBeanDefinition bf = new RootBeanDefinition();

            bf.setBeanClass(UserSpringBean.class);

            ctx.registerBeanDefinition(SPRING_BEAN_RSRC_NAME, bf);

            ctx.refresh();

            grid1 = startGrid(1, new GridSpringResourceContextImpl(ctx));
            grid2 = startGrid(2, new GridSpringResourceContextImpl(ctx));

            assert grid1.forRemotes().nodes().size() == 1;
            assert grid2.forRemotes().nodes().size() == 1;

            grid1.compute().execute(UserResourceTask.class, null).get();

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
            GridTestUtils.close(grid1, log());
            GridTestUtils.close(grid2, log());
        }
        checkUsageCount(deployClss, UserResource5.class, 8);

        checkUsageCount(undeployClss, UserResource1.class, 4);
        checkUsageCount(undeployClss, UserResource2.class, 4);
        checkUsageCount(undeployClss, UserResource3.class, 4);
        checkUsageCount(undeployClss, UserResource4.class, 4);
        checkUsageCount(undeployClss, UserResource5.class, 8);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonTransientFieldInjection() throws Exception {
        Grid grid = startGrid(getTestGridName(), new GridSpringResourceContextImpl(createContext()));

        try {
            grid.compute().execute(NonTransientUserResourceTask.class, null).get();

            assert false : "Did not get exception for non-transient field.";
        }
        catch (GridException e) {
            info("Got correct exception for non-transient field: " + e.getMessage());
        }
        finally {
            GridTestUtils.close(grid, log());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonTransientSpringBeanFieldInjection() throws Exception {
        Grid grid = startGrid(getTestGridName(), new GridSpringResourceContextImpl(createContext()));

        try {
            grid.compute().execute(NonTransientSpringBeanResourceTask.class, null).get();

            assert false : "Did not get exception for non-transient field.";
        }
        catch (GridException e) {
            info("Got correct exception for non-transient field: " + e.getMessage());
        }

        stopGrid(getTestGridName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnknownNameSpringBeanFieldInjection() throws Exception {
        Grid grid = startGrid(getTestGridName(), new GridSpringResourceContextImpl(createContext()));

        try {
            grid.compute().execute(UnknownNameSpringBeanResourceTask.class, null).get();

            assert false : "Did not get exception for unknown Spring bean name.";
        }
        catch (GridException e) {
            info("Got correct exception for with unknown Spring bean name: " + e.getMessage());
        }

        stopGrid(getTestGridName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidTypeSpringBeanFieldInjection() throws Exception {
        Grid grid = startGrid(getTestGridName(), new GridSpringResourceContextImpl(createContext()));

        try {
            grid.compute().execute(InvalidTypeSpringBeanResourceTask.class, null).get();

            assert false : "Did not get exception for different Spring bean classes.";
        }
        catch (GridException e) {
            info("Got correct exception for for different Spring bean classes: " + e.getMessage());
        }

        stopGrid(getTestGridName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInjectInClosure() throws Exception {
        Grid grid = startGrid();

        try {
            grid.compute().apply(new GridClosure<Object, Object>() {
                /** */
                @GridInstanceResource
                private Grid grid;

                @Override public Object apply(Object o) {
                    assertNotNull(grid);

                    return null;
                }
            }, new Object()).get();

            grid.compute().broadcast(new GridClosure<Object, Object>() {
                /** */
                @GridInstanceResource
                private Grid grid;

                @Override public Object apply(Object o) {
                    assertNotNull(grid);

                    return null;
                }
            }, new Object()).get();

            grid.compute().apply(new TestClosure(), new Object()).get();

            grid.compute().broadcast(new TestClosure(), new Object()).get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Creates Spring context with registered test bean.
     *
     * @return Test Spring context.
     */
    private ApplicationContext createContext() {
        GenericApplicationContext ctx = new GenericApplicationContext();

        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(UserSpringBean.class);

        ctx.registerBeanDefinition(SPRING_BEAN_RSRC_NAME, builder.getBeanDefinition());

        return ctx;
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
        @GridSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        private UserSpringBean springBean;

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
    public static class UserSpringBean {
        // No-op.
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    public static class NonTransientSpringBeanResourceTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @GridSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        private Object rsrc;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            // Never reached.
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            // Never reached.
            assert false;

            return null;
        }
    }

    /**
     * Task that will always fail due to resource injection with unknown Spring bean name.
     */
    public static class UnknownNameSpringBeanResourceTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @GridSpringResource(resourceName = "unknown-bean-name")
        private transient Object springBean;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            // Never reached.
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            // Never reached.
            assert false;

            return null;
        }
    }

    /**
     * Task that will always fail due to resource injection with invalid declared bean type.
     */
    public static class InvalidTypeSpringBeanResourceTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @GridSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        private transient Serializable springBean;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            // Never reached.
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            // Never reached.
            assert false;

            return null;
        }
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    public static class NonTransientUserResourceTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @GridUserResource(resourceClass = UserResource1.class)
        private Object rsrc;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            // Never reached.
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            // Never reached.
            assert false;

            return null;
        }
    }

    /** */
    public static class UserResourceTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @GridUserResource(resourceClass = UserResource1.class)
        private transient Object rsrc1;

        /** */
        @GridUserResource
        private transient UserResource2 rsrc2;

        /** */
        @GridUserResource(resourceClass = UserResource1.class, resourceName = "rsrc3")
        private transient Object rsrc3;

        /** */
        @GridUserResource(resourceName = "rsrc4")
        private transient UserResource2 rsrc4;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        @GridInstanceResource
        private Grid grid;

        /** */
        @GridLocalHostResource
        private String locHost;

        /** */
        @GridLocalNodeIdResource
        private UUID nodeId;

        /** */
        @GridMBeanServerResource
        private MBeanServer mbeanSrv;

        /** */
        @GridExecutorServiceResource
        private ExecutorService exec;

        /** */
        @GridLoadBalancerResource
        private GridComputeLoadBalancer balancer;

        /** */
        @GridHomeResource
        private String ggHome;

        /** */
        @GridNameResource
        private String gridName;

        /** */
        @GridMarshallerResource
        private GridMarshaller marshaller;

        /** */
        @GridSpringApplicationContextResource
        private ApplicationContext springCtx;

        /** */
        @GridSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        private transient UserSpringBean springBean;

        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession ses;

        /** Job context is job resource, not task resource. */
        @GridJobContextResource
        private GridComputeJobContext outerJobCtx;

        /** */
        @GridTaskContinuousMapperResource
        private transient GridComputeTaskContinuousMapper mapper;

        /** {@inheritDoc} */
        @Override protected Collection<GridComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
            assert rsrc1 != null;
            assert rsrc2 != null;
            assert rsrc3 != null;
            assert rsrc4 != null;
            assert log != null;
            assert grid != null;
            assert nodeId != null;
            assert locHost != null;
            assert mbeanSrv != null;
            assert exec != null;
            assert ggHome != null;
            assert gridName != null;
            assert marshaller != null;
            assert springCtx != null;
            assert springBean != null;
            assert ses != null;
            assert balancer != null;
            assert mapper != null;

            assert outerJobCtx == null;

            assert gridSize == 2;

            log.info("Injected shared resource1 into task: " + rsrc1);
            log.info("Injected shared resource2 into task: " + rsrc2);
            log.info("Injected shared resource3 into task: " + rsrc3);
            log.info("Injected shared resource4 into task: " + rsrc4);
            log.info("Injected log resource into task: " + log);
            log.info("Injected grid resource into task: " + grid);
            log.info("Injected nodeId resource into task: " + nodeId);
            log.info("Injected local host resource into task: " + locHost);
            log.info("Injected mbean server resource into task: " + mbeanSrv);
            log.info("Injected executor service resource into task: " + exec);
            log.info("Injected gridgain home resource into task: " + ggHome);
            log.info("Injected grid name resource into task: " + gridName);
            log.info("Injected marshaller resource into task: " + marshaller);
            log.info("Injected spring context resource into task: " + springCtx);
            log.info("Injected spring bean resource into task: " + springBean);
            log.info("Injected load balancer into task: " + balancer);
            log.info("Injected session resource into task: " + ses);
            log.info("Injected continuous mapper: " + mapper);

            Collection<GridComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new GridComputeJobAdapter() {
                    /** */
                    @GridUserResource(resourceClass = UserResource3.class)
                    private transient GridAbstractUserResource rsrc5;

                    /** */
                    @GridUserResource private transient UserResource4 rsrc6;

                    /** */
                    @GridUserResource private transient UserResource5 rsrc7;

                    /** */
                    @GridUserResource(resourceClass = UserResource3.class, resourceName = "rsrc8")
                    private transient GridAbstractUserResource rsrc8;

                    /** */
                    @GridUserResource(resourceName = "rsrc9")
                    private transient UserResource4 rsrc9;

                    /** */
                    @GridUserResource(resourceName = "rsrc10")
                    private transient UserResource5 rsrc10;

                    /** */
                    @GridSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
                    private transient UserSpringBean springBean2;

                    /** */
                    @GridJobContextResource private GridComputeJobContext jobCtx;

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        assert rsrc1 != null;
                        assert rsrc2 != null;
                        assert rsrc3 != null;
                        assert rsrc4 != null;
                        assert log != null;
                        assert grid != null;
                        assert nodeId != null;
                        assert mbeanSrv != null;
                        assert exec != null;
                        assert ggHome != null;
                        assert gridName != null;
                        assert marshaller != null;
                        assert springCtx != null;
                        assert springBean != null;
                        assert springBean2 != null;
                        assert ses != null;
                        assert jobCtx != null;
                        assert outerJobCtx == null;
                        assert locHost != null;

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
                        log.info("Injected grid resource into job: " + grid);
                        log.info("Injected nodeId resource into job: " + nodeId);
                        log.info("Injected localHost resource into job: " + locHost);
                        log.info("Injected mbean server resource into job: " + mbeanSrv);
                        log.info("Injected executor service resource into job: " + exec);
                        log.info("Injected gridgain home resource into job: " + ggHome);
                        log.info("Injected grid name resource into job: " + gridName);
                        log.info("Injected marshaller resource into job: " + marshaller);
                        log.info("Injected spring context resource into job: " + springCtx);
                        log.info("Injected spring bean resource into job: " + springBean2);
                        log.info("Injected session resource into job: " + ses);
                        log.info("Injected job context resource into job: " + jobCtx);
                        log.info("Injected job context resource into outer class: " + outerJobCtx);

                        return null;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assert rsrc1 != null;
            assert rsrc2 != null;
            assert rsrc3 != null;
            assert rsrc4 != null;
            assert log != null;
            assert grid != null;
            assert nodeId != null;
            assert mbeanSrv != null;
            assert exec != null;
            assert ggHome != null;
            assert gridName != null;
            assert marshaller != null;
            assert springCtx != null;
            assert springBean != null;
            assert ses != null;
            assert balancer != null;
            assert locHost != null;

            // Nothing to reduce.
            return null;
        }
    }
}
