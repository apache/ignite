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
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
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

            assert ignite1.cluster().forRemotes().nodes().size() == 1;
            assert ignite2.cluster().forRemotes().nodes().size() == 1;

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

    /**
     * @throws Exception If failed.
     */
    public void testNonTransientFieldInjection() throws Exception {
        Ignite ignite = startGrid(getTestGridName(), new GridSpringResourceContextImpl(createContext()));

        try {
            ignite.compute().execute(NonTransientUserResourceTask.class, null);

            assert false : "Did not get exception for non-transient field.";
        }
        catch (IgniteCheckedException e) {
            info("Got correct exception for non-transient field: " + e.getMessage());
        }
        finally {
            GridTestUtils.close(ignite, log());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonTransientSpringBeanFieldInjection() throws Exception {
        Ignite ignite = startGrid(getTestGridName(), new GridSpringResourceContextImpl(createContext()));

        try {
            ignite.compute().execute(NonTransientSpringBeanResourceTask.class, null);

            assert false : "Did not get exception for non-transient field.";
        }
        catch (IgniteCheckedException e) {
            info("Got correct exception for non-transient field: " + e.getMessage());
        }

        stopGrid(getTestGridName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnknownNameSpringBeanFieldInjection() throws Exception {
        Ignite ignite = startGrid(getTestGridName(), new GridSpringResourceContextImpl(createContext()));

        try {
            ignite.compute().execute(UnknownNameSpringBeanResourceTask.class, null);

            assert false : "Did not get exception for unknown Spring bean name.";
        }
        catch (IgniteCheckedException e) {
            info("Got correct exception for with unknown Spring bean name: " + e.getMessage());
        }

        stopGrid(getTestGridName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidTypeSpringBeanFieldInjection() throws Exception {
        Ignite ignite = startGrid(getTestGridName(), new GridSpringResourceContextImpl(createContext()));

        try {
            ignite.compute().execute(InvalidTypeSpringBeanResourceTask.class, null);

            assert false : "Did not get exception for different Spring bean classes.";
        }
        catch (IgniteCheckedException e) {
            info("Got correct exception for for different Spring bean classes: " + e.getMessage());
        }

        stopGrid(getTestGridName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInjectInClosure() throws Exception {
        Ignite ignite = startGrid();

        try {
            ignite.compute().apply(new IgniteClosure<Object, Object>() {
                /** */
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object apply(Object o) {
                    assertNotNull(this.ignite);

                    return null;
                }
            }, new Object());

            ignite.compute().broadcast(new IgniteClosure<Object, Object>() {
                /** */
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Object apply(Object o) {
                    assertNotNull(this.ignite);

                    return null;
                }
            }, new Object());

            ignite.compute().apply(new TestClosure(), new Object());

            ignite.compute().broadcast(new TestClosure(), new Object());
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
        @IgniteSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        private UserSpringBean springBean;
    }

    /** */
    public static class UserSpringBean {
        // No-op.
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    public static class NonTransientSpringBeanResourceTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @IgniteSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        private Object rsrc;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            // Never reached.
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            // Never reached.
            assert false;

            return null;
        }
    }

    /**
     * Task that will always fail due to resource injection with unknown Spring bean name.
     */
    public static class UnknownNameSpringBeanResourceTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @IgniteSpringResource(resourceName = "unknown-bean-name")
        private transient Object springBean;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            // Never reached.
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            // Never reached.
            assert false;

            return null;
        }
    }

    /**
     * Task that will always fail due to resource injection with invalid declared bean type.
     */
    public static class InvalidTypeSpringBeanResourceTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration", "unused"})
        @IgniteSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        private transient Serializable springBean;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            // Never reached.
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            // Never reached.
            assert false;

            return null;
        }
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    public static class NonTransientUserResourceTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
            // Never reached.
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            // Never reached.
            assert false;

            return null;
        }
    }

    /** */
    public static class UserResourceTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @IgniteLoadBalancerResource
        private ComputeLoadBalancer balancer;

        /** */
        @IgniteSpringApplicationContextResource
        private ApplicationContext springCtx;

        /** */
        @IgniteSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
        private transient UserSpringBean springBean;

        /** */
        @IgniteTaskSessionResource
        private ComputeTaskSession ses;

        /** Job context is job resource, not task resource. */
        @IgniteJobContextResource
        private ComputeJobContext outerJobCtx;

        /** */
        @IgniteTaskContinuousMapperResource
        private transient ComputeTaskContinuousMapper mapper;

        /** {@inheritDoc} */
        @Override protected Collection<ComputeJobAdapter> split(int gridSize, Object arg) throws IgniteCheckedException {
            assert log != null;
            assert ignite != null;
            assert springCtx != null;
            assert springBean != null;
            assert ses != null;
            assert balancer != null;
            assert mapper != null;

            assert outerJobCtx == null;

            assert gridSize == 2;

            log.info("Injected log resource into task: " + log);
            log.info("Injected grid resource into task: " + ignite);
            log.info("Injected spring context resource into task: " + springCtx);
            log.info("Injected spring bean resource into task: " + springBean);
            log.info("Injected load balancer into task: " + balancer);
            log.info("Injected session resource into task: " + ses);
            log.info("Injected continuous mapper: " + mapper);

            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    /** */
                    @IgniteSpringResource(resourceName = SPRING_BEAN_RSRC_NAME)
                    private transient UserSpringBean springBean2;

                    /** */
                    @IgniteJobContextResource
                    private ComputeJobContext jobCtx;

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        assert log != null;
                        assert ignite != null;
                        assert springCtx != null;
                        assert springBean != null;
                        assert springBean2 != null;
                        assert ses != null;
                        assert jobCtx != null;
                        assert outerJobCtx == null;

                        log.info("Injected log resource into job: " + log);
                        log.info("Injected grid resource into job: " + ignite);
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
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            assert log != null;
            assert ignite != null;
            assert springCtx != null;
            assert springBean != null;
            assert ses != null;
            assert balancer != null;

            // Nothing to reduce.
            return null;
        }
    }
}
