/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.lang.annotation.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Processor for all Grid and task/job resources.
 */
public class GridResourceProcessor extends GridProcessorAdapter {
    /** */
    private static final Collection<Class<? extends Annotation>> JOB_INJECTIONS = Arrays.asList(
        GridTaskSessionResource.class,
        GridJobContextResource.class,
        GridInstanceResource.class,
        GridExecutorServiceResource.class,
        GridLocalNodeIdResource.class,
        GridLocalHostResource.class,
        GridMBeanServerResource.class,
        GridHomeResource.class,
        GridNameResource.class,
        GridMarshallerResource.class,
        GridSpringApplicationContextResource.class,
        GridSpringResource.class,
        GridLoggerResource.class,
        GridUserResource.class);

    /** */
    private static final Collection<Class<? extends Annotation>> TASK_INJECTIONS = Arrays.asList(
        GridTaskSessionResource.class,
        GridLoadBalancerResource.class,
        GridTaskContinuousMapperResource.class,
        GridInstanceResource.class,
        GridExecutorServiceResource.class,
        GridLocalNodeIdResource.class,
        GridLocalHostResource.class,
        GridMBeanServerResource.class,
        GridHomeResource.class,
        GridNameResource.class,
        GridMarshallerResource.class,
        GridSpringApplicationContextResource.class,
        GridSpringResource.class,
        GridLoggerResource.class,
        GridUserResource.class);

    /** Grid instance injector. */
    private GridResourceBasicInjector<GridEx> gridInjector;

    /** GridGain home folder injector. */
    private GridResourceBasicInjector<String> ggHomeInjector;

    /** Grid name injector. */
    private GridResourceBasicInjector<String> ggNameInjector;

    /** Local host binding injector. */
    private GridResourceBasicInjector<String> locHostInjector;

    /** MBean server injector. */
    private GridResourceBasicInjector<MBeanServer> mbeanSrvInjector;

    /** Grid thread executor injector. */
    private GridResourceBasicInjector<Executor> execInjector;

    /** Grid marshaller injector. */
    private GridResourceBasicInjector<GridMarshaller> marshInjector;

    /** Local node ID injector. */
    private GridResourceBasicInjector<UUID> nodeIdInjector;

    /** Spring application context injector. */
    private GridResourceInjector springCtxInjector;

    /** Logger injector. */
    private GridResourceBasicInjector<GridLogger> logInjector;

    /** Address resolver injector. */
    private GridResourceBasicInjector<GridAddressResolver> addrsRslvrInjector;

    /** Spring bean resources injector. */
    private GridResourceInjector springBeanInjector;

    /** Task resources injector. */
    private GridResourceCustomInjector customInjector;

    /** Cleaning injector. */
    private final GridResourceInjector nullInjector = new GridResourceBasicInjector<>(null);

    /** */
    private GridSpringResourceContext rsrcCtx;

    /** */
    private final GridResourceIoc ioc = new GridResourceIoc();

    /**
     * Creates resources processor.
     *
     * @param ctx Kernal context.
     */
    public GridResourceProcessor(GridKernalContext ctx) {
        super(ctx);

        gridInjector = new GridResourceBasicInjector<>(ctx.grid());
        ggHomeInjector = new GridResourceBasicInjector<>(ctx.config().getGridGainHome());
        ggNameInjector = new GridResourceBasicInjector<>(ctx.config().getGridName());
        locHostInjector = new GridResourceBasicInjector<>(ctx.config().getLocalHost());
        mbeanSrvInjector = new GridResourceBasicInjector<>(ctx.config().getMBeanServer());
        marshInjector = new GridResourceBasicInjector<>(ctx.config().getMarshaller());
        execInjector = new GridResourceBasicInjector<Executor>(ctx.config().getExecutorService());
        nodeIdInjector = new GridResourceBasicInjector<>(ctx.config().getNodeId());
        logInjector = new GridResourceLoggerInjector(ctx.config().getGridLogger());
        addrsRslvrInjector = new GridResourceBasicInjector<>(ctx.config().getAddressResolver());
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        customInjector = new GridResourceCustomInjector(log, ioc);

        customInjector.setExecutorInjector(execInjector);
        customInjector.setGridgainHomeInjector(ggHomeInjector);
        customInjector.setGridNameInjector(ggNameInjector);
        customInjector.setGridInjector(gridInjector);
        customInjector.setMbeanServerInjector(mbeanSrvInjector);
        customInjector.setNodeIdInjector(nodeIdInjector);
        customInjector.setMarshallerInjector(marshInjector);
        customInjector.setSpringContextInjector(springCtxInjector);
        customInjector.setSpringBeanInjector(springBeanInjector);
        customInjector.setLogInjector(logInjector);

        if (log.isDebugEnabled())
            log.debug("Started resource processor.");
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        ioc.undeployAll();

        if (log.isDebugEnabled())
            log.debug("Stopped resource processor.");
    }

    /**
     * Sets Spring resource context.
     *
     * @param rsrcCtx Spring resource context.
     */
    public void setSpringContext(@Nullable GridSpringResourceContext rsrcCtx) {
        this.rsrcCtx = rsrcCtx;

        springCtxInjector = rsrcCtx != null ? rsrcCtx.springContextInjector() : nullInjector;
        springBeanInjector = rsrcCtx != null ? rsrcCtx.springBeanInjector() : nullInjector;
    }

    /**
     * Callback to be called when class loader is undeployed.
     *
     * @param dep Deployment to release resources for.
     */
    public void onUndeployed(GridDeployment dep) {
        customInjector.undeploy(dep);

        ioc.onUndeployed(dep.classLoader());
    }

    /**
     * @param dep Deployment.
     * @param target Target object.
     * @param annCls Annotation class.
     * @throws GridException If failed to execute annotated methods.
     */
    public void invokeAnnotated(GridDeployment dep, Object target, Class<? extends Annotation> annCls)
        throws GridException {
        if (target != null) {
            Collection<Method> mtds = getMethodsWithAnnotation(dep, target.getClass(), annCls);

            if (mtds != null) {
                for (Method mtd : mtds) {
                    try {
                        mtd.setAccessible(true);

                        mtd.invoke(target);
                    }
                    catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException e) {
                        throw new GridException("Failed to invoke annotated method [job=" + target + ", mtd=" + mtd +
                            ", ann=" + annCls + ']', e);
                    }
                }
            }
        }
    }

    /**
     * Injects resources into generic class.
     *
     * @param dep Deployment.
     * @param depCls Deployed class.
     * @param target Target instance to inject into.
     * @throws GridException Thrown in case of any errors.
     */
    public void inject(GridDeployment dep, Class<?> depCls, Object target) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + target);

        // Unwrap Proxy object.
        target = unwrapTarget(target);

        ioc.inject(target, GridInstanceResource.class, gridInjector, dep, depCls);
        ioc.inject(target, GridExecutorServiceResource.class, execInjector, dep, depCls);
        ioc.inject(target, GridLocalNodeIdResource.class, nodeIdInjector, dep, depCls);
        ioc.inject(target, GridLocalHostResource.class, locHostInjector, dep, depCls);
        ioc.inject(target, GridMBeanServerResource.class, mbeanSrvInjector, dep, depCls);
        ioc.inject(target, GridHomeResource.class, ggHomeInjector, dep, depCls);
        ioc.inject(target, GridNameResource.class, ggNameInjector, dep, depCls);
        ioc.inject(target, GridMarshallerResource.class, marshInjector, dep, depCls);
        ioc.inject(target, GridSpringApplicationContextResource.class, springCtxInjector, dep, depCls);
        ioc.inject(target, GridSpringResource.class, springBeanInjector, dep, depCls);
        ioc.inject(target, GridLoggerResource.class, logInjector, dep, depCls);

        // Inject users resource.
        ioc.inject(target, GridUserResource.class, customInjector, dep, depCls);
    }

    /**
     * Injects cache name into given object.
     *
     * @param obj Object.
     * @param cacheName Cache name to inject.
     * @throws GridException If failed to inject.
     */
    public void injectCacheName(Object obj, String cacheName) throws GridException {
        assert obj != null;

        if (log.isDebugEnabled())
            log.debug("Injecting cache name: " + obj);

        // Unwrap Proxy object.
        obj = unwrapTarget(obj);

        ioc.inject(obj, GridCacheNameResource.class, new GridResourceBasicInjector<>(cacheName), null, null);
    }

    /**
     * @param obj Object to inject.
     * @throws GridException If failed to inject.
     */
    public void injectGeneric(Object obj) throws GridException {
        assert obj != null;

        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + obj);

        // Unwrap Proxy object.
        obj = unwrapTarget(obj);

        // No deployment for lifecycle beans.
        ioc.inject(obj, GridExecutorServiceResource.class, execInjector, null, null);
        ioc.inject(obj, GridLocalNodeIdResource.class, nodeIdInjector, null, null);
        ioc.inject(obj, GridLocalHostResource.class, locHostInjector, null, null);
        ioc.inject(obj, GridMBeanServerResource.class, mbeanSrvInjector, null, null);
        ioc.inject(obj, GridHomeResource.class, ggHomeInjector, null, null);
        ioc.inject(obj, GridNameResource.class, ggNameInjector, null, null);
        ioc.inject(obj, GridMarshallerResource.class, marshInjector, null, null);
        ioc.inject(obj, GridSpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, GridSpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, GridInstanceResource.class, gridInjector, null, null);
        ioc.inject(obj, GridLoggerResource.class, logInjector, null, null);
    }

    /**
     * @param obj Object.
     * @throws GridException If failed.
     */
    public void cleanupGeneric(Object obj) throws GridException {
        if (obj != null) {
            if (log.isDebugEnabled())
                log.debug("Cleaning up resources: " + obj);

            // Unwrap Proxy object.
            obj = unwrapTarget(obj);

            // Caching key is null for the life-cycle beans.
            ioc.inject(obj, GridLoggerResource.class, nullInjector, null, null);
            ioc.inject(obj, GridExecutorServiceResource.class, nullInjector, null, null);
            ioc.inject(obj, GridLocalNodeIdResource.class, nullInjector, null, null);
            ioc.inject(obj, GridLocalHostResource.class, locHostInjector, null, null);
            ioc.inject(obj, GridMBeanServerResource.class, nullInjector, null, null);
            ioc.inject(obj, GridHomeResource.class, nullInjector, null, null);
            ioc.inject(obj, GridNameResource.class, nullInjector, null, null);
            ioc.inject(obj, GridMarshallerResource.class, nullInjector, null, null);
            ioc.inject(obj, GridSpringApplicationContextResource.class, nullInjector, null, null);
            ioc.inject(obj, GridSpringResource.class, nullInjector, null, null);
            ioc.inject(obj, GridInstanceResource.class, nullInjector, null, null);
        }
    }

    /**
     * Injects held resources into given {@code job}.
     *
     * @param dep Deployment.
     * @param taskCls Task class.
     * @param job Grid job to inject resources to.
     * @param ses Current task session.
     * @param jobCtx Job context.
     * @throws GridException Thrown in case of any errors.
     */
    public void inject(GridDeployment dep, Class<?> taskCls, GridComputeJob job, GridComputeTaskSession ses,
        GridJobContextImpl jobCtx) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + job);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(unwrapJob(job));

        injectToJob(dep, taskCls, obj, ses, jobCtx);

        if (obj instanceof GridInternalWrapper) {
            Object usrObj = ((GridInternalWrapper)obj).userObject();

            if (usrObj != null)
                injectToJob(dep, taskCls, usrObj, ses, jobCtx);
        }
    }

    /**
     * Internal routine for resource injection into job.
     *
     * @param dep Deployment.
     * @param taskCls Task class.
     * @param job Job.
     * @param ses Session.
     * @param jobCtx Job context.
     * @throws GridException If failed.
     */
    private void injectToJob(GridDeployment dep, Class<?> taskCls, Object job, GridComputeTaskSession ses,
        GridJobContextImpl jobCtx) throws GridException {
        Class<? extends Annotation>[] filtered = ioc.filter(dep, job, JOB_INJECTIONS);

        if (filtered.length > 0) {
            for (Class<? extends Annotation> annCls : filtered) {
                if (annCls == GridTaskSessionResource.class)
                    injectBasicResource(job, GridTaskSessionResource.class, ses, dep, taskCls);
                else if (annCls == GridJobContextResource.class)
                    ioc.inject(job, GridJobContextResource.class, new GridResourceJobContextInjector(jobCtx),
                        dep, taskCls);
                else if (annCls == GridInstanceResource.class)
                    ioc.inject(job, GridInstanceResource.class, gridInjector, dep, taskCls);
                else if (annCls == GridExecutorServiceResource.class)
                    ioc.inject(job, GridExecutorServiceResource.class, execInjector, dep, taskCls);
                else if (annCls == GridLocalNodeIdResource.class)
                    ioc.inject(job, GridLocalNodeIdResource.class, nodeIdInjector, dep, taskCls);
                else if (annCls == GridLocalHostResource.class)
                    ioc.inject(job, GridLocalHostResource.class, locHostInjector, dep, taskCls);
                else if (annCls == GridMBeanServerResource.class)
                    ioc.inject(job, GridMBeanServerResource.class, mbeanSrvInjector, dep, taskCls);
                else if (annCls == GridHomeResource.class)
                    ioc.inject(job, GridHomeResource.class, ggHomeInjector, dep, taskCls);
                else if (annCls == GridNameResource.class)
                    ioc.inject(job, GridNameResource.class, ggNameInjector, dep, taskCls);
                else if (annCls == GridMarshallerResource.class)
                    ioc.inject(job, GridMarshallerResource.class, marshInjector, dep, taskCls);
                else if (annCls == GridSpringApplicationContextResource.class)
                    ioc.inject(job, GridSpringApplicationContextResource.class, springCtxInjector, dep, taskCls);
                else if (annCls == GridSpringResource.class)
                    ioc.inject(job, GridSpringResource.class, springBeanInjector, dep, taskCls);
                else if (annCls == GridLoggerResource.class)
                    ioc.inject(job, GridLoggerResource.class, logInjector, dep, taskCls);
                else {
                    assert annCls == GridUserResource.class;

                    ioc.inject(job, GridUserResource.class, customInjector, dep, taskCls);
                }
            }
        }
    }

    /**
     * Gets rid of job wrapper, if any.
     *
     * @param job Job to unwrap.
     * @return Unwrapped job.
     */
    private GridComputeJob unwrapJob(GridComputeJob job) {
        if (job instanceof GridComputeJobWrapper)
            return ((GridComputeJobWrapper)job).wrappedJob();

        return job;
    }

    /**
     * Injects held resources into given grid task.
     *
     * @param dep Deployed class.
     * @param task Grid task.
     * @param ses Grid task session.
     * @param balancer Load balancer.
     * @param mapper Continuous task mapper.
     * @throws GridException Thrown in case of any errors.
     */
    public void inject(GridDeployment dep, GridComputeTask<?, ?> task, GridTaskSessionImpl ses,
        GridComputeLoadBalancer balancer, GridComputeTaskContinuousMapper mapper) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + task);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(task);

        Class<? extends Annotation>[] filtered = ioc.filter(dep, obj, TASK_INJECTIONS);

        if (filtered.length == 0)
            return;

        Class<?> taskCls = obj.getClass();

        for (Class<? extends Annotation> annCls : filtered) {
            if (annCls == GridTaskSessionResource.class)
                injectBasicResource(obj, GridTaskSessionResource.class, ses, dep, taskCls);
            else if (annCls == GridLoadBalancerResource.class)
                injectBasicResource(obj, GridLoadBalancerResource.class, balancer, dep, taskCls);
            else if (annCls == GridTaskContinuousMapperResource.class)
                injectBasicResource(obj, GridTaskContinuousMapperResource.class, mapper, dep, taskCls);
            else if (annCls == GridInstanceResource.class)
                ioc.inject(obj, GridInstanceResource.class, gridInjector, dep, taskCls);
            else if (annCls == GridExecutorServiceResource.class)
                ioc.inject(obj, GridExecutorServiceResource.class, execInjector, dep, taskCls);
            else if (annCls == GridLocalNodeIdResource.class)
                ioc.inject(obj, GridLocalNodeIdResource.class, nodeIdInjector, dep, taskCls);
            else if (annCls == GridLocalHostResource.class)
                ioc.inject(obj, GridLocalHostResource.class, locHostInjector, dep, taskCls);
            else if (annCls == GridMBeanServerResource.class)
                ioc.inject(obj, GridMBeanServerResource.class, mbeanSrvInjector, dep, taskCls);
            else if (annCls == GridHomeResource.class)
                ioc.inject(obj, GridHomeResource.class, ggHomeInjector, dep, taskCls);
            else if (annCls == GridNameResource.class)
                ioc.inject(obj, GridNameResource.class, ggNameInjector, dep, taskCls);
            else if (annCls == GridMarshallerResource.class)
                ioc.inject(obj, GridMarshallerResource.class, marshInjector, dep, taskCls);
            else if (annCls == GridSpringApplicationContextResource.class)
                ioc.inject(obj, GridSpringApplicationContextResource.class, springCtxInjector, dep, taskCls);
            else if (annCls == GridSpringResource.class)
                ioc.inject(obj, GridSpringResource.class, springBeanInjector, dep, taskCls);
            else if (annCls == GridLoggerResource.class)
                ioc.inject(obj, GridLoggerResource.class, logInjector, dep, taskCls);
            else {
                assert annCls == GridUserResource.class;

                ioc.inject(obj, GridUserResource.class, customInjector, dep, taskCls);
            }
        }
    }

    /**
     * Checks if annotation presents in specified object.
     *
     * @param dep Class deployment.
     * @param target Object to check.
     * @param annCls Annotation to find.
     * @return {@code true} if annotation is presented, {@code false} otherwise.
     */
    public boolean isAnnotationPresent(GridDeployment dep, Object target, Class<? extends Annotation> annCls) {
        return ioc.isAnnotationPresent(target, annCls, dep);
    }

    /**
     * Injects held resources into given SPI implementation.
     *
     * @param spi SPI implementation.
     * @throws GridException Throw in case of any errors.
     */
    public void inject(GridSpi spi) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + spi);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(spi);

        // Caching key is null for the SPIs.
        ioc.inject(obj, GridExecutorServiceResource.class, execInjector, null, null);
        ioc.inject(obj, GridLocalNodeIdResource.class, nodeIdInjector, null, null);
        ioc.inject(obj, GridLocalHostResource.class, locHostInjector, null, null);
        ioc.inject(obj, GridMBeanServerResource.class, mbeanSrvInjector, null, null);
        ioc.inject(obj, GridHomeResource.class, ggHomeInjector, null, null);
        ioc.inject(obj, GridNameResource.class, ggNameInjector, null, null);
        ioc.inject(obj, GridMarshallerResource.class, marshInjector, null, null);
        ioc.inject(obj, GridSpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, GridSpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, GridLoggerResource.class, logInjector, null, null);
        ioc.inject(obj, GridAddressResolverResource.class, addrsRslvrInjector, null, null);
    }

    /**
     * Cleans up resources from given SPI implementation. Essentially, this
     * method injects {@code null}s into SPI implementation.
     *
     * @param spi SPI implementation.
     * @throws GridException Thrown in case of any errors.
     */
    public void cleanup(GridSpi spi) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Cleaning up resources: " + spi);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(spi);

        ioc.inject(obj, GridLoggerResource.class, nullInjector, null, null);
        ioc.inject(obj, GridExecutorServiceResource.class, nullInjector, null, null);
        ioc.inject(obj, GridLocalNodeIdResource.class, nullInjector, null, null);
        ioc.inject(obj, GridLocalHostResource.class, nullInjector, null, null);
        ioc.inject(obj, GridMBeanServerResource.class, nullInjector, null, null);
        ioc.inject(obj, GridHomeResource.class, nullInjector, null, null);
        ioc.inject(obj, GridNameResource.class, nullInjector, null, null);
        ioc.inject(obj, GridMarshallerResource.class, nullInjector, null, null);
        ioc.inject(obj, GridSpringApplicationContextResource.class, nullInjector, null, null);
        ioc.inject(obj, GridSpringResource.class, nullInjector, null, null);
        ioc.inject(obj, GridAddressResolverResource.class, nullInjector, null, null);
    }

    /**
     * Injects held resources into given lifecycle bean.
     *
     * @param lifecycleBean Lifecycle bean.
     * @throws GridException Thrown in case of any errors.
     */
    public void inject(GridLifecycleBean lifecycleBean) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + lifecycleBean);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(lifecycleBean);

        // No deployment for lifecycle beans.
        ioc.inject(obj, GridExecutorServiceResource.class, execInjector, null, null);
        ioc.inject(obj, GridLocalNodeIdResource.class, nodeIdInjector, null, null);
        ioc.inject(obj, GridLocalHostResource.class, locHostInjector, null, null);
        ioc.inject(obj, GridMBeanServerResource.class, mbeanSrvInjector, null, null);
        ioc.inject(obj, GridHomeResource.class, ggHomeInjector, null, null);
        ioc.inject(obj, GridNameResource.class, ggNameInjector, null, null);
        ioc.inject(obj, GridMarshallerResource.class, marshInjector, null, null);
        ioc.inject(obj, GridSpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, GridSpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, GridInstanceResource.class, gridInjector, null, null);
        ioc.inject(obj, GridLoggerResource.class, logInjector, null, null);
    }

    /**
     * Cleans up resources from given lifecycle beans. Essentially, this
     * method injects {@code null}s into lifecycle bean.
     *
     * @param lifecycleBean Lifecycle bean.
     * @throws GridException Thrown in case of any errors.
     */
    public void cleanup(GridLifecycleBean lifecycleBean) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Cleaning up resources: " + lifecycleBean);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(lifecycleBean);

        // Caching key is null for the life-cycle beans.
        ioc.inject(obj, GridLoggerResource.class, nullInjector, null, null);
        ioc.inject(obj, GridExecutorServiceResource.class, nullInjector, null, null);
        ioc.inject(obj, GridLocalNodeIdResource.class, nullInjector, null, null);
        ioc.inject(obj, GridLocalHostResource.class, nullInjector, null, null);
        ioc.inject(obj, GridMBeanServerResource.class, nullInjector, null, null);
        ioc.inject(obj, GridHomeResource.class, nullInjector, null, null);
        ioc.inject(obj, GridNameResource.class, nullInjector, null, null);
        ioc.inject(obj, GridMarshallerResource.class, nullInjector, null, null);
        ioc.inject(obj, GridSpringApplicationContextResource.class, nullInjector, null, null);
        ioc.inject(obj, GridSpringResource.class, nullInjector, null, null);
        ioc.inject(obj, GridInstanceResource.class, nullInjector, null, null);
    }

    /**
     * Injects resources into service.
     *
     * @param svc Service to inject.
     * @throws GridException If failed.
     */
    public void inject(GridService svc) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + svc);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(svc);

        // No deployment for lifecycle beans.
        ioc.inject(obj, GridExecutorServiceResource.class, execInjector, null, null);
        ioc.inject(obj, GridLocalNodeIdResource.class, nodeIdInjector, null, null);
        ioc.inject(obj, GridLocalHostResource.class, locHostInjector, null, null);
        ioc.inject(obj, GridMBeanServerResource.class, mbeanSrvInjector, null, null);
        ioc.inject(obj, GridHomeResource.class, ggHomeInjector, null, null);
        ioc.inject(obj, GridNameResource.class, ggNameInjector, null, null);
        ioc.inject(obj, GridMarshallerResource.class, marshInjector, null, null);
        ioc.inject(obj, GridSpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, GridSpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, GridInstanceResource.class, gridInjector, null, null);
        ioc.inject(obj, GridLoggerResource.class, logInjector, null, null);
    }

    /**
     * Cleans up resources from given service. Essentially, this
     * method injects {@code null}s into service bean.
     *
     * @param svc Service.
     * @throws GridException Thrown in case of any errors.
     */
    public void cleanup(GridService svc) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Cleaning up resources: " + svc);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(svc);

        // Caching key is null for the life-cycle beans.
        ioc.inject(obj, GridLoggerResource.class, nullInjector, null, null);
        ioc.inject(obj, GridExecutorServiceResource.class, nullInjector, null, null);
        ioc.inject(obj, GridLocalNodeIdResource.class, nullInjector, null, null);
        ioc.inject(obj, GridLocalHostResource.class, nullInjector, null, null);
        ioc.inject(obj, GridMBeanServerResource.class, nullInjector, null, null);
        ioc.inject(obj, GridHomeResource.class, nullInjector, null, null);
        ioc.inject(obj, GridNameResource.class, nullInjector, null, null);
        ioc.inject(obj, GridMarshallerResource.class, nullInjector, null, null);
        ioc.inject(obj, GridSpringApplicationContextResource.class, nullInjector, null, null);
        ioc.inject(obj, GridSpringResource.class, nullInjector, null, null);
        ioc.inject(obj, GridInstanceResource.class, nullInjector, null, null);
    }

    /**
     * This method is declared public as it is used from tests as well.
     * Note, that this method can be used only with unwrapped objects
     * (see {@link #unwrapTarget(Object)}).
     *
     * @param target Target object.
     * @param annCls Setter annotation.
     * @param rsrc Resource to inject.
     * @param dep Deployment.
     * @param depCls Deployed class.
     * @throws GridException If injection failed.
     */
    public void injectBasicResource(Object target, Class<? extends Annotation> annCls, Object rsrc,
        GridDeployment dep, Class<?> depCls) throws GridException {
        // Safety.
        assert !(rsrc instanceof GridResourceInjector) : "Invalid injection.";

        // Basic injection don't cache anything. Use null as a key.
        ioc.inject(target, annCls, new GridResourceBasicInjector<>(rsrc), dep, depCls);
    }

    /**
     * This method is declared public as it is used from tests as well.
     * Note, that this method can be used only with unwrapped objects
     * (see {@link #unwrapTarget(Object)}).
     *
     * @param target Target object.
     * @param annCls Setter annotation.
     * @param rsrc Resource to inject.
     * @throws GridException If injection failed.
     */
    public void injectBasicResource(Object target, Class<? extends Annotation> annCls, Object rsrc)
        throws GridException {
        // Safety.
        assert !(rsrc instanceof GridResourceInjector) : "Invalid injection.";

        // Basic injection don't cache anything. Use null as a key.
        ioc.inject(target, annCls, new GridResourceBasicInjector<>(rsrc), null, null);
    }

    /**
     * Gets list of methods in specified class annotated with specified annotation.
     *
     * @param dep Class deployment.
     * @param rsrcCls Class to find methods in.
     * @param annCls Annotation to find annotated methods with.
     * @return List of annotated methods.
     */
    @Nullable public Collection<Method> getMethodsWithAnnotation(GridDeployment dep, Class<?> rsrcCls,
        Class<? extends Annotation> annCls) {
        assert dep != null;
        assert rsrcCls != null;
        assert annCls != null;

        List<GridResourceMethod> mtds = ioc.getMethodsWithAnnotation(dep, rsrcCls, annCls);

        assert mtds != null;

        if (!mtds.isEmpty()) {
            return F.viewReadOnly(mtds, new C1<GridResourceMethod, Method>() {
                @Override public Method apply(GridResourceMethod rsrcMtd) {
                    return rsrcMtd.getMethod();
                }
            });
        }

        return null;
    }

    /**
     * Returns GridResourceIoc object. For tests only!!!
     *
     * @return GridResourceIoc object.
     */
    GridResourceIoc getResourceIoc() {
        return ioc;
    }

    /**
     * Returns GridResourceCustomInjector object. For tests only!!!
     *
     * @return GridResourceCustomInjector object.
     */
    GridResourceCustomInjector getResourceCustomInjector() {
        return customInjector;
    }

    /**
     * Return original object if Spring AOP used with proxy objects.
     *
     * @param target Target object.
     * @return Original object wrapped by proxy.
     * @throws GridException If unwrap failed.
     */
    private Object unwrapTarget(Object target) throws GridException {
        return rsrcCtx != null ? rsrcCtx.unwrapTarget(target) : target;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Resource processor memory stats [grid=" + ctx.gridName() + ']');

        ioc.printMemoryStats();
    }
}
