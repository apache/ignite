/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.managed.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.lang.annotation.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * Processor for all Ignite and task/job resources.
 */
public class GridResourceProcessor extends GridProcessorAdapter {
    /** */
    private static final Collection<Class<? extends Annotation>> JOB_INJECTIONS = Arrays.asList(
        IgniteTaskSessionResource.class,
        IgniteJobContextResource.class,
        IgniteInstanceResource.class,
        IgniteSpringApplicationContextResource.class,
        IgniteSpringResource.class,
        IgniteLoggerResource.class,
        IgniteServiceResource.class);

    /** */
    private static final Collection<Class<? extends Annotation>> TASK_INJECTIONS = Arrays.asList(
        IgniteTaskSessionResource.class,
        IgniteLoadBalancerResource.class,
        IgniteTaskContinuousMapperResource.class,
        IgniteInstanceResource.class,
        IgniteSpringApplicationContextResource.class,
        IgniteSpringResource.class,
        IgniteLoggerResource.class,
        IgniteServiceResource.class);

    /** Grid instance injector. */
    private GridResourceBasicInjector<GridEx> gridInjector;

    /** Spring application context injector. */
    private GridResourceInjector springCtxInjector;

    /** Logger injector. */
    private GridResourceBasicInjector<IgniteLogger> logInjector;

    /** Services injector. */
    private GridResourceBasicInjector<Collection<ManagedService>> srvcInjector;

    /** Spring bean resources injector. */
    private GridResourceInjector springBeanInjector;

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
        logInjector = new GridResourceLoggerInjector(ctx.config().getGridLogger());
        srvcInjector = new GridResourceServiceInjector(ctx.grid());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
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
        ioc.onUndeployed(dep.classLoader());
    }

    /**
     * @param dep Deployment.
     * @param target Target object.
     * @param annCls Annotation class.
     * @throws IgniteCheckedException If failed to execute annotated methods.
     */
    public void invokeAnnotated(GridDeployment dep, Object target, Class<? extends Annotation> annCls)
        throws IgniteCheckedException {
        if (target != null) {
            Collection<Method> mtds = getMethodsWithAnnotation(dep, target.getClass(), annCls);

            if (mtds != null) {
                for (Method mtd : mtds) {
                    try {
                        mtd.setAccessible(true);

                        mtd.invoke(target);
                    }
                    catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException e) {
                        throw new IgniteCheckedException("Failed to invoke annotated method [job=" + target + ", mtd=" + mtd +
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
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void inject(GridDeployment dep, Class<?> depCls, Object target) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + target);

        // Unwrap Proxy object.
        target = unwrapTarget(target);

        ioc.inject(target, IgniteInstanceResource.class, gridInjector, dep, depCls);
        ioc.inject(target, IgniteSpringApplicationContextResource.class, springCtxInjector, dep, depCls);
        ioc.inject(target, IgniteSpringResource.class, springBeanInjector, dep, depCls);
        ioc.inject(target, IgniteLoggerResource.class, logInjector, dep, depCls);
        ioc.inject(target, IgniteServiceResource.class, srvcInjector, dep, depCls);
    }

    /**
     * Injects cache name into given object.
     *
     * @param obj Object.
     * @param cacheName Cache name to inject.
     * @throws IgniteCheckedException If failed to inject.
     */
    public void injectCacheName(Object obj, String cacheName) throws IgniteCheckedException {
        assert obj != null;

        if (log.isDebugEnabled())
            log.debug("Injecting cache name: " + obj);

        // Unwrap Proxy object.
        obj = unwrapTarget(obj);

        ioc.inject(obj, IgniteCacheNameResource.class, new GridResourceBasicInjector<>(cacheName), null, null);
    }

    /**
     * @param obj Object to inject.
     * @throws IgniteCheckedException If failed to inject.
     */
    public void injectGeneric(Object obj) throws IgniteCheckedException {
        assert obj != null;

        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + obj);

        // Unwrap Proxy object.
        obj = unwrapTarget(obj);

        // No deployment for lifecycle beans.
        ioc.inject(obj, IgniteSpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, IgniteSpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, gridInjector, null, null);
        ioc.inject(obj, IgniteLoggerResource.class, logInjector, null, null);
        ioc.inject(obj, IgniteServiceResource.class, srvcInjector, null, null);
    }

    /**
     * @param obj Object.
     * @throws IgniteCheckedException If failed.
     */
    public void cleanupGeneric(Object obj) throws IgniteCheckedException {
        if (obj != null) {
            if (log.isDebugEnabled())
                log.debug("Cleaning up resources: " + obj);

            // Unwrap Proxy object.
            obj = unwrapTarget(obj);

            // Caching key is null for the life-cycle beans.
            ioc.inject(obj, IgniteLoggerResource.class, nullInjector, null, null);
            ioc.inject(obj, IgniteServiceResource.class, nullInjector, null, null);
            ioc.inject(obj, IgniteSpringApplicationContextResource.class, nullInjector, null, null);
            ioc.inject(obj, IgniteSpringResource.class, nullInjector, null, null);
            ioc.inject(obj, IgniteInstanceResource.class, nullInjector, null, null);
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
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void inject(GridDeployment dep, Class<?> taskCls, ComputeJob job, ComputeTaskSession ses,
        GridJobContextImpl jobCtx) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If failed.
     */
    private void injectToJob(GridDeployment dep, Class<?> taskCls, Object job, ComputeTaskSession ses,
        GridJobContextImpl jobCtx) throws IgniteCheckedException {
        Class<? extends Annotation>[] filtered = ioc.filter(dep, job, JOB_INJECTIONS);

        if (filtered.length > 0) {
            for (Class<? extends Annotation> annCls : filtered) {
                if (annCls == IgniteTaskSessionResource.class)
                    injectBasicResource(job, IgniteTaskSessionResource.class, ses, dep, taskCls);
                else if (annCls == IgniteJobContextResource.class)
                    ioc.inject(job, IgniteJobContextResource.class, new GridResourceJobContextInjector(jobCtx),
                        dep, taskCls);
                else if (annCls == IgniteInstanceResource.class)
                    ioc.inject(job, IgniteInstanceResource.class, gridInjector, dep, taskCls);
                else if (annCls == IgniteSpringApplicationContextResource.class)
                    ioc.inject(job, IgniteSpringApplicationContextResource.class, springCtxInjector, dep, taskCls);
                else if (annCls == IgniteSpringResource.class)
                    ioc.inject(job, IgniteSpringResource.class, springBeanInjector, dep, taskCls);
                else if (annCls == IgniteLoggerResource.class)
                    ioc.inject(job, IgniteLoggerResource.class, logInjector, dep, taskCls);
                else {
                    assert annCls == IgniteServiceResource.class;

                    ioc.inject(job, IgniteServiceResource.class, srvcInjector, dep, taskCls);
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
    private ComputeJob unwrapJob(ComputeJob job) {
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
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void inject(GridDeployment dep, ComputeTask<?, ?> task, GridTaskSessionImpl ses,
        ComputeLoadBalancer balancer, ComputeTaskContinuousMapper mapper) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + task);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(task);

        Class<? extends Annotation>[] filtered = ioc.filter(dep, obj, TASK_INJECTIONS);

        if (filtered.length == 0)
            return;

        Class<?> taskCls = obj.getClass();

        for (Class<? extends Annotation> annCls : filtered) {
            if (annCls == IgniteTaskSessionResource.class)
                injectBasicResource(obj, IgniteTaskSessionResource.class, ses, dep, taskCls);
            else if (annCls == IgniteLoadBalancerResource.class)
                injectBasicResource(obj, IgniteLoadBalancerResource.class, balancer, dep, taskCls);
            else if (annCls == IgniteTaskContinuousMapperResource.class)
                injectBasicResource(obj, IgniteTaskContinuousMapperResource.class, mapper, dep, taskCls);
            else if (annCls == IgniteInstanceResource.class)
                ioc.inject(obj, IgniteInstanceResource.class, gridInjector, dep, taskCls);
            else if (annCls == IgniteSpringApplicationContextResource.class)
                ioc.inject(obj, IgniteSpringApplicationContextResource.class, springCtxInjector, dep, taskCls);
            else if (annCls == IgniteSpringResource.class)
                ioc.inject(obj, IgniteSpringResource.class, springBeanInjector, dep, taskCls);
            else if (annCls == IgniteLoggerResource.class)
                ioc.inject(obj, IgniteLoggerResource.class, logInjector, dep, taskCls);
            else {
                assert annCls == IgniteServiceResource.class;

                ioc.inject(obj, IgniteServiceResource.class, srvcInjector, dep, taskCls);
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
     * @throws IgniteCheckedException Throw in case of any errors.
     */
    public void inject(IgniteSpi spi) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + spi);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(spi);

        // Caching key is null for the SPIs.
        ioc.inject(obj, IgniteSpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, IgniteSpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, IgniteLoggerResource.class, logInjector, null, null);
        ioc.inject(obj, IgniteServiceResource.class, srvcInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, gridInjector, null, null);
    }

    /**
     * Cleans up resources from given SPI implementation. Essentially, this
     * method injects {@code null}s into SPI implementation.
     *
     * @param spi SPI implementation.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void cleanup(IgniteSpi spi) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Cleaning up resources: " + spi);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(spi);

        ioc.inject(obj, IgniteLoggerResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteServiceResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteSpringApplicationContextResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteSpringResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, nullInjector, null, null);
    }

    /**
     * Injects held resources into given lifecycle bean.
     *
     * @param lifecycleBean Lifecycle bean.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void inject(LifecycleBean lifecycleBean) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + lifecycleBean);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(lifecycleBean);

        // No deployment for lifecycle beans.
        ioc.inject(obj, IgniteSpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, IgniteSpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, gridInjector, null, null);
        ioc.inject(obj, IgniteLoggerResource.class, logInjector, null, null);
        ioc.inject(obj, IgniteServiceResource.class, srvcInjector, null, null);
    }

    /**
     * Cleans up resources from given lifecycle beans. Essentially, this
     * method injects {@code null}s into lifecycle bean.
     *
     * @param lifecycleBean Lifecycle bean.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void cleanup(LifecycleBean lifecycleBean) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Cleaning up resources: " + lifecycleBean);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(lifecycleBean);

        // Caching key is null for the life-cycle beans.
        ioc.inject(obj, IgniteLoggerResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteServiceResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteSpringApplicationContextResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteSpringResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, nullInjector, null, null);
    }

    /**
     * Injects resources into service.
     *
     * @param svc Service to inject.
     * @throws IgniteCheckedException If failed.
     */
    public void inject(ManagedService svc) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + svc);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(svc);

        // No deployment for lifecycle beans.
        ioc.inject(obj, IgniteSpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, IgniteSpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, gridInjector, null, null);
        ioc.inject(obj, IgniteLoggerResource.class, logInjector, null, null);
        ioc.inject(obj, IgniteServiceResource.class, srvcInjector, null, null);
    }

    /**
     * Cleans up resources from given service. Essentially, this
     * method injects {@code null}s into service bean.
     *
     * @param svc Service.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void cleanup(ManagedService svc) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Cleaning up resources: " + svc);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(svc);

        // Caching key is null for the life-cycle beans.
        ioc.inject(obj, IgniteLoggerResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteServiceResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteSpringApplicationContextResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteSpringResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, nullInjector, null, null);
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
     * @throws IgniteCheckedException If injection failed.
     */
    public void injectBasicResource(Object target, Class<? extends Annotation> annCls, Object rsrc,
        GridDeployment dep, Class<?> depCls) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If injection failed.
     */
    public void injectBasicResource(Object target, Class<? extends Annotation> annCls, Object rsrc)
        throws IgniteCheckedException {
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
     * Return original object if Spring AOP used with proxy objects.
     *
     * @param target Target object.
     * @return Original object wrapped by proxy.
     * @throws IgniteCheckedException If unwrap failed.
     */
    private Object unwrapTarget(Object target) throws IgniteCheckedException {
        return rsrcCtx != null ? rsrcCtx.unwrapTarget(target) : target;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Resource processor memory stats [grid=" + ctx.gridName() + ']');

        ioc.printMemoryStats();
    }
}
