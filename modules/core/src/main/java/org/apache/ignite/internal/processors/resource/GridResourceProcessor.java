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

package org.apache.ignite.internal.processors.resource;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskContinuousMapper;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.GridInternalWrapper;
import org.apache.ignite.internal.GridJobContextImpl;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoadBalancerResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.resources.TaskContinuousMapperResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.spi.IgniteSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Processor for all Ignite and task/job resources.
 */
public class GridResourceProcessor extends GridProcessorAdapter {
    /** */
    private static final Collection<Class<? extends Annotation>> JOB_INJECTIONS = Arrays.asList(
        TaskSessionResource.class,
        JobContextResource.class,
        IgniteInstanceResource.class,
        SpringApplicationContextResource.class,
        SpringResource.class,
        LoggerResource.class,
        ServiceResource.class);

    /** */
    private static final Collection<Class<? extends Annotation>> TASK_INJECTIONS = Arrays.asList(
        TaskSessionResource.class,
        LoadBalancerResource.class,
        TaskContinuousMapperResource.class,
        IgniteInstanceResource.class,
        SpringApplicationContextResource.class,
        SpringResource.class,
        LoggerResource.class,
        ServiceResource.class);

    /** Grid instance injector. */
    private GridResourceBasicInjector<IgniteEx> gridInjector;

    /** Spring application context injector. */
    private GridResourceInjector springCtxInjector;

    /** Logger injector. */
    private GridResourceBasicInjector<IgniteLogger> logInjector;

    /** Services injector. */
    private GridResourceBasicInjector<Collection<Service>> srvcInjector;

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
            GridResourceMethod[] rsrcMtds = ioc.getMethodsWithAnnotation(dep, target.getClass(), annCls);

            for (GridResourceMethod rsrcMtd : rsrcMtds) {
                Method mtd = rsrcMtd.getMethod();

                try {
                    // No need to call mtd.setAccessible(true);
                    // It has been called in GridResourceMethod constructor.
                    mtd.invoke(target);
                }
                catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException e) {
                    throw new IgniteCheckedException("Failed to invoke annotated method [job=" + target + ", mtd=" + mtd +
                        ", ann=" + annCls + ']', e);
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
        ioc.inject(target, SpringApplicationContextResource.class, springCtxInjector, dep, depCls);
        ioc.inject(target, SpringResource.class, springBeanInjector, dep, depCls);
        ioc.inject(target, LoggerResource.class, logInjector, dep, depCls);
        ioc.inject(target, ServiceResource.class, srvcInjector, dep, depCls);
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

        ioc.inject(obj, CacheNameResource.class, new GridResourceBasicInjector<>(cacheName), null, null);
    }

    /**
     * Injects cache store session into given object.
     *
     * @param obj Object.
     * @param ses Session to inject.
     * @return {@code True} if session was injected.
     * @throws IgniteCheckedException If failed to inject.
     */
    public boolean injectStoreSession(Object obj, CacheStoreSession ses) throws IgniteCheckedException {
        assert obj != null;

        if (log.isDebugEnabled())
            log.debug("Injecting cache store session: " + obj);

        // Unwrap Proxy object.
        obj = unwrapTarget(obj);

        return ioc.inject(obj, CacheStoreSessionResource.class, new GridResourceBasicInjector<>(ses), null, null);
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
        ioc.inject(obj, SpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, SpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, gridInjector, null, null);
        ioc.inject(obj, LoggerResource.class, logInjector, null, null);
        ioc.inject(obj, ServiceResource.class, srvcInjector, null, null);
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
            ioc.inject(obj, LoggerResource.class, nullInjector, null, null);
            ioc.inject(obj, ServiceResource.class, nullInjector, null, null);
            ioc.inject(obj, SpringApplicationContextResource.class, nullInjector, null, null);
            ioc.inject(obj, SpringResource.class, nullInjector, null, null);
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
        Object obj = unwrapTarget(job);

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
                if (annCls == TaskSessionResource.class)
                    injectBasicResource(job, TaskSessionResource.class, ses, dep, taskCls);
                else if (annCls == JobContextResource.class)
                    ioc.inject(job, JobContextResource.class, new GridResourceJobContextInjector(jobCtx),
                        dep, taskCls);
                else if (annCls == IgniteInstanceResource.class)
                    ioc.inject(job, IgniteInstanceResource.class, gridInjector, dep, taskCls);
                else if (annCls == SpringApplicationContextResource.class)
                    ioc.inject(job, SpringApplicationContextResource.class, springCtxInjector, dep, taskCls);
                else if (annCls == SpringResource.class)
                    ioc.inject(job, SpringResource.class, springBeanInjector, dep, taskCls);
                else if (annCls == LoggerResource.class)
                    ioc.inject(job, LoggerResource.class, logInjector, dep, taskCls);
                else {
                    assert annCls == ServiceResource.class;

                    ioc.inject(job, ServiceResource.class, srvcInjector, dep, taskCls);
                }
            }
        }
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
            if (annCls == TaskSessionResource.class)
                injectBasicResource(obj, TaskSessionResource.class, ses, dep, taskCls);
            else if (annCls == LoadBalancerResource.class)
                injectBasicResource(obj, LoadBalancerResource.class, balancer, dep, taskCls);
            else if (annCls == TaskContinuousMapperResource.class)
                injectBasicResource(obj, TaskContinuousMapperResource.class, mapper, dep, taskCls);
            else if (annCls == IgniteInstanceResource.class)
                ioc.inject(obj, IgniteInstanceResource.class, gridInjector, dep, taskCls);
            else if (annCls == SpringApplicationContextResource.class)
                ioc.inject(obj, SpringApplicationContextResource.class, springCtxInjector, dep, taskCls);
            else if (annCls == SpringResource.class)
                ioc.inject(obj, SpringResource.class, springBeanInjector, dep, taskCls);
            else if (annCls == LoggerResource.class)
                ioc.inject(obj, LoggerResource.class, logInjector, dep, taskCls);
            else {
                assert annCls == ServiceResource.class;

                ioc.inject(obj, ServiceResource.class, srvcInjector, dep, taskCls);
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
        ioc.inject(obj, SpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, SpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, LoggerResource.class, logInjector, null, null);
        ioc.inject(obj, ServiceResource.class, srvcInjector, null, null);
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

        ioc.inject(obj, LoggerResource.class, nullInjector, null, null);
        ioc.inject(obj, ServiceResource.class, nullInjector, null, null);
        ioc.inject(obj, SpringApplicationContextResource.class, nullInjector, null, null);
        ioc.inject(obj, SpringResource.class, nullInjector, null, null);
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
        ioc.inject(obj, SpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, SpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, gridInjector, null, null);
        ioc.inject(obj, LoggerResource.class, logInjector, null, null);
        ioc.inject(obj, ServiceResource.class, srvcInjector, null, null);
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
        ioc.inject(obj, LoggerResource.class, nullInjector, null, null);
        ioc.inject(obj, ServiceResource.class, nullInjector, null, null);
        ioc.inject(obj, SpringApplicationContextResource.class, nullInjector, null, null);
        ioc.inject(obj, SpringResource.class, nullInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, nullInjector, null, null);
    }

    /**
     * Injects resources into service.
     *
     * @param svc Service to inject.
     * @throws IgniteCheckedException If failed.
     */
    public void inject(Service svc) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + svc);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(svc);

        // No deployment for lifecycle beans.
        ioc.inject(obj, SpringApplicationContextResource.class, springCtxInjector, null, null);
        ioc.inject(obj, SpringResource.class, springBeanInjector, null, null);
        ioc.inject(obj, IgniteInstanceResource.class, gridInjector, null, null);
        ioc.inject(obj, LoggerResource.class, logInjector, null, null);
        ioc.inject(obj, ServiceResource.class, srvcInjector, null, null);
    }

    /**
     * Cleans up resources from given service. Essentially, this
     * method injects {@code null}s into service bean.
     *
     * @param svc Service.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void cleanup(Service svc) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Cleaning up resources: " + svc);

        // Unwrap Proxy object.
        Object obj = unwrapTarget(svc);

        // Caching key is null for the life-cycle beans.
        ioc.inject(obj, LoggerResource.class, nullInjector, null, null);
        ioc.inject(obj, ServiceResource.class, nullInjector, null, null);
        ioc.inject(obj, SpringApplicationContextResource.class, nullInjector, null, null);
        ioc.inject(obj, SpringResource.class, nullInjector, null, null);
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