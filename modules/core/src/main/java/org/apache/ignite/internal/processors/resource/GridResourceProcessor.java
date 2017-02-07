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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskContinuousMapper;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.GridInternalWrapper;
import org.apache.ignite.internal.GridJobContextImpl;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.services.Service;
import org.apache.ignite.spi.IgniteSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Processor for all Ignite and task/job resources.
 */
public class GridResourceProcessor extends GridProcessorAdapter {
    /** Cleaning injector. */
    private final GridResourceInjector nullInjector = new GridResourceBasicInjector<>(null);

    /** */
    private GridSpringResourceContext rsrcCtx;

    /** */
    private final GridResourceIoc ioc = new GridResourceIoc();

    /** */
    private final GridResourceInjector[] injectorByAnnotation;

    /**
     * Creates resources processor.
     *
     * @param ctx Kernal context.
     */
    public GridResourceProcessor(GridKernalContext ctx) {
        super(ctx);

        injectorByAnnotation = new GridResourceInjector[GridResourceIoc.ResourceAnnotation.values().length];

        injectorByAnnotation[GridResourceIoc.ResourceAnnotation.SERVICE.ordinal()] =
            new GridResourceServiceInjector(ctx.grid());
        injectorByAnnotation[GridResourceIoc.ResourceAnnotation.LOGGER.ordinal()] =
            new GridResourceLoggerInjector(ctx.config().getGridLogger());
        injectorByAnnotation[GridResourceIoc.ResourceAnnotation.IGNITE_INSTANCE.ordinal()] =
            new GridResourceBasicInjector<>(ctx.grid());
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

        GridResourceInjector springCtxInjector = rsrcCtx != null ? rsrcCtx.springContextInjector() : nullInjector;
        GridResourceInjector springBeanInjector = rsrcCtx != null ? rsrcCtx.springBeanInjector() : nullInjector;

        injectorByAnnotation[GridResourceIoc.ResourceAnnotation.SPRING.ordinal()] = springBeanInjector;
        injectorByAnnotation[GridResourceIoc.ResourceAnnotation.SPRING_APPLICATION_CONTEXT.ordinal()] =
            springCtxInjector;
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
        assert target != null;

        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + target);

        // Unwrap Proxy object.
        target = unwrapTarget(target);

        inject(target, GridResourceIoc.AnnotationSet.GENERIC, dep, depCls);
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

        inject(obj, GridResourceIoc.ResourceAnnotation.CACHE_NAME, null, null, cacheName);
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

        return inject(obj, GridResourceIoc.ResourceAnnotation.CACHE_STORE_SESSION, null, null, ses);
    }

    /**
     * @param obj Object to inject.
     * @throws IgniteCheckedException If failed to inject.
     */
    public void injectGeneric(Object obj) throws IgniteCheckedException {
        inject(obj, GridResourceIoc.AnnotationSet.GENERIC);
    }

    /**
     * @param obj Object to inject.
     * @param annSet Supported annotations.
     * @param params Parameters.
     * @throws IgniteCheckedException If failed to inject.
     */
    public void inject(Object obj, GridResourceIoc.AnnotationSet annSet, Object... params)
        throws IgniteCheckedException {
        assert obj != null;

        if (log.isDebugEnabled())
            log.debug("Injecting resources: " + obj);

        // Unwrap Proxy object.
        obj = unwrapTarget(obj);

        inject(obj, annSet, null, null, params);
    }

    /**
     * @param obj Object to inject.
     * @param annSet Supported annotations.
     * @param dep Deployment.
     * @param depCls Deployment class.
     * @param params Parameters.
     * @throws IgniteCheckedException If failed to inject.
     */
    private void inject(Object obj,
        GridResourceIoc.AnnotationSet annSet,
        @Nullable GridDeployment dep,
        @Nullable Class<?> depCls,
        Object... params)
        throws IgniteCheckedException {
        GridResourceIoc.ClassDescriptor clsDesc = ioc.descriptor(null, obj.getClass());

        assert clsDesc != null;

        if (clsDesc.isAnnotated(annSet) == 0)
            return;

        int i = 0;
        for (GridResourceIoc.ResourceAnnotation ann : annSet.annotations) {
            if (clsDesc.isAnnotated(ann)) {
                final GridResourceInjector injector = injectorByAnnotation(ann, i < params.length ? params[i] : null);

                if (injector != null)
                    clsDesc.inject(obj, ann, injector, dep, depCls);
            }

            i++;
        }
    }

    /**
     * @param obj Object.
     * @param annSet Supported annotations.
     * @throws IgniteCheckedException If failed.
     */
    private void cleanup(Object obj, GridResourceIoc.AnnotationSet annSet)
        throws IgniteCheckedException {
        assert obj != null;

        if (log.isDebugEnabled())
            log.debug("Cleaning up resources: " + obj);

        // Unwrap Proxy object.
        obj = unwrapTarget(obj);

        GridResourceIoc.ClassDescriptor clsDesc = ioc.descriptor(null, obj.getClass());

        assert clsDesc != null;

        if (clsDesc.isAnnotated(annSet) == 0)
            return;

        for (GridResourceIoc.ResourceAnnotation ann : annSet.annotations)
            clsDesc.inject(obj, ann, nullInjector, null, null);
    }

    /**
     * @param ann Annotation.
     * @param param Injector parameter.
     * @return Injector.
     */
    private GridResourceInjector injectorByAnnotation(GridResourceIoc.ResourceAnnotation ann, Object param) {
        final GridResourceInjector res;

        switch (ann) {
            case CACHE_NAME:
            case TASK_SESSION:
            case LOAD_BALANCER:
            case TASK_CONTINUOUS_MAPPER:
            case CACHE_STORE_SESSION:
                res = new GridResourceBasicInjector<>(param);
                break;

            case JOB_CONTEXT:
                res = new GridResourceJobContextInjector((ComputeJobContext)param);
                break;

            default:
                res = injectorByAnnotation[ann.ordinal()];
                break;
        }

        return res;
    }

    /**
     * @param obj Object to inject.
     * @throws IgniteCheckedException If failed to inject.
     */
    private boolean inject(Object obj, GridResourceIoc.ResourceAnnotation ann, @Nullable GridDeployment dep,
        @Nullable Class<?> depCls, Object param)
        throws IgniteCheckedException {
        GridResourceIoc.ClassDescriptor clsDesc = ioc.descriptor(null, obj.getClass());

        assert clsDesc != null;

        if (clsDesc.isAnnotated(ann)) {
            GridResourceInjector injector = injectorByAnnotation(ann, param);

            if (injector != null)
                return clsDesc.inject(obj, ann, injector, dep, depCls);
        }

        return false;
    }

    /**
     * @param obj Object.
     * @throws IgniteCheckedException If failed.
     */
    public void cleanupGeneric(Object obj) throws IgniteCheckedException {
        if (obj != null)
            cleanup(obj, GridResourceIoc.AnnotationSet.GENERIC);
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

        inject(job, GridResourceIoc.AnnotationSet.JOB, dep, taskCls, ses, jobCtx);
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

        inject(obj, GridResourceIoc.AnnotationSet.TASK, dep, null, ses, balancer, mapper);
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
     * Checks if annotations presents in specified object.
     *
     * @param dep Class deployment.
     * @param target Object to check.
     * @param annSet Annotations to find.
     * @return {@code true} if any annotation is presented, {@code false} if it's not.
     */
    public boolean isAnnotationsPresent(GridDeployment dep, Object target, GridResourceIoc.AnnotationSet annSet) {
        return ioc.isAnnotationsPresent(dep, target, annSet);
    }

    /**
     * Injects held resources into given SPI implementation.
     *
     * @param spi SPI implementation.
     * @throws IgniteCheckedException Throw in case of any errors.
     */
    public void inject(IgniteSpi spi) throws IgniteCheckedException {
        injectGeneric(spi);
    }

    /**
     * Cleans up resources from given SPI implementation. Essentially, this
     * method injects {@code null}s into SPI implementation.
     *
     * @param spi SPI implementation.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void cleanup(IgniteSpi spi) throws IgniteCheckedException {
        cleanupGeneric(spi);
    }

    /**
     * Injects held resources into given lifecycle bean.
     *
     * @param lifecycleBean Lifecycle bean.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void inject(LifecycleBean lifecycleBean) throws IgniteCheckedException {
        injectGeneric(lifecycleBean);
    }

    /**
     * Cleans up resources from given lifecycle beans. Essentially, this
     * method injects {@code null}s into lifecycle bean.
     *
     * @param lifecycleBean Lifecycle bean.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void cleanup(LifecycleBean lifecycleBean) throws IgniteCheckedException {
        cleanupGeneric(lifecycleBean);
    }

    /**
     * Injects resources into service.
     *
     * @param svc Service to inject.
     * @throws IgniteCheckedException If failed.
     */
    public void inject(Service svc) throws IgniteCheckedException {
        injectGeneric(svc);
    }

    /**
     * Cleans up resources from given service. Essentially, this
     * method injects {@code null}s into service bean.
     *
     * @param svc Service.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void cleanup(Service svc) throws IgniteCheckedException {
        cleanupGeneric(svc);
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
