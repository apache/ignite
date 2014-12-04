/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.managed.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.management.*;
import java.lang.annotation.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Custom injector implementation works with user resources.
 * Injector creates and collects all created user resources.
 * All resources will be cleaned before task undeploy.
 * Task resources should be marked in task with {@link org.apache.ignite.resources.IgniteUserResource} annotation.
 */
class GridResourceCustomInjector implements GridResourceInjector {
    /** Class-based resource attachment. */
    private static final String CLS_RSRC_CACHE = "CLASS_RESOURCE_CACHE";

    /** Class-loader based resource attachment. */
    private static final String CLS_LDR_RSRC_CACHE = "CLASS_LOADER_RESOURCE_CACHE";

    /** Grid logger. */
    private final IgniteLogger log;

    /** Grid instance injector. */
    private GridResourceBasicInjector<GridEx> gridInjector;

    /** GridGain home folder injector. */
    private GridResourceBasicInjector<String> ggHomeInjector;

    /** Grid name injector. */
    private GridResourceBasicInjector<String> ggNameInjector;

    /** MBean server injector. */
    private GridResourceBasicInjector<MBeanServer> mbeanServerInjector;

    /** Grid thread executor injector. */
    private GridResourceBasicInjector<Executor> execInjector;

    /** Local node ID injector. */
    private GridResourceBasicInjector<UUID> nodeIdInjector;

    /** Marshaller injector. */
    private GridResourceBasicInjector<GridMarshaller> marshallerInjector;

    /** Spring application context injector. */
    private GridResourceInjector springCtxInjector;

    /** Logger injector. */
    private GridResourceBasicInjector<IgniteLogger> logInjector;

    /** Service injector. */
    private GridResourceBasicInjector<Collection<GridService>> srvcInjector;

    /** Spring bean resources injector. */
    private GridResourceInjector springBeanInjector;

    /** Null injector for cleaning resources. */
    private final GridResourceInjector nullInjector = new GridResourceBasicInjector<>(null);

    /** Resource container. */
    private final GridResourceIoc ioc;

    /** */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Creates injector object.
     *
     * @param log Grid logger.
     * @param ioc Resource container for injections.
     */
    GridResourceCustomInjector(IgniteLogger log, GridResourceIoc ioc) {
        assert log != null;
        assert ioc != null;

        this.log = log;
        this.ioc = ioc;
    }

    /**
     * Sets injector with Grid instance.
     *
     * @param gridInjector Grid instance.
     */
    public void setGridInjector(GridResourceBasicInjector<GridEx> gridInjector) {
        this.gridInjector = gridInjector;
    }

    /**
     * Sets injector with GridGain home folder.
     *
     * @param ggHomeInjector GridGain home folder.
     */
    public void setGridgainHomeInjector(GridResourceBasicInjector<String> ggHomeInjector) {
        this.ggHomeInjector = ggHomeInjector;
    }

    /**
     * Sets injector with GridGain home folder.
     *
     * @param ggNameInjector Grid name injector.
     */
    public void setGridNameInjector(GridResourceBasicInjector<String> ggNameInjector) {
        this.ggNameInjector = ggNameInjector;
    }

    /**
     * Sets injector with MBean server.
     *
     * @param mbeanServerInjector MBean server.
     */
    public void setMbeanServerInjector(GridResourceBasicInjector<MBeanServer> mbeanServerInjector) {
        this.mbeanServerInjector = mbeanServerInjector;
    }

    /**
     * Sets injector with Grid thread executor.
     *
     * @param execInjector Grid thread executor.
     */
    public void setExecutorInjector(GridResourceBasicInjector<Executor> execInjector) {
        this.execInjector = execInjector;
    }

    /**
     * Sets injector with local node ID.
     *
     * @param nodeIdInjector Local node ID.
     */
    void setNodeIdInjector(GridResourceBasicInjector<UUID> nodeIdInjector) {
        this.nodeIdInjector = nodeIdInjector;
    }

    /**
     * Sets injector with marshaller.
     *
     * @param marshallerInjector Grid marshaller.
     */
    public void setMarshallerInjector(GridResourceBasicInjector<GridMarshaller> marshallerInjector) {
        this.marshallerInjector = marshallerInjector;
    }

    /**
     * Sets injector with Spring application context.
     *
     * @param springCtxInjector Spring application context.
     */
    void setSpringContextInjector(GridResourceInjector springCtxInjector) {
        this.springCtxInjector = springCtxInjector;
    }

    /**
     * Sets injector for Spring beans.
     *
     * @param springBeanInjector Injector for Spring beans.
     */
    public void setSpringBeanInjector(GridResourceInjector springBeanInjector) {
        this.springBeanInjector = springBeanInjector;
    }

    /**
     * Sets injector with log.
     *
     * @param logInjector Log injector.
     */
    public void setLogInjector(GridResourceBasicInjector<IgniteLogger> logInjector) {
        this.logInjector = logInjector;
    }

    /**
     * Sets injector for grid services.
     *
     * @param srvcInjector Service injector.
     */
    public void setSrvcInjector(GridResourceBasicInjector<Collection<GridService>> srvcInjector) {
        this.srvcInjector = srvcInjector;
    }

    /** {@inheritDoc} */
    @Override public void undeploy(GridDeployment dep) {
        lock.writeLock().lock();

        try {
            IgniteInClosure<Map<Class<?>, Map<String, CachedResource>>> x = new CI1<Map<Class<?>, Map<String, CachedResource>>>() {
                @Override public void apply(Map<Class<?>, Map<String, CachedResource>> map) {
                    if (map != null) {
                        for (Map<String, CachedResource> m : map.values()) {
                            if (m != null)
                                undeploy(m.values());
                        }
                    }
                }
            };

            Map<Class<?>, Map<String, CachedResource>> map = dep.removeMeta(CLS_LDR_RSRC_CACHE);

            x.apply(map);

            Map<Class<?>, Map<Class<?>, Map<String, CachedResource>>> clsRsrcs = dep.removeMeta(CLS_RSRC_CACHE);

            if (clsRsrcs != null)
                F.forEach(clsRsrcs.values(), x);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Undeploy collection of resources.
     * Every resource method marked with {@link org.apache.ignite.resources.IgniteUserResourceOnUndeployed}
     * annotation will be invoked before cleanup.
     *
     * @param rsrcs Resources to undeploy.
     */
    private void undeploy(Iterable<CachedResource> rsrcs) {
        assert lock.isWriteLockedByCurrentThread();

        for (CachedResource rsrc : rsrcs) {
            try {
                List<Method> finalizers = getMethodsWithAnnotation(rsrc.getResource().getClass(),
                    IgniteUserResourceOnUndeployed.class);

                for (Method mtd : finalizers) {
                    try {
                        mtd.setAccessible(true);

                        mtd.invoke(rsrc.getResource());
                    }
                    catch (IllegalAccessException | InvocationTargetException e) {
                        U.error(log, "Failed to finalize task shared resource [method=" + mtd + ", resource=" + rsrc +
                            ']', e);
                    }
                }
            }
            catch (GridException e) {
                U.error(log, "Failed to find finalizers for resource: " + rsrc, e);
            }

            // Clean up injected resources.
            cleanup(rsrc, IgniteLoggerResource.class);
            cleanup(rsrc, IgniteInstanceResource.class);
            cleanup(rsrc, IgniteExecutorServiceResource.class);
            cleanup(rsrc, IgniteLocalNodeIdResource.class);
            cleanup(rsrc, IgniteMBeanServerResource.class);
            cleanup(rsrc, IgniteHomeResource.class);
            cleanup(rsrc, IgniteMarshallerResource.class);
            cleanup(rsrc, IgniteSpringApplicationContextResource.class);
            cleanup(rsrc, IgniteSpringResource.class);
        }
    }

    /**
     * Cleanup object where resources was injected before.
     *
     * @param rsrc Object where resources should be cleaned.
     * @param annCls Annotation.
     */
    private void cleanup(CachedResource rsrc, Class<? extends Annotation> annCls) {
        try {
            ioc.inject(rsrc.getResource(), annCls, nullInjector, null, null);
        }
        catch (GridException e) {
            U.error(log, "Failed to clean up resource [ann=" + annCls + ", rsrc=" + rsrc + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceField field, Object target, Class<?> depCls,
        GridDeployment dep) throws GridException {
        assert dep != null;

        IgniteUserResource ann = (IgniteUserResource)field.getAnnotation();

        assert ann != null;

        if (!Modifier.isTransient(field.getField().getModifiers())) {
            throw new GridException("@GridUserResource must only be used with 'transient' fields: " +
                field.getField());
        }

        Class<?> rsrcCls = !ann.resourceClass().equals(Void.class) ? ann.resourceClass() :
            field.getField().getType();

        String rsrcName = ann.resourceName();

        GridResourceUtils.inject(field.getField(), target, getResource(dep, depCls, rsrcCls, rsrcName));
    }

    /** {@inheritDoc} */
    @Override public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep)
        throws GridException {
        assert dep != null;

        IgniteUserResource ann = (IgniteUserResource)mtd.getAnnotation();

        if (mtd.getMethod().getParameterTypes().length != 1)
            throw new GridException("Method injection setter must have only one parameter: " + mtd.getMethod());

        Class<?> rsrcCls = !ann.resourceClass().equals(Void.class) ? ann.resourceClass() :
            mtd.getMethod().getParameterTypes()[0];

        String rsrcName = ann.resourceName();

        GridResourceUtils.inject(mtd.getMethod(), target, getResource(dep, depCls, rsrcCls, rsrcName));
    }

    /**
     * Gets resource for defined task class.
     * If task resource not found it will be created with all necessary grid
     * injections.
     *
     * @param dep Deployment.
     * @param depCls Deployed class.
     * @param rsrcCls Resource class.
     * @param rsrcName Resource name.
     * @return Created resource.
     * @throws GridException If resource creation failed.
     */
    private Object getResource(GridDeployment dep, Class<?> depCls, Class<?> rsrcCls, String rsrcName)
        throws GridException {
        assert dep != null;

        // For performance reasons we first try to acquire read lock and
        // return the cached resource.
        lock.readLock().lock();

        try {
            Map<String, CachedResource> map = null;

            if (dep.deployMode() == GridDeploymentMode.PRIVATE) {
                Map<Class<?>, Map<Class<?>, Map<String, CachedResource>>> m = dep.meta(CLS_RSRC_CACHE);

                if (m != null) {
                    Map<Class<?>, Map<String, CachedResource>> m1 = m.get(depCls);

                    if (m1 != null)
                        map = m1.get(rsrcCls);
                }
            }
            else {
                Map<Class<?>, Map<String, CachedResource>> m = dep.meta(CLS_LDR_RSRC_CACHE);

                if (m != null)
                    map = m.get(rsrcCls);
            }

            if (map != null) {
                CachedResource rsrc = map.get(rsrcName);

                if (rsrc != null) {
                    if (log.isDebugEnabled())
                        log.debug("Read resource from cache: [rsrcCls=" + rsrcCls + ", rsrcName=" + rsrcName + ']');

                    return rsrc.getResource();
                }
            }
        }
        finally {
            lock.readLock().unlock();
        }

        // If resource was not cached, then
        // we acquire write lock and cache it.
        lock.writeLock().lock();

        try {
            Map<String, CachedResource> map;

            if (dep.deployMode() == GridDeploymentMode.PRIVATE) {
                Map<Class<?>, Map<Class<?>, Map<String, CachedResource>>> m = dep.addMetaIfAbsent(CLS_RSRC_CACHE,
                    F.<Class<?>, Map<Class<?>, Map<String, CachedResource>>>newMap());

                Map<Class<?>, Map<String, CachedResource>> m1 = F.addIfAbsent(m, depCls,
                    F.<Class<?>, Map<String, CachedResource>>newMap());

                map = F.addIfAbsent(m1, rsrcCls, F.<String, CachedResource>newMap());
            }
            else {
                Map<Class<?>, Map<String, CachedResource>> m = dep.addMetaIfAbsent(CLS_LDR_RSRC_CACHE,
                    F.<Class<?>, Map<String, CachedResource>>newMap());

                map = F.addIfAbsent(m, rsrcCls, F.<String, CachedResource>newMap());
            }

            CachedResource rsrc = map.get(rsrcName);

            if (rsrc == null) {
                rsrc = createResource(rsrcCls, dep, depCls);

                map.put(rsrcName, rsrc);

                if (log.isDebugEnabled()) {
                    log.debug("Created resource [rsrcCls=" + rsrcCls.getName() + ", rsrcName=" + rsrcName +
                        ", rsrc=" + rsrc + ", depCls=" + depCls + ", dep=" + dep + ']');
                }
            }

            return rsrc.getResource();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Creates object from class {@code rsrcCls} and inject all
     * necessary resources for it.
     *
     * @param rsrcCls Class in which resources should be injected.
     * @param dep Deployment.
     * @param depCls Deployed class.
     * @return Created object with injected resources.
     * @throws GridException Thrown in case of any errors during injection.
     */
    private CachedResource createResource(Class<?> rsrcCls, GridDeployment dep, Class<?> depCls) throws GridException {
        assert dep != null;

        try {
            Object rsrc = rsrcCls.newInstance();

            // Inject resources into shared resource.
            ioc.inject(rsrc, IgniteLoggerResource.class, new GridResourceBasicInjector<>(
                log.getLogger(rsrcCls)), dep, depCls);
            ioc.inject(rsrc, IgniteInstanceResource.class, gridInjector, dep, depCls);
            ioc.inject(rsrc, IgniteExecutorServiceResource.class, execInjector, dep, depCls);
            ioc.inject(rsrc, IgniteLocalNodeIdResource.class, nodeIdInjector, dep, depCls);
            ioc.inject(rsrc, IgniteMBeanServerResource.class, mbeanServerInjector, dep, depCls);
            ioc.inject(rsrc, IgniteHomeResource.class, ggHomeInjector, dep, depCls);
            ioc.inject(rsrc, IgniteNameResource.class, ggNameInjector, dep, depCls);
            ioc.inject(rsrc, IgniteMarshallerResource.class, marshallerInjector, dep, depCls);
            ioc.inject(rsrc, IgniteSpringApplicationContextResource.class, springCtxInjector, dep, depCls);
            ioc.inject(rsrc, IgniteSpringResource.class, springBeanInjector, dep, depCls);
            ioc.inject(rsrc, IgniteLoggerResource.class, logInjector, dep, depCls);
            ioc.inject(rsrc, IgniteServiceResource.class, srvcInjector, dep, depCls);

            for (Method mtd : getMethodsWithAnnotation(rsrcCls, IgniteUserResourceOnDeployed.class)) {
                mtd.setAccessible(true);

                mtd.invoke(rsrc);
            }

            return new CachedResource(rsrc, dep.classLoader());
        }
        catch (InstantiationException e) {
            throw new GridException("Failed to instantiate task shared resource: " + rsrcCls, e);
        }
        catch (IllegalAccessException e) {
            throw new GridException("Failed to access task shared resource (is class public?): " + rsrcCls, e);
        }
        catch (InvocationTargetException e) {
            throw new GridException("Failed to initialize task shared resource: " + rsrcCls, e);
        }
    }

    /**
     * Gets set of methods with given annotation.
     *
     * @param cls Class in which search for methods.
     * @param annCls Annotation.
     * @return Set of methods with given annotations.
     * @throws GridException Thrown in case when method contains parameters.
     */
    private List<Method> getMethodsWithAnnotation(Class<?> cls, Class<? extends Annotation> annCls)
        throws GridException {
        List<Method> mtds = new ArrayList<>();

        for (Class<?> c = cls; !c.equals(Object.class); c = c.getSuperclass()) {
            for (Method mtd : c.getDeclaredMethods()) {
                if (mtd.getAnnotation(annCls) != null) {
                    if (mtd.getParameterTypes().length > 0) {
                        throw new GridException("Task shared resource initialization or finalization method should " +
                            "not have parameters: " + mtd);
                    }

                    mtds.add(mtd);
                }
            }
        }

        return mtds;
    }

    /** */
    private static class CachedResource {
        /** */
        private final Object rsrc;

        /** */
        private final ClassLoader ldr;

        /**
         * @param rsrc Resource.
         * @param ldr Class loader.
         */
        CachedResource(Object rsrc, ClassLoader ldr) {
            this.rsrc = rsrc;
            this.ldr = ldr;
        }

        /**
         * Gets property rsrc.
         *
         * @return Property rsrc.
         */
        public Object getResource() {
            return rsrc;
        }

        /**
         * Gets property ldr.
         *
         * @return Property ldr.
         */
        public ClassLoader getClassLoader() {
            return ldr;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CachedResource.class, this);
        }
    }
}
