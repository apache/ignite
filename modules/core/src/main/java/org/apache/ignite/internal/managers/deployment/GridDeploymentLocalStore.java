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

package org.apache.ignite.internal.managers.deployment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.events.DeploymentEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.util.GridAnnotationsCache;
import org.apache.ignite.internal.util.GridClassLoaderCache;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.events.EventType.EVT_CLASS_DEPLOYED;
import static org.apache.ignite.events.EventType.EVT_CLASS_DEPLOY_FAILED;
import static org.apache.ignite.events.EventType.EVT_CLASS_UNDEPLOYED;
import static org.apache.ignite.events.EventType.EVT_TASK_DEPLOYED;
import static org.apache.ignite.events.EventType.EVT_TASK_DEPLOY_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_UNDEPLOYED;

/**
 * Storage for local deployments.
 */
class GridDeploymentLocalStore extends GridDeploymentStoreAdapter {
    /** Deployment cache by class name. */
    private final ConcurrentMap<String, ConcurrentLinkedDeque8<GridDeployment>> cache =
        new ConcurrentHashMap8<>();

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * @param spi Deployment SPI.
     * @param ctx Grid kernal context.
     * @param comm Deployment communication.
     */
    GridDeploymentLocalStore(DeploymentSpi spi, GridKernalContext ctx, GridDeploymentCommunication comm) {
        super(spi, ctx, comm);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        spi.setListener(new LocalDeploymentListener());

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        spi.setListener(null);

        Map<String, Collection<GridDeployment>> cp;

        synchronized (mux) {
            cp = new HashMap<String, Collection<GridDeployment>>(cache);

            for (Entry<String, Collection<GridDeployment>> entry : cp.entrySet())
                entry.setValue(new ArrayList<>(entry.getValue()));
        }

        for (Collection<GridDeployment> deps : cp.values()) {
            for (GridDeployment cls : deps)
                undeploy(cls.classLoader());
        }

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDeployment> getDeployments() {
        Collection<GridDeployment> deps = new ArrayList<>();

        synchronized (mux) {
            for (ConcurrentLinkedDeque8<GridDeployment> depList : cache.values())
                for (GridDeployment d : depList)
                    if (!deps.contains(d))
                        deps.add(d);
        }

        return deps;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDeployment getDeployment(IgniteUuid ldrId) {
        synchronized (mux) {
            for (ConcurrentLinkedDeque8<GridDeployment> deps : cache.values())
                for (GridDeployment dep : deps)
                    if (dep.classLoaderId().equals(ldrId))
                        return dep;
        }

        for (GridDeployment dep : ctx.task().getUsedDeployments())
            if (dep.classLoaderId().equals(ldrId))
                return dep;

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDeployment getDeployment(GridDeploymentMetadata meta) {
        if (log.isDebugEnabled())
            log.debug("Deployment meta for local deployment: " + meta);

        String alias = meta.alias();

        // Validate metadata.
        assert alias != null : "Meta is invalid: " + meta;

        GridDeployment dep = deployment(alias);

        if (dep != null) {
            if (log.isDebugEnabled())
                log.debug("Acquired deployment class from local cache: " + dep);

            return dep;
        }

        DeploymentResource rsrc = spi.findResource(alias);

        if (rsrc != null) {
            dep = deploy(ctx.config().getDeploymentMode(), rsrc.getClassLoader(), rsrc.getResourceClass(), alias,
                meta.record());

            assert dep != null;

            if (log.isDebugEnabled())
                log.debug("Acquired deployment class from SPI: " + dep);
        }
        // Auto-deploy.
        else {
            ClassLoader ldr = meta.classLoader();

            if (ldr == null) {
                ldr = Thread.currentThread().getContextClassLoader();

                // Safety.
                if (ldr == null)
                    ldr = U.gridClassLoader();
            }

            if (ldr instanceof GridDeploymentClassLoader) {
                if (log.isDebugEnabled())
                    log.debug("Skipping local auto-deploy (nested execution) [ldr=" + ldr + ", meta=" + meta + ']');

                return null;
            }

            try {
                // Check that class can be loaded.
                String clsName = meta.className();

                Class<?> cls = Class.forName(clsName != null ? clsName : alias, true, ldr);

                spi.register(ldr, cls);

                rsrc = spi.findResource(cls.getName());

                if (rsrc != null && rsrc.getResourceClass().equals(cls)) {
                    if (log.isDebugEnabled())
                        log.debug("Retrieved auto-loaded resource from spi: " + rsrc);

                    dep = deploy(ctx.config().getDeploymentMode(), ldr, cls, meta.alias(), meta.record());

                    assert dep != null;
                }
                else {
                    U.warn(log, "Failed to find resource from deployment SPI even after registering: " + meta);

                    return null;
                }
            }
            catch (ClassNotFoundException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Failed to load class for local auto-deployment [ldr=" + ldr + ", meta=" + meta + ']');

                return null;
            }
            catch (IgniteSpiException e) {
                U.error(log, "Failed to deploy local class with meta: " + meta, e);

                return null;
            }
        }

        if (log.isDebugEnabled())
            log.debug("Acquired deployment class: " + dep);

        return dep;
    }

    /**
     * @param alias Class alias.
     * @return Deployment.
     */
    @Nullable private GridDeployment deployment(String alias) {
        ConcurrentLinkedDeque8<GridDeployment> deps = cache.get(alias);

        if (deps != null) {
            GridDeployment dep = deps.peekFirst();

            if (dep != null && !dep.undeployed())
                return dep;
        }

        return null;
    }

    /**
     * @param depMode Deployment mode.
     * @param ldr Class loader to deploy.
     * @param cls Class.
     * @param alias Class alias.
     * @param recordEvt {@code True} to record event.
     * @return Deployment.
     */
    private GridDeployment deploy(DeploymentMode depMode, ClassLoader ldr, Class<?> cls, String alias,
        boolean recordEvt) {
        GridDeployment dep = null;

        synchronized (mux) {
            boolean fireEvt = false;

            try {
                ConcurrentLinkedDeque8<GridDeployment> cachedDeps = null;

                // Find existing class loader info.
                for (ConcurrentLinkedDeque8<GridDeployment> deps : cache.values()) {
                    for (GridDeployment d : deps) {
                        if (d.classLoader() == ldr) {
                            // Cache class and alias.
                            fireEvt = d.addDeployedClass(cls, alias);

                            cachedDeps = deps;

                            dep = d;

                            break;
                        }
                    }

                    if (cachedDeps != null)
                        break;
                }

                if (cachedDeps != null) {
                    assert dep != null;

                    cache.put(alias, cachedDeps);

                    if (!cls.getName().equals(alias))
                        // Cache by class name as well.
                        cache.put(cls.getName(), cachedDeps);

                    return dep;
                }

                IgniteUuid ldrId = IgniteUuid.fromUuid(ctx.localNodeId());

                String userVer = userVersion(ldr);

                dep = new GridDeployment(depMode, ldr, ldrId, userVer, cls.getName(), true);

                fireEvt = dep.addDeployedClass(cls, alias);

                assert fireEvt : "Class was not added to newly created deployment [cls=" + cls +
                    ", depMode=" + depMode + ", dep=" + dep + ']';

                ConcurrentLinkedDeque8<GridDeployment> deps =
                    F.addIfAbsent(cache, alias, F.<GridDeployment>newDeque());

                if (!deps.isEmpty()) {
                    for (GridDeployment d : deps) {
                        if (!d.undeployed()) {
                            U.error(log, "Found more than one active deployment for the same resource " +
                                "[cls=" + cls + ", depMode=" + depMode + ", dep=" + d + ']');

                            return null;
                        }
                    }
                }

                // Add at the beginning of the list for future fast access.
                deps.addFirst(dep);

                if (!cls.getName().equals(alias))
                    // Cache by class name as well.
                    cache.put(cls.getName(), deps);

                if (log.isDebugEnabled())
                    log.debug("Created new deployment: " + dep);
            }
            finally {
                if (fireEvt)
                    recordDeploy(cls, alias, recordEvt);
            }
        }

        return dep;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDeployment explicitDeploy(Class<?> cls, ClassLoader clsLdr) throws IgniteCheckedException {
        try {
            // Make sure not to deploy peer loaded tasks with non-local class loader,
            // if local one exists.
            if (clsLdr.getClass().equals(GridDeploymentClassLoader.class))
                clsLdr = clsLdr.getParent();

            spi.register(clsLdr, cls);

            GridDeployment dep = deployment(cls.getName());

            if (dep == null) {
                DeploymentResource rsrc = spi.findResource(cls.getName());

                if (rsrc != null && rsrc.getClassLoader() == clsLdr)
                    dep = deploy(ctx.config().getDeploymentMode(), rsrc.getClassLoader(),
                        rsrc.getResourceClass(), rsrc.getName(), true);
            }

            return dep;
        }
        catch (IgniteSpiException e) {
            recordDeployFailed(cls, clsLdr, true);

            // Avoid double wrapping.
            if (e.getCause() instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e.getCause();

            throw new IgniteDeploymentCheckedException("Failed to deploy class: " + cls.getName(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void explicitUndeploy(UUID nodeId, String rsrcName) {
        assert rsrcName != null;

        // Simply delegate to SPI.
        // Internal cache will be cleared once undeployment callback is received from SPI.
        spi.unregister(rsrcName);
    }

    /** {@inheritDoc} */
    @Override public void addParticipants(Map<UUID, IgniteUuid> allParticipants,
        Map<UUID, IgniteUuid> addedParticipants) {
        assert false;
    }

    /**
     * Records deploy event.
     * <p>
     * This needs to be called in synchronized block.
     *
     * @param cls Deployed class.
     * @param alias Class alias.
     * @param recordEvt Flag indicating whether to record events.
     */
    private void recordDeploy(Class<?> cls, String alias, boolean recordEvt) {
        assert cls != null;

        boolean isTask = isTask(cls);

        String msg = (isTask ? "Task" : "Class") + " locally deployed: " + cls;

        if (recordEvt && ctx.event().isRecordable(isTask ? EVT_TASK_DEPLOYED : EVT_CLASS_DEPLOYED)) {
            DeploymentEvent evt = new DeploymentEvent();

            evt.message(msg);
            evt.node(ctx.discovery().localNode());
            evt.type(isTask ? EVT_TASK_DEPLOYED : EVT_CLASS_DEPLOYED);
            evt.alias(alias);

            ctx.event().record(evt);
        }

        // Don't record JDK or Grid classes.
        if (U.isGrid(cls) || U.isJdk(cls))
            return;

        if (log.isInfoEnabled())
            log.info(msg);
    }

    /**
     * Records deploy event.
     *
     * @param cls Deployed class.
     * @param clsLdr Class loader.
     * @param recordEvt Flag indicating whether to record events.
     */
    @SuppressWarnings({"unchecked"})
    private void recordDeployFailed(Class<?> cls, ClassLoader clsLdr, boolean recordEvt) {
        assert cls != null;
        assert clsLdr != null;

        boolean isTask = isTask(cls);

        String msg = "Failed to deploy " + (isTask ? "task" : "class") + " [cls=" + cls + ", clsLdr=" + clsLdr + ']';

        if (recordEvt && ctx.event().isRecordable(isTask ? EVT_CLASS_DEPLOY_FAILED : EVT_TASK_DEPLOY_FAILED)) {
            String taskName = isTask ? U.getTaskName((Class<? extends ComputeTask<?, ?>>)cls) : null;

            DeploymentEvent evt = new DeploymentEvent();

            evt.message(msg);
            evt.node(ctx.discovery().localNode());
            evt.type(isTask(cls) ? EVT_CLASS_DEPLOY_FAILED : EVT_TASK_DEPLOY_FAILED);
            evt.alias(taskName);

            ctx.event().record(evt);
        }

        if (log.isInfoEnabled())
            log.info(msg);
    }

    /**
     * Records undeploy event.
     *
     * @param dep Undeployed class loader.
     */
    private void recordUndeploy(GridDeployment dep) {
        assert dep.undeployed();

        if (ctx.event().isRecordable(EVT_TASK_UNDEPLOYED) ||
            ctx.event().isRecordable(EVT_CLASS_UNDEPLOYED)) {
            for (Class<?> cls : dep.deployedClasses()) {
                boolean isTask = isTask(cls);

                String msg = isTask ? "Task locally undeployed: " + cls : "Class locally undeployed: " + cls;

                if (ctx.event().isRecordable(isTask ? EVT_TASK_UNDEPLOYED : EVT_CLASS_UNDEPLOYED)) {
                    DeploymentEvent evt = new DeploymentEvent();

                    evt.message(msg);
                    evt.node(ctx.discovery().localNode());
                    evt.type(isTask ? EVT_TASK_UNDEPLOYED : EVT_CLASS_UNDEPLOYED);
                    evt.alias(getAlias(dep, cls));

                    ctx.event().record(evt);
                }

                if (log.isInfoEnabled())
                    log.info(msg);
            }
        }
    }

    /**
     * Gets alias for a class.
     *
     * @param dep Deployment.
     * @param cls Class.
     * @return Alias for a class.
     */
    private String getAlias(GridDeployment dep, Class<?> cls) {
        String alias = cls.getName();

        if (isTask(cls)) {
            ComputeTaskName ann = dep.annotation(cls, ComputeTaskName.class);

            if (ann != null)
                alias = ann.value();
        }

        return alias;
    }

    /**
     * @param ldr Class loader to undeploy.
     */
    private void undeploy(ClassLoader ldr) {
        Collection<GridDeployment> doomed = new HashSet<>();

        synchronized (mux) {
            for (Iterator<ConcurrentLinkedDeque8<GridDeployment>> i1 = cache.values().iterator(); i1.hasNext();) {
                ConcurrentLinkedDeque8<GridDeployment> deps = i1.next();

                for (Iterator<GridDeployment> i2 = deps.iterator(); i2.hasNext();) {
                    GridDeployment dep = i2.next();

                    if (dep.classLoader() == ldr) {
                        dep.undeploy();

                        i2.remove();

                        doomed.add(dep);

                        if (log.isInfoEnabled())
                            log.info("Removed undeployed class: " + dep);
                    }
                }

                if (deps.isEmpty())
                    i1.remove();
            }
        }

        for (GridDeployment dep : doomed) {
            if (dep.obsolete()) {
                // Resource cleanup.
                ctx.resource().onUndeployed(dep);

                // Clear optimized marshaller's cache.
                if (ctx.config().getMarshaller() instanceof OptimizedMarshaller)
                    ((OptimizedMarshaller)ctx.config().getMarshaller()).onUndeploy(ldr);

                clearSerializationCaches();

                // Class loader cache should be cleared in the last order.
                GridAnnotationsCache.onUndeployed(ldr);
                GridClassLoaderCache.onUndeployed(ldr);
            }

            recordUndeploy(dep);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentLocalStore.class, this);
    }

    /**
     *
     */
    private class LocalDeploymentListener implements DeploymentListener {
        /** {@inheritDoc} */
        @Override public void onUnregistered(ClassLoader ldr) {
            if (log.isDebugEnabled())
                log.debug("Received callback from SPI to unregister class loader: " + ldr);

            undeploy(ldr);
        }
    }
}