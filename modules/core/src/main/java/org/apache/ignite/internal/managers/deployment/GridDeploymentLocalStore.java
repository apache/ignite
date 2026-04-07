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

import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.events.DeploymentEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.util.GridAnnotationsCache;
import org.apache.ignite.internal.util.GridClassLoaderCache;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.jetbrains.annotations.Nullable;

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
    /** Primary index: deployment by class loader. Not thread-safe, access must be guarded by {@link #mux}. */
    private final Map<ClassLoader, GridDeployment> depByLdr = new IdentityHashMap<>();

    /** Secondary index: deployments by alias or class name. Not thread-safe, access must be guarded by {@link #mux}. */
    private final Map<String, Map<ClassLoader, GridDeployment>> depsByAlias = new HashMap<>();

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

        Set<ClassLoader> ldrs = U.newIdentityHashSet();

        synchronized (mux) {
            ldrs.addAll(depByLdr.keySet());
        }

        for (ClassLoader ldr : ldrs)
            undeploy(ldr);

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        Set<ClassLoader> obsoleteClsLdrs = U.newIdentityHashSet();

        synchronized (mux) {
            // There can be obsolete class loaders in deployment indexes after client node reconnect with the new node id.
            for (GridDeployment dep : depByLdr.values())
                if (!dep.classLoaderId().globalId().equals(ctx.localNodeId()))
                    obsoleteClsLdrs.add(dep.classLoader());
        }

        for (ClassLoader clsLdr : obsoleteClsLdrs)
            undeploy(clsLdr);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDeployment> getDeployments() {
        Collection<GridDeployment> deps = U.newIdentityHashSet();

        synchronized (mux) {
            deps.addAll(depByLdr.values());
        }

        return deps;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDeployment getDeployment(IgniteUuid ldrId) {
        synchronized (mux) {
            for (GridDeployment dep : depByLdr.values())
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

        GridDeployment dep = deployment(meta);

        if (dep != null) {
            if (log.isDebugEnabled())
                log.debug("Acquired deployment class from local cache: " + dep);

            return dep;
        }

        if (meta.classLoader() == null) {
            DeploymentResource rsrc = spi.findResource(alias);

            if (rsrc != null) {
                dep = deploy(ctx.config().getDeploymentMode(), rsrc.getClassLoader(), rsrc.getResourceClass(), alias,
                    meta.record());

                assert dep != null;

                if (log.isDebugEnabled())
                    log.debug("Acquired deployment class from SPI: " + dep);

                return dep;
            }
        }

        // Auto-deploy.
        ClassLoader ldr = meta.classLoader();

        if (ldr == null) {
            ldr = Thread.currentThread().getContextClassLoader();

            // Safety.
            if (ldr == null)
                ldr = U.resolveClassLoader(ctx.config());
        }

        if (ldr instanceof GridDeploymentClassLoader) {
            if (log.isDebugEnabled())
                log.debug("Skipping local auto-deploy (nested execution) [ldr=" + ldr + ", meta=" + meta + ']');

            return null;
        }

        try {
            // Check that class can be loaded.
            String clsName = meta.className();

            Class<?> cls = U.forName(clsName != null ? clsName : alias, ldr);

            if (spi.register(ldr, cls)) {
                if (log.isDebugEnabled()) {
                    log.debug("Resource registered automatically: [name=" + U.getResourceName(cls)
                        + ", class=" + cls.getName()
                        + ", ldr=" + ldr + ']');
                }
            }

            dep = deploy(ctx.config().getDeploymentMode(), ldr, cls, alias, meta.record());
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

        if (log.isDebugEnabled())
            log.debug("Acquired deployment class: " + dep);

        return dep;
    }

    /** {@inheritDoc} */
    @Override public GridDeployment searchDeploymentCache(GridDeploymentMetadata meta) {
        return deployment(meta);
    }

    /**
     * @param meta Deployment meta.
     * @return Deployment.
     */
    @Nullable private GridDeployment deployment(final GridDeploymentMetadata meta) {
        Map<ClassLoader, GridDeployment> deps;

        synchronized (mux) {
            Map<ClassLoader, GridDeployment> cached = depsByAlias.get(meta.alias());

            deps = cached == null ? null : new IdentityHashMap<>(cached);
        }

        if (deps != null) {
            GridDeployment dep = null;

            if (meta.classLoader() != null)
                dep = deps.get(meta.classLoader());

            if ((dep == null || dep.undeployed()) && meta.classLoaderId() != null) {
                for (GridDeployment d : deps.values()) {
                    if (d.classLoaderId().equals(meta.classLoaderId()) && !d.undeployed()) {
                        dep = d;

                        break;
                    }
                }
            }

            if (dep != null && !dep.undeployed()) {
                if (log.isTraceEnabled())
                    log.trace("Deployment was found for class with specific class loader [alias=" + meta.alias() +
                        ", clsLdrId=" + meta.classLoaderId() + "]");

                return dep;
            }

            ClassLoader appLdr = Thread.currentThread().getContextClassLoader();

            if (appLdr == null)
                appLdr = U.resolveClassLoader(ctx.config());

            appLdr = (appLdr instanceof GridDeploymentClassLoader) ? null : appLdr;

            if (appLdr != null)
                dep = deps.get(appLdr);

            if (dep != null && !dep.undeployed()) {
                if (log.isTraceEnabled())
                    log.trace("Deployment was found for class with the local app class loader [alias="
                        + meta.alias() + "]");

                return dep;
            }
        }

        if (log.isDebugEnabled())
            log.debug("Deployment was not found for class with specific class loader [alias=" + meta.alias() +
                ", clsLdrId=" + meta.classLoaderId() + "]");

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
    private GridDeployment deploy(
        DeploymentMode depMode,
        ClassLoader ldr,
        Class<?> cls,
        String alias,
        boolean recordEvt
    ) {
        GridDeployment dep;

        synchronized (mux) {
            boolean fireEvt = false;

            try {
                dep = depByLdr.get(ldr);

                if (dep != null) {
                    fireEvt = dep.addDeployedClass(cls, alias);

                    addAliasMapping(alias, ldr, dep);

                    if (!cls.getName().equals(alias))
                        addAliasMapping(cls.getName(), ldr, dep);

                    return dep;
                }

                IgniteUuid ldrId = IgniteUuid.fromUuid(ctx.localNodeId());

                String userVer = userVersion(ldr);

                dep = new GridDeployment(depMode, ldr, ldrId, userVer, cls.getName(), true);

                fireEvt = dep.addDeployedClass(cls, alias);

                assert fireEvt : "Class was not added to newly created deployment [cls=" + cls +
                    ", depMode=" + depMode + ", dep=" + dep + ']';

                depByLdr.put(ldr, dep);

                addAliasMapping(alias, ldr, dep);

                if (!cls.getName().equals(alias))
                    addAliasMapping(cls.getName(), ldr, dep);

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

            if (spi.register(clsLdr, cls)) {
                if (log.isDebugEnabled()) {
                    log.debug("Resource registered automatically: [name=" + U.getResourceName(cls)
                        + ", class=" + cls.getName() + ", ldr=" + clsLdr + ']');
                }
            }

            GridDeploymentMetadata meta = new GridDeploymentMetadata();

            meta.alias(cls.getName());
            meta.classLoader(clsLdr);

            GridDeployment dep = deployment(meta);

            if (dep == null) {
                dep = deploy(ctx.config().getDeploymentMode(), clsLdr,
                    cls, U.getResourceName(cls), true);
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
            evt.type(isTask ? EVT_CLASS_DEPLOY_FAILED : EVT_TASK_DEPLOY_FAILED);
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
        GridDeployment dep;

        synchronized (mux) {
            dep = depByLdr.remove(ldr);

            if (dep != null) {
                dep.undeploy();

                if (log.isInfoEnabled())
                    log.info("Removed undeployed class: " + dep);

                for (Iterator<Map<ClassLoader, GridDeployment>> it = depsByAlias.values().iterator(); it.hasNext();) {
                    Map<ClassLoader, GridDeployment> deps = it.next();

                    deps.remove(ldr);

                    if (deps.isEmpty())
                        it.remove();
                }
            }
        }

        if (dep != null) {
            if (dep.obsolete()) {
                // Resource cleanup.
                ctx.resource().onUndeployed(dep);

                // Clear optimized marshaller's cache.
                ctx.marshaller().onUndeploy(ldr);

                clearSerializationCaches();

                // Class loader cache should be cleared in the last order.
                GridAnnotationsCache.onUndeployed(ldr);
                GridClassLoaderCache.onUndeployed(ldr);
            }

            recordUndeploy(dep);
        }
    }

    /**
     * Adds deployment to the alias-based index. Must be called under {@link #mux}.
     *
     * @param key Alias or classname.
     * @param ldr Class loader.
     * @param dep Deployment.
     */
    private void addAliasMapping(String key, ClassLoader ldr, GridDeployment dep) {
        Map<ClassLoader, GridDeployment> deps = depsByAlias.computeIfAbsent(key, k -> new IdentityHashMap<>());

        deps.put(ldr, dep);
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
