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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.*;
import org.apache.ignite.internal.managers.deployment.protocol.gg.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.deployment.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.configuration.DeploymentMode.*;

/**
 * Deployment manager.
 */
public class GridDeploymentManager extends GridManagerAdapter<DeploymentSpi> {
    /** Local deployment storage. */
    private GridDeploymentStore locStore;

    /** Isolated mode storage. */
    private GridDeploymentStore ldrStore;

    /** Shared mode storage. */
    private GridDeploymentStore verStore;

    /** */
    private GridDeploymentCommunication comm;

    /** */
    private final GridDeployment locDep;

    /**
     * @param ctx Grid kernal context.
     */
    public GridDeploymentManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getDeploymentSpi());

        if (!ctx.config().isPeerClassLoadingEnabled()) {
            DeploymentSpi spi = ctx.config().getDeploymentSpi();

            IgnoreIfPeerClassLoadingDisabled ann = U.getAnnotation(spi.getClass(),
                IgnoreIfPeerClassLoadingDisabled.class);

            locDep = ann != null ?
                new LocalDeployment(
                    ctx.config().getDeploymentMode(),
                    ctx.config().getClassLoader() != null ? ctx.config().getClassLoader() : U.gridClassLoader(),
                    IgniteUuid.fromUuid(ctx.localNodeId()),
                    ctx.userVersion(U.gridClassLoader()),
                    String.class.getName()) :
                null;
        }
        else
            locDep = null;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        GridProtocolHandler.registerDeploymentManager(this);

        assertParameter(ctx.config().getDeploymentMode() != null, "ctx.config().getDeploymentMode() != null");

        if (ctx.config().isPeerClassLoadingEnabled())
            assertParameter(ctx.config().getNetworkTimeout() > 0, "networkTimeout > 0");

        startSpi();

        comm = new GridDeploymentCommunication(ctx, log);

        comm.start();

        startStores();

        if (log.isDebugEnabled()) {
            log.debug("Local deployment: " + locDep);

            log.debug(startInfo());
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        storesOnKernalStop();

        storesStop();

        startStores();
    }

    /** {@inheritDoc} */
    @Override public void onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        storesOnKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        GridProtocolHandler.deregisterDeploymentManager();

        storesStop();

        if (comm != null)
            comm.stop();

        getSpi().setListener(null);

        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart0() throws IgniteCheckedException {
        storesOnKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop0(boolean cancel) {
        storesOnKernalStop();
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return super.enabled() && locDep == null;
    }

    /**
     * @param p Filtering predicate.
     * @return All deployed tasks for given predicate.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Class<? extends ComputeTask<?, ?>>> findAllTasks(
        @Nullable IgnitePredicate<? super Class<? extends ComputeTask<?, ?>>>... p) {
        Map<String, Class<? extends ComputeTask<?, ?>>> map = new HashMap<>();
        if (locDep != null)
            tasks(map, locDep, p);
        else {
            Collection<GridDeployment> deps = locStore.getDeployments();

            for (GridDeployment dep : deps)
                tasks(map, dep, p);
        }

        return map;
    }

    /**
     * @param map Map (out parameter).
     * @param dep Deployment.
     * @param p Predicate.
     */
    private void tasks(Map<String, Class<? extends ComputeTask<?, ?>>> map, GridDeployment dep,
        IgnitePredicate<? super Class<? extends ComputeTask<?, ?>>>[] p) {
        assert map != null;
        assert dep != null;

        for (Map.Entry<String, Class<?>> clsEntry : dep.deployedClassMap().entrySet()) {
            if (ComputeTask.class.isAssignableFrom(clsEntry.getValue())) {
                Class<? extends ComputeTask<?, ?>> taskCls = (Class<? extends ComputeTask<?, ?>>)clsEntry.getValue();

                if (F.isAll(taskCls, p))
                    map.put(clsEntry.getKey(), taskCls);
            }
        }
    }

    /**
     * @param taskName Task name.
     * @param locUndeploy Local undeploy flag.
     * @param rmtNodes Nodes to send request to.
     */
    public void undeployTask(String taskName, boolean locUndeploy, Collection<ClusterNode> rmtNodes) {
        assert taskName != null;
        assert !rmtNodes.contains(ctx.discovery().localNode());

        if (locDep == null) {
            if (locUndeploy)
                locStore.explicitUndeploy(null, taskName);

            try {
                comm.sendUndeployRequest(taskName, rmtNodes);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send undeployment request for task: " + taskName, e);
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param taskName Task name.
     */
    void undeployTask(UUID nodeId, String taskName) {
        assert taskName != null;

        if (locDep != null) {
            U.warn(log, "Received unexpected undeploy request [nodeId=" + nodeId + ", taskName=" + taskName + ']');

            return;
        }

        locStore.explicitUndeploy(nodeId, taskName);
        ldrStore.explicitUndeploy(nodeId, taskName);
        verStore.explicitUndeploy(nodeId, taskName);
    }

    /**
     * @param cls Class to deploy.
     * @param clsLdr Class loader.
     * @throws IgniteCheckedException If deployment failed.
     * @return Grid deployment.
     */
    @Nullable public GridDeployment deploy(Class<?> cls, ClassLoader clsLdr) throws IgniteCheckedException {
        if (clsLdr == null)
            clsLdr = getClass().getClassLoader();

        String clsName = cls.getName();

        String lambdaParent = U.lambdaEnclosingClassName(clsName);

        if (lambdaParent != null) {
            clsName = lambdaParent;

            // Need to override passed in class if class is Lambda.
            try {
                cls = Class.forName(clsName, true, clsLdr);
            }
            catch (ClassNotFoundException e) {
                throw new IgniteCheckedException("Cannot deploy parent class for lambda: " + clsName, e);
            }
        }

        if (clsLdr instanceof GridDeploymentClassLoader) {
            GridDeploymentInfo ldr = (GridDeploymentInfo)clsLdr;

            // Expecting that peer-deploy awareness handled on upper level.
            if ((ldr.deployMode() == ISOLATED || ldr.deployMode() == PRIVATE) &&
                (ctx.config().getDeploymentMode() == SHARED || ctx.config().getDeploymentMode() == CONTINUOUS) &&
                !U.hasAnnotation(cls, GridInternal.class))
                throw new IgniteCheckedException("Attempt to deploy class loaded in ISOLATED or PRIVATE mode on node with " +
                    "SHARED or CONTINUOUS deployment mode [cls=" + cls + ", clsDeployMode=" + ldr.deployMode() +
                    ", localDeployMode=" + ctx.config().getDeploymentMode() + ']');

            GridDeploymentMetadata meta = new GridDeploymentMetadata();

            meta.alias(clsName);
            meta.classLoader(clsLdr);

            // Check for nested execution. In that case, if task
            // is available locally by name, then we should ignore
            // class loader ID.
            GridDeployment dep = locStore.getDeployment(meta);

            if (dep == null) {
                dep = ldrStore.getDeployment(ldr.classLoaderId());

                if (dep == null)
                    dep = verStore.getDeployment(ldr.classLoaderId());
            }

            return dep;
        }
        else if (locDep != null) {
            if (ComputeTask.class.isAssignableFrom(cls)) {
                ComputeTaskName taskNameAnn = locDep.annotation(cls, ComputeTaskName.class);

                if (taskNameAnn != null)
                    locDep.addDeployedClass(cls, taskNameAnn.value());
            }

            return locDep;
        }
        else
            return locStore.explicitDeploy(cls, clsLdr);
    }

    /**
     * Gets any deployment by loader ID.
     *
     * @param ldrId Loader ID.
     * @return Deployment for given ID.
     */
    @Nullable public GridDeployment getDeployment(IgniteUuid ldrId) {
        if (locDep != null)
           return locDep.classLoaderId().equals(ldrId) ? locDep : null;

        GridDeployment dep = locStore.getDeployment(ldrId);

        if (dep == null) {
            dep = ldrStore.getDeployment(ldrId);

            if (dep == null)
                dep = verStore.getDeployment(ldrId);
        }

        return dep;
    }

    /**
     * @param rsrcName Resource to find deployment for.
     * @return Found deployment or {@code null} if one was not found.
     */
    @Nullable public GridDeployment getDeployment(String rsrcName) {
        if (locDep != null)
            return locDep;

        GridDeployment dep = getLocalDeployment(rsrcName);

        if (dep == null) {
            ClassLoader ldr = Thread.currentThread().getContextClassLoader();

            if (ldr instanceof GridDeploymentClassLoader) {
                GridDeploymentInfo depLdr = (GridDeploymentInfo)ldr;

                dep = ldrStore.getDeployment(depLdr.classLoaderId());

                if (dep == null)
                    dep = verStore.getDeployment(depLdr.classLoaderId());
            }
        }

        return dep;
    }

    /**
     * @param rsrcName Class name.
     * @return Grid cached task.
     */
    @Nullable public GridDeployment getLocalDeployment(String rsrcName) {
        if (locDep != null)
            return locDep;

        String lambdaEnclosingClsName = U.lambdaEnclosingClassName(rsrcName);

        String clsName = lambdaEnclosingClsName == null ? rsrcName : lambdaEnclosingClsName;

        GridDeploymentMetadata meta = new GridDeploymentMetadata();

        meta.record(true);
        meta.deploymentMode(ctx.config().getDeploymentMode());
        meta.alias(rsrcName);
        meta.className(clsName);
        meta.senderNodeId(ctx.localNodeId());

        return locStore.getDeployment(meta);
    }

    /**
     * @param depMode Deployment mode.
     * @param rsrcName Resource name (could be task name).
     * @param clsName Class name.
     * @param userVer User version.
     * @param sndNodeId Sender node ID.
     * @param clsLdrId Class loader ID.
     * @param participants Node class loader participant map.
     * @param nodeFilter Node filter for class loader.
     * @return Deployment class if found.
     */
    @Nullable public GridDeployment getGlobalDeployment(
        DeploymentMode depMode,
        String rsrcName,
        String clsName,
        String userVer,
        UUID sndNodeId,
        IgniteUuid clsLdrId,
        Map<UUID, IgniteUuid> participants,
        @Nullable IgnitePredicate<ClusterNode> nodeFilter) {
        if (locDep != null)
            return locDep;

        String lambdaEnclosingClsName = U.lambdaEnclosingClassName(clsName);

        if (lambdaEnclosingClsName != null)
            clsName = lambdaEnclosingClsName;

        GridDeploymentMetadata meta = new GridDeploymentMetadata();

        meta.deploymentMode(depMode);
        meta.className(clsName);
        meta.alias(rsrcName);
        meta.userVersion(userVer);
        meta.senderNodeId(sndNodeId);
        meta.classLoaderId(clsLdrId);
        meta.participants(participants);
        meta.nodeFilter(nodeFilter);

        if (!ctx.config().isPeerClassLoadingEnabled()) {
            meta.record(true);

            return locStore.getDeployment(meta);
        }

        // In shared mode, if class is locally available, we never load
        // from remote node simply because the class loader needs to be "shared".
        if (isPerVersionMode(meta.deploymentMode())) {
            meta.record(true);

            boolean reuse = true;

            // Check local exclusions.
            if (!sndNodeId.equals(ctx.localNodeId())) {
                String[] p2pExc = ctx.config().getPeerClassLoadingLocalClassPathExclude();

                if (p2pExc != null) {
                    for (String rsrc : p2pExc) {
                        // Remove star (*) at the end.
                        if (rsrc.endsWith("*"))
                            rsrc = rsrc.substring(0, rsrc.length() - 1);

                        if (meta.alias().startsWith(rsrc) || meta.className().startsWith(rsrc)) {
                            if (log.isDebugEnabled())
                                log.debug("Will not reuse local deployment because resource is excluded [meta=" +
                                    meta + ']');

                            reuse = false;

                            break;
                        }
                    }
                }
            }

            if (reuse) {
                GridDeployment locDep = locStore.getDeployment(meta);

                if (locDep == null && participants != null && participants.containsKey(ctx.localNodeId()))
                    locDep = locStore.getDeployment(participants.get(ctx.localNodeId()));

                if (locDep != null) {
                    if (!isPerVersionMode(locDep.deployMode())) {
                        U.warn(log, "Failed to deploy class in SHARED or CONTINUOUS mode (class is locally deployed " +
                            "in some other mode). Either change IgniteConfiguration.getDeploymentMode() property to " +
                            "SHARED or CONTINUOUS or remove class from local classpath and any of " +
                            "the local GAR deployments that may have it [cls=" + meta.className() + ", depMode=" +
                            locDep.deployMode() + ']', "Failed to deploy class in SHARED or CONTINUOUS mode.");

                        return null;
                    }

                    if (!locDep.userVersion().equals(meta.userVersion())) {
                        U.warn(log, "Failed to deploy class in SHARED or CONTINUOUS mode for given user version " +
                            "(class is locally deployed for a different user version) [cls=" + meta.className() +
                            ", localVer=" + locDep.userVersion() + ", otherVer=" + meta.userVersion() + ']',
                            "Failed to deploy class in SHARED or CONTINUOUS mode.");

                        return null;
                    }

                    if (log.isDebugEnabled())
                        log.debug("Reusing local deployment for SHARED or CONTINUOUS mode: " + locDep);

                    return locDep;
                }
            }

            return verStore.getDeployment(meta);
        }

        // Private or Isolated mode.
        meta.record(false);

        GridDeployment dep = locStore.getDeployment(meta);

        if (sndNodeId.equals(ctx.localNodeId())) {
            if (dep == null)
                U.warn(log, "Task got undeployed while deployment was in progress: " + meta);

            // For local execution, return the same deployment as for the task.
            return dep;
        }

        if (dep != null)
            meta.parentLoader(dep.classLoader());

        meta.record(true);

        return ldrStore.getDeployment(meta);
    }

    /**
     * Adds participants to all SHARED deployments.
     *
     * @param allParticipants All participants.
     * @param addedParticipants Added participants.
     */
    public void addCacheParticipants(Map<UUID, IgniteUuid> allParticipants, Map<UUID, IgniteUuid> addedParticipants) {
        verStore.addParticipants(allParticipants, addedParticipants);
    }

    /**
     * @param mode Mode to check.
     * @return {@code True} if shared mode.
     */
    private boolean isPerVersionMode(DeploymentMode mode) {
        return mode == DeploymentMode.CONTINUOUS || mode == DeploymentMode.SHARED;
    }

    /**
     * @param ldr Class loader to get ID for.
     * @return ID for given class loader or {@code null} if given loader is not
     *      grid deployment class loader.
     */
    @Nullable public IgniteUuid getClassLoaderId(ClassLoader ldr) {
        assert ldr != null;

        return ldr instanceof GridDeploymentClassLoader ? ((GridDeploymentInfo)ldr).classLoaderId() : null;
    }

    /**
     * @param ldr Loader to check.
     * @return {@code True} if P2P class loader.
     */
    public boolean isGlobalLoader(ClassLoader ldr) {
        return ldr instanceof GridDeploymentClassLoader;
    }


    /**
     * @throws IgniteCheckedException If failed.
     */
    private void startStores() throws IgniteCheckedException {
        locStore = new GridDeploymentLocalStore(getSpi(), ctx, comm);
        ldrStore = new GridDeploymentPerLoaderStore(getSpi(), ctx, comm);
        verStore = new GridDeploymentPerVersionStore(getSpi(), ctx, comm);

        locStore.start();
        ldrStore.start();
        verStore.start();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void storesOnKernalStart() throws IgniteCheckedException {
        locStore.onKernalStart();
        ldrStore.onKernalStart();
        verStore.onKernalStart();
    }

    /**
     *
     */
    private void storesOnKernalStop() {
        if (verStore != null)
            verStore.onKernalStop();

        if (ldrStore != null)
            ldrStore.onKernalStop();

        if (locStore != null)
            locStore.onKernalStop();
    }

    /**
     *
     */
    private void storesStop() {
        if (verStore != null)
            verStore.stop();

        if (ldrStore != null)
            ldrStore.stop();

        if (locStore != null)
            locStore.stop();
    }

    /**
     *
     */
    private static class LocalDeployment extends GridDeployment {
        /**
         * @param depMode Mode.
         * @param clsLdr Loader.
         * @param clsLdrId Loader ID.
         * @param userVer User version.
         * @param sampleClsName Sample class name.
         */
        private LocalDeployment(DeploymentMode depMode, ClassLoader clsLdr, IgniteUuid clsLdrId, String userVer,
            String sampleClsName) {
            super(depMode, clsLdr, clsLdrId, userVer, sampleClsName, /*local*/true);
        }

        /** {@inheritDoc} */
        @Override public boolean undeployed() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void undeploy() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean pendingUndeploy() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void onUndeployScheduled() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean acquire() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void release() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean obsolete() {
            return false;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<UUID, IgniteUuid> participants() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LocalDeployment.class, this, super.toString());
        }
    }
}
