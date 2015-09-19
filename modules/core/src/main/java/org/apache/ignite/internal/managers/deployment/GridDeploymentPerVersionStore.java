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

import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.events.DeploymentEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridAnnotationsCache;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.GridClassLoaderCache;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.apache.ignite.events.EventType.EVT_CLASS_DEPLOYED;
import static org.apache.ignite.events.EventType.EVT_CLASS_UNDEPLOYED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_TASK_DEPLOYED;
import static org.apache.ignite.events.EventType.EVT_TASK_UNDEPLOYED;

/**
 * Deployment storage for {@link org.apache.ignite.configuration.DeploymentMode#SHARED} and
 * {@link org.apache.ignite.configuration.DeploymentMode#CONTINUOUS} modes.
 */
public class GridDeploymentPerVersionStore extends GridDeploymentStoreAdapter {
    /** Shared deployment cache. */
    private final Map<String, List<SharedDeployment>> cache = new HashMap<>();

    /** Set of obsolete class loaders. */
    private final Collection<IgniteUuid> deadClsLdrs = new GridBoundedConcurrentLinkedHashSet<>(1024, 64);

    /** Discovery listener. */
    private GridLocalEventListener discoLsnr;

    /**
     * Resources hits/misses cache (by loader ID). If missed resources cache is turned off,
     * only hits are cached.
     */
    private final ConcurrentMap<IgniteUuid, Map<String, Boolean>> rsrcCache;

    /** Mutex. */
    private final Object mux = new Object();

    /** Missed resources cache size. */
    private final int missedRsrcCacheSize;

    /** Lock to synchronize remote load checks. */
    private final GridStripedLock loadRmtLock = new GridStripedLock(16);

    /**
     * @param spi Underlying SPI.
     * @param ctx Grid kernal context.
     * @param comm Deployment communication.
     */
    GridDeploymentPerVersionStore(DeploymentSpi spi, GridKernalContext ctx, GridDeploymentCommunication comm) {
        super(spi, ctx, comm);

        missedRsrcCacheSize = ctx.config().getPeerClassLoadingMissedResourcesCacheSize();

        rsrcCache = new ConcurrentHashMap8<>();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        discoLsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt instanceof DiscoveryEvent;

                assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                Collection<SharedDeployment> undeployed = new LinkedList<>();

                if (log.isDebugEnabled())
                    log.debug("Processing node departure event: " + evt);

                synchronized (mux) {
                    for (Iterator<List<SharedDeployment>> i1 = cache.values().iterator(); i1.hasNext();) {
                        List<SharedDeployment> deps = i1.next();

                        for (Iterator<SharedDeployment> i2 = deps.iterator(); i2.hasNext();) {
                            SharedDeployment dep = i2.next();

                            dep.removeParticipant(discoEvt.eventNode().id());

                            if (!dep.hasParticipants()) {
                                if (dep.deployMode() == SHARED) {
                                    if (!dep.undeployed()) {
                                        dep.undeploy();

                                        // Undeploy.
                                        i2.remove();

                                        assert !dep.isRemoved();

                                        dep.onRemoved();

                                        undeployed.add(dep);

                                        if (log.isDebugEnabled())
                                            log.debug("Undeployed class loader as there are no participating " +
                                                "nodes: " + dep);
                                    }
                                }
                                else if (log.isDebugEnabled())
                                    log.debug("Preserving deployment without node participants: " + dep);
                            }
                            else if (log.isDebugEnabled())
                                log.debug("Keeping deployment as it still has participants: " + dep);
                        }

                        if (deps.isEmpty())
                            i1.remove();
                    }
                }

                recordUndeployed(discoEvt.eventNode().id(), undeployed);
            }
        };

        ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        Collection<SharedDeployment> cp = new HashSet<>();

        synchronized (mux) {
            for (List<SharedDeployment> deps : cache.values())
                for (SharedDeployment dep : deps) {
                    // Mark undeployed.
                    dep.undeploy();

                    cp.add(dep);
                }

            cache.clear();
        }

        for (SharedDeployment dep : cp)
            dep.recordUndeployed(null);

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        Collection<SharedDeployment> undeployed = new LinkedList<>();

        synchronized (mux) {
            for (Iterator<List<SharedDeployment>> i1 = cache.values().iterator(); i1.hasNext();) {
                List<SharedDeployment> deps = i1.next();

                for (Iterator<SharedDeployment> i2 = deps.iterator(); i2.hasNext();) {
                    SharedDeployment dep = i2.next();

                    for (UUID nodeId : dep.getParticipantNodeIds())
                        if (ctx.discovery().node(nodeId) == null)
                            dep.removeParticipant(nodeId);

                    if (!dep.hasParticipants()) {
                        if (dep.deployMode() == SHARED) {
                            if (!dep.undeployed()) {
                                dep.undeploy();

                                // Undeploy.
                                i2.remove();

                                dep.onRemoved();

                                undeployed.add(dep);

                                if (log.isDebugEnabled())
                                    log.debug("Undeployed class loader as there are no participating nodes: " + dep);
                            }
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Preserving deployment without node participants: " + dep);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Keeping deployment as it still has participants: " + dep);
                }

                if (deps.isEmpty())
                    i1.remove();
            }
        }

        recordUndeployed(null, undeployed);

        if (log.isDebugEnabled())
            log.debug("Registered deployment discovery listener: " + discoLsnr);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        if (discoLsnr != null) {
            ctx.event().removeLocalEventListener(discoLsnr);

            if (log.isDebugEnabled())
                log.debug("Unregistered deployment discovery listener: " + discoLsnr);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDeployment> getDeployments() {
        Collection<GridDeployment> deps = new LinkedList<>();

        synchronized (mux) {
            for (List<SharedDeployment> list : cache.values())
                for (SharedDeployment d : list)
                    deps.add(d);
        }

        return deps;
    }

    /** {@inheritDoc} */
    @Override public GridDeployment getDeployment(final IgniteUuid ldrId) {
        synchronized (mux) {
            return F.find(F.flat(cache.values()), null, new P1<SharedDeployment>() {
                @Override public boolean apply(SharedDeployment d) {
                    return d.classLoaderId().equals(ldrId);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridDeployment getDeployment(GridDeploymentMetadata meta) {
        assert meta != null;

        assert ctx.config().isPeerClassLoadingEnabled();

        // Validate metadata.
        assert meta.classLoaderId() != null;
        assert meta.senderNodeId() != null;
        assert meta.sequenceNumber() >= -1;
        assert meta.parentLoader() == null;

        if (log.isDebugEnabled())
            log.debug("Starting to peer-load class based on deployment metadata: " + meta);

        if (meta.participants() == null && !checkLoadRemoteClass(meta.className(), meta)) {
            if (log.isDebugEnabled())
                log.debug("Skipping deployment check as remote node does not have required class: " + meta);

            return null;
        }

        while (true) {
            List<SharedDeployment> depsToCheck = null;

            SharedDeployment dep = null;

            synchronized (mux) {
                // Check obsolete request.
                if (isDeadClassLoader(meta))
                    return null;

                if (meta.participants() != null && !meta.participants().isEmpty()) {
                    Map<UUID, IgniteUuid> participants = new LinkedHashMap<>();

                    for (Map.Entry<UUID, IgniteUuid> e : meta.participants().entrySet()) {
                        // Warn if local node is in participants.
                        if (ctx.localNodeId().equals(e.getKey())) {
                            // Warn only if mode is not CONTINUOUS.
                            if (meta.deploymentMode() != CONTINUOUS)
                                LT.warn(log, null, "Local node is in participants (most probably, " +
                                    "IgniteConfiguration.getPeerClassLoadingLocalClassPathExclude() " +
                                    "is not used properly " +
                                    "[locNodeId=" + ctx.localNodeId() + ", meta=" + meta + ']');

                            continue;
                        }

                        // Skip left nodes.
                        if (ctx.discovery().node(e.getKey()) != null)
                            participants.put(e.getKey(), e.getValue());
                    }

                    if (!participants.isEmpty()) {
                        // Set new participants.
                        meta = new GridDeploymentMetadata(meta);

                        meta.participants(participants);

                        if (log.isDebugEnabled())
                            log.debug("Created new meta with updated participants: " + meta);
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("All participants has gone: " + meta);

                        // Should try locally cached deployments in CONTINUOUS mode because local node
                        // can have this classes deployed.
                        if (meta.deploymentMode() != CONTINUOUS)
                            return null;
                    }
                }
                else if (ctx.discovery().node(meta.senderNodeId()) == null) {
                    if (log.isDebugEnabled())
                        log.debug("Sender node has gone: " + meta);

                    return null;
                }

                List<SharedDeployment> deps = cache.get(meta.userVersion());

                if (deps != null) {
                    assert !deps.isEmpty();

                    for (SharedDeployment d : deps) {
                        if (d.hasParticipant(meta.senderNodeId(), meta.classLoaderId()) ||
                            meta.senderNodeId().equals(ctx.localNodeId())) {
                            // Done.
                            dep = d;

                            break;
                        }
                    }

                    if (dep == null) {
                        checkRedeploy(meta);

                        // Find existing deployments that need to be checked
                        // whether they should be reused for this request.
                        for (SharedDeployment d : deps) {
                            if (!d.pendingUndeploy() && !d.undeployed()) {
                                Map<UUID, IgniteUuid> parties = d.participants();

                                if (parties != null) {
                                    IgniteUuid ldrId = parties.get(meta.senderNodeId());

                                    if (ldrId != null) {
                                        assert !ldrId.equals(meta.classLoaderId());

                                        if (log.isDebugEnabled())
                                            log.debug("Skipping deployment (loaders on remote node are different) " +
                                                "[dep=" + d + ", meta=" + meta + ']');

                                        continue;
                                    }
                                }

                                if (depsToCheck == null)
                                    depsToCheck = new LinkedList<>();

                                if (log.isDebugEnabled())
                                    log.debug("Adding deployment to check: " + d);

                                depsToCheck.add(d);
                            }
                        }

                        // If no deployment can be reused, create a new one.
                        if (depsToCheck == null) {
                            dep = createNewDeployment(meta, false);

                            deps.add(dep);
                        }
                    }
                }
                else {
                    checkRedeploy(meta);

                    // Create peer class loader.
                    dep = createNewDeployment(meta, true);
                }
            }

            if (dep != null) {
                if (log.isDebugEnabled())
                    log.debug("Found SHARED or CONTINUOUS deployment after first check: " + dep);

                // Cache the deployed class.
                Class<?> cls = dep.deployedClass(meta.className(), meta.alias());

                if (cls == null) {
                    U.warn(log, "Failed to load peer class (ignore if class got undeployed during preloading) [alias=" +
                        meta.alias() + ", dep=" + dep + ']');

                    return null;
                }

                return dep;
            }

            assert meta.parentLoader() == null;
            assert depsToCheck != null;
            assert !depsToCheck.isEmpty();

            /*
             * Logic below must be performed outside of synchronization
             * because it involves network calls.
             */

            // Check if class can be loaded from existing nodes.
            // In most cases this loop will find something.
            for (SharedDeployment d : depsToCheck) {
                // Load class. Note, that remote node will not load this class.
                // The class will only be loaded on this node.
                Class<?> cls = d.deployedClass(meta.className(), meta.alias());

                if (cls != null) {
                    synchronized (mux) {
                        if (!d.undeployed() && !d.pendingUndeploy()) {
                            if (!addParticipant(d, meta))
                                return null;

                            if (log.isDebugEnabled())
                                log.debug("Acquired deployment after verifying it's availability on " +
                                    "existing nodes [depCls=" + cls + ", dep=" + d + ", meta=" + meta + ']');

                            return d;
                        }
                    }
                }
                else if (log.isDebugEnabled()) {
                    log.debug("Deployment cannot be reused (class does not exist on participating nodes) [dep=" + d +
                        ", meta=" + meta + ']');
                }
            }

            // We are here either because all participant nodes failed
            // or the class indeed should have a separate deployment.
            for (SharedDeployment d : depsToCheck) {
                // We check if any random class from existing deployment can be
                // loaded from sender node. If it can, then we reuse existing
                // deployment.
                if (meta.participants() != null || checkLoadRemoteClass(d.sampleClassName(), meta)) {
                    synchronized (mux) {
                        if (d.undeployed() || d.pendingUndeploy())
                            continue;

                        // Add new node prior to loading the class, so we attempt
                        // to load the class from the latest node.
                        if (!addParticipant(d, meta)) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to add participant to deployment " +
                                    "[meta=" + meta + ", dep=" + dep + ']');

                            return null;
                        }
                    }

                    Class<?> depCls = d.deployedClass(meta.className(), meta.alias());

                    if (depCls == null) {
                        U.error(log, "Failed to peer load class after loading it as a resource [alias=" +
                            meta.alias() + ", dep=" + dep + ']');

                        return null;
                    }

                    if (log.isDebugEnabled())
                        log.debug("Acquired deployment class after verifying other class " +
                            "availability on sender node [depCls=" + depCls + ", rndCls=" + d.sampleClassName() +
                            ", sampleClsName=" + d.sampleClassName() + ", meta=" + meta + ']');

                    return d;
                }
                else if (log.isDebugEnabled())
                    log.debug("Deployment cannot be reused (random class could not be loaded from sender node) [dep=" +
                        d + ", meta=" + meta + ']');
            }

            synchronized (mux) {
                if (log.isDebugEnabled())
                    log.debug("None of the existing class-loaders fits (will try to create a new one): " + meta);

                // Check obsolete request.
                if (isDeadClassLoader(meta))
                    return null;

                // Check that deployment picture has not changed.
                List<SharedDeployment> deps = cache.get(meta.userVersion());

                if (deps != null) {
                    assert !deps.isEmpty();

                    boolean retry = false;

                    for (SharedDeployment d : deps) {
                        // Double check if sender was already added.
                        if (d.hasParticipant(meta.senderNodeId(), meta.classLoaderId())) {
                            dep = d;

                            retry = false;

                            break;
                        }

                        // New deployment was added while outside of synchronization.
                        // Need to recheck it again.
                        if (!d.pendingUndeploy() && !d.undeployed() && !depsToCheck.contains(d))
                            retry = true;
                    }

                    if (retry) {
                        if (log.isDebugEnabled())
                            log.debug("Retrying due to concurrency issues: " + meta);

                        // Outer while loop.
                        continue;
                    }

                    if (dep == null) {
                        // No new deployments were added, so we can safely add ours.
                        dep = createNewDeployment(meta, false);

                        deps.add(dep);

                        if (log.isDebugEnabled())
                            log.debug("Adding new deployment within second check [dep=" + dep + ", meta=" + meta + ']');
                    }
                }
                else {
                    dep = createNewDeployment(meta, true);

                    if (log.isDebugEnabled())
                        log.debug("Created new deployment within second check [dep=" + dep + ", meta=" + meta + ']');
                }
            }

            if (dep != null) {
                // Cache the deployed class.
                Class<?> cls = dep.deployedClass(meta.className(), meta.alias());

                if (cls == null) {
                    U.warn(log, "Failed to load peer class (ignore if class got undeployed during preloading) [alias=" +
                        meta.alias() + ", dep=" + dep + ']');

                    return null;
                }
            }

            return dep;
        }
    }

    /** {@inheritDoc} */
    @Override public void addParticipants(Map<UUID, IgniteUuid> allParticipants,
        Map<UUID, IgniteUuid> addedParticipants) {
        synchronized (mux) {
            for (List<SharedDeployment> deps : cache.values()) {
                for (SharedDeployment dep : deps) {
                    if (dep.undeployed() || dep.pendingUndeploy() || dep.deployMode() == CONTINUOUS)
                        continue;

                    if (hasAnyParticipant(dep, allParticipants)) {
                        for (Map.Entry<UUID, IgniteUuid> added : addedParticipants.entrySet()) {
                            UUID nodeId = added.getKey();

                            if (ctx.discovery().node(nodeId) == null)
                                continue; // For.

                            if (!dep.hasParticipant(nodeId, added.getValue()) &&
                                dep.addParticipant(nodeId, added.getValue())) {
                                if (log.isDebugEnabled())
                                    log.debug("Explicitly added participant [dep=" + dep + ", nodeId=" + nodeId +
                                        ", ldrId=" + added.getValue() + ']');
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Checks if given deployment contains any of given participants.
     *
     * @param dep Deployment to check.
     * @param participants Participants to check.
     * @return {@code True} if deployment contains any of given participants.
     */
    private boolean hasAnyParticipant(SharedDeployment dep, Map<UUID, IgniteUuid> participants) {
        assert Thread.holdsLock(mux);

        for (Map.Entry<UUID, IgniteUuid> entry : participants.entrySet()) {
            if (dep.hasParticipant(entry.getKey(), entry.getValue()))
                return true;
        }

        return false;
    }

    /**
     * Checks that given class can be loaded from remote node specified by sender ID in metadata.
     *
     * @param clsName Class name to try load.
     * @param meta Deployment metadata.
     * @return {@code True} if class do exist on remote node, {@code false} otherwise.
     */
    private boolean checkLoadRemoteClass(String clsName, GridDeploymentMetadata meta) {
        assert clsName != null;
        assert meta != null;
        assert meta.participants() == null;

        // First check resource cache.
        Map<String, Boolean> ldrRsrcCache = rsrcCache.get(meta.classLoaderId());

        if (ldrRsrcCache != null) {
            Boolean res = ldrRsrcCache.get(clsName);

            if (res != null)
                return res;
        }

        // Check dead class loaders.
        if (deadClsLdrs.contains(meta.classLoaderId()))
            return false;

        int lockId = clsName.hashCode() * 31 + meta.classLoaderId().hashCode();

        loadRmtLock.lock(lockId);

        try {
            // Check once more inside lock.
            ldrRsrcCache = rsrcCache.get(meta.classLoaderId());

            if (ldrRsrcCache != null) {
                Boolean res = ldrRsrcCache.get(clsName);

                if (res != null)
                    return res;
            }

            // Check dead class loaders.
            if (deadClsLdrs.contains(meta.classLoaderId()))
                return false;

            // Temporary class loader.
            ClassLoader temp = new GridDeploymentClassLoader(
                IgniteUuid.fromUuid(ctx.localNodeId()),
                meta.userVersion(),
                meta.deploymentMode(),
                true,
                ctx,
                ctx.config().getClassLoader() != null ? ctx.config().getClassLoader() : U.gridClassLoader(),
                meta.classLoaderId(),
                meta.senderNodeId(),
                comm,
                ctx.config().getNetworkTimeout(),
                log,
                ctx.config().getPeerClassLoadingLocalClassPathExclude(),
                0,
                false,
                true);

            String path = U.classNameToResourceName(clsName);

            // Check if specified class can be loaded from sender node.
            InputStream rsrcIn = null;

            try {
                rsrcIn = temp.getResourceAsStream(path);

                boolean found = rsrcIn != null;

                // Add result to resource cache.
                // If missed resource cache is disabled and resource not found
                // we do not save the result (only hits are saved in this case).
                if (found || missedRsrcCacheSize > 0) {
                    if (ldrRsrcCache == null)
                        ldrRsrcCache = F.addIfAbsent(rsrcCache, meta.classLoaderId(),
                            new ConcurrentHashMap8<String, Boolean>());

                    // This is the only place where cache could have been changed,
                    // so we remove only here if classloader have been undeployed
                    // concurrently.
                    if (deadClsLdrs.contains(meta.classLoaderId())) {
                        rsrcCache.remove(meta.classLoaderId());

                        return false;
                    }
                    else
                        // Cache result if classloader is still alive.
                        ldrRsrcCache.put(clsName, found);
                }

                return found;
            }
            finally {
                // We don't need the actual stream.
                U.closeQuiet(rsrcIn);
            }
        }
        finally {
            loadRmtLock.unlock(lockId);
        }
    }

    /**
     * Records all undeployed tasks.
     *
     * @param nodeId Left node ID.
     * @param undeployed Undeployed deployments.
     */
    private void recordUndeployed(@Nullable UUID nodeId, Collection<SharedDeployment> undeployed) {
        if (!F.isEmpty(undeployed))
            for (SharedDeployment d : undeployed)
                d.recordUndeployed(nodeId);
    }

    /**
     * @param meta Request metadata.
     * @return {@code True} if class loader is obsolete.
     */
    private boolean isDeadClassLoader(GridDeploymentMetadata meta) {
        assert Thread.holdsLock(mux);

        if (deadClsLdrs.contains(meta.classLoaderId())) {
            if (log.isDebugEnabled())
                log.debug("Ignoring request for obsolete class loader: " + meta);

            return true;
        }

        return false;
    }

    /**
     * Adds new participant to deployment.
     *
     * @param dep Shared deployment.
     * @param meta Request metadata.
     * @return {@code True} if participant was added.
     */
    private boolean addParticipant(SharedDeployment dep, GridDeploymentMetadata meta) {
        assert dep != null;
        assert meta != null;

        assert Thread.holdsLock(mux);

        if (!checkModeMatch(dep, meta))
            return false;

        if (meta.participants() != null) {
            for (Map.Entry<UUID, IgniteUuid> e : meta.participants().entrySet()) {
                if (ctx.discovery().node(e.getKey()) != null) {
                    dep.addParticipant(e.getKey(), e.getValue());

                    if (log.isDebugEnabled())
                        log.debug("Added new participant [nodeId=" + e.getKey() + ", clsLdrId=" + e.getValue() +
                            ", seqNum=" + e.getValue().localId() + ']');
                }
                else if (log.isDebugEnabled())
                    log.debug("Skipped participant (node left?) [nodeId=" + e.getKey() +
                        ", clsLdrId=" + e.getValue() + ", seqNum=" + e.getValue().localId() + ']');
            }
        }

        if (dep.deployMode() == CONTINUOUS || meta.participants() == null) {
            if (!dep.addParticipant(meta.senderNodeId(), meta.classLoaderId())) {
                U.warn(log, "Failed to create shared mode deployment " +
                    "(requested class loader was already undeployed, did sender node leave grid?) " +
                    "[clsLdrId=" + meta.classLoaderId() + ", senderNodeId=" + meta.senderNodeId() + ']');

                return false;
            }

            if (log.isDebugEnabled())
                log.debug("Added new participant [nodeId=" + meta.senderNodeId() + ", clsLdrId=" +
                    meta.classLoaderId() + ", seqNum=" + meta.sequenceNumber() + ']');
        }

        return true;
    }

    /**
     * Checks if deployment modes match.
     *
     * @param dep Shared deployment.
     * @param meta Request metadata.
     * @return {@code True} if shared deployment modes match.
     */
    private boolean checkModeMatch(GridDeploymentInfo dep, GridDeploymentMetadata meta) {
        if (dep.deployMode() != meta.deploymentMode()) {
            U.warn(log, "Received invalid deployment mode (will not deploy, make sure that all nodes " +
                "executing the same classes in shared mode have identical GridDeploymentMode parameter) [mode=" +
                meta.deploymentMode() + ", expected=" + dep.deployMode() + ']');

            return false;
        }

        return true;
    }

    /**
     * Removes obsolete deployments in case of redeploy.
     *
     * @param meta Request metadata.
     */
    private void checkRedeploy(GridDeploymentMetadata meta) {
        assert Thread.holdsLock(mux);

        for (List<SharedDeployment> deps : cache.values()) {
            for (SharedDeployment dep : deps) {
                if (!dep.undeployed() && !dep.pendingUndeploy()) {
                    long undeployTimeout = ctx.config().getNetworkTimeout();

                    // Only check deployments with no participants.
                    if (!dep.hasParticipants() &&
                        dep.deployMode() == CONTINUOUS &&
                        dep.existingDeployedClass(meta.className()) != null &&
                        !meta.userVersion().equals(dep.userVersion())) {
                        // In case of SHARED deployment it is possible to get hear if
                        // unmarshalling happens during undeploy. In this case, we
                        // simply don't do anything.
                        // Change from shared deploy to shared undeploy or user version change.
                        // Simply remove all deployments with no participating nodes.
                        dep.onUndeployScheduled();

                        if (log.isDebugEnabled())
                            log.debug("Deployment was scheduled for undeploy: " + dep);

                        // Lifespan time.
                        final long endTime = U.currentTimeMillis() + undeployTimeout;

                        // Deployment to undeploy.
                        final SharedDeployment undep = dep;

                        if (endTime > 0) {
                            ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                                @Override public IgniteUuid timeoutId() {
                                    return undep.classLoaderId();
                                }

                                @Override public long endTime() {
                                    return endTime < 0 ? Long.MAX_VALUE : endTime;
                                }

                                @Override public void onTimeout() {
                                    boolean rmv = false;

                                    // Hot redeployment.
                                    synchronized (mux) {
                                        assert undep.pendingUndeploy();

                                        if (!undep.undeployed()) {
                                            undep.undeploy();

                                            undep.onRemoved();

                                            rmv = true;

                                            Collection<SharedDeployment> deps = cache.get(undep.userVersion());

                                            if (deps != null) {
                                                for (Iterator<SharedDeployment> i = deps.iterator(); i.hasNext();)
                                                    if (i.next() == undep)
                                                        i.remove();

                                                if (deps.isEmpty())
                                                    cache.remove(undep.userVersion());
                                            }

                                            if (log.isInfoEnabled())
                                                log.info("Undeployed class loader due to deployment mode change, " +
                                                    "user version change, or hot redeployment: " + undep);
                                        }
                                    }

                                    // Outside synchronization.
                                    if (rmv)
                                        undep.recordUndeployed(null);
                                }
                            });
                        }
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void explicitUndeploy(UUID nodeId, String rsrcName) {
        Collection<SharedDeployment> undeployed = new LinkedList<>();

        synchronized (mux) {
            for (Iterator<List<SharedDeployment>> i1 = cache.values().iterator(); i1.hasNext();) {
                List<SharedDeployment> deps = i1.next();

                for (Iterator<SharedDeployment> i2 = deps.iterator(); i2.hasNext();) {
                    SharedDeployment dep = i2.next();


                    if (dep.hasName(rsrcName)) {
                        if (!dep.undeployed()) {
                            dep.undeploy();

                            dep.onRemoved();

                            // Undeploy.
                            i2.remove();

                            undeployed.add(dep);

                            if (log.isInfoEnabled())
                                log.info("Undeployed per-version class loader: " + dep);
                        }

                        break;
                    }
                }

                if (deps.isEmpty())
                    i1.remove();
            }
        }

        recordUndeployed(null, undeployed);
    }

    /**
     * Creates and caches new deployment.
     *
     * @param meta Deployment metadata.
     * @param isCache Whether or not to cache.
     * @return New deployment.
     */
    private SharedDeployment createNewDeployment(GridDeploymentMetadata meta, boolean isCache) {
        assert Thread.holdsLock(mux);

        assert meta.parentLoader() == null;

        IgniteUuid ldrId = IgniteUuid.fromUuid(ctx.localNodeId());

        GridDeploymentClassLoader clsLdr;

        if (meta.deploymentMode() == CONTINUOUS || meta.participants() == null) {
            // Create peer class loader.
            // Note that we are passing empty list for local P2P exclude, as it really
            // does not make sense with shared deployment.
            clsLdr = new GridDeploymentClassLoader(
                ldrId,
                meta.userVersion(),
                meta.deploymentMode(),
                false,
                ctx,
                ctx.config().getClassLoader() != null ? ctx.config().getClassLoader() : U.gridClassLoader(),
                meta.classLoaderId(),
                meta.senderNodeId(),
                comm,
                ctx.config().getNetworkTimeout(),
                log,
                ctx.config().getPeerClassLoadingLocalClassPathExclude(),
                ctx.config().getPeerClassLoadingMissedResourcesCacheSize(),
                meta.deploymentMode() == CONTINUOUS /* enable class byte cache in CONTINUOUS mode */,
                false);

            if (meta.participants() != null)
                for (Map.Entry<UUID, IgniteUuid> e : meta.participants().entrySet())
                    clsLdr.register(e.getKey(), e.getValue());

            if (log.isDebugEnabled())
                log.debug("Created class loader in CONTINUOUS mode or without participants " +
                    "[ldr=" + clsLdr + ", meta=" + meta + ']');
        }
        else {
            assert meta.deploymentMode() == SHARED;

            // Create peer class loader.
            // Note that we are passing empty list for local P2P exclude, as it really
            // does not make sense with shared deployment.
            clsLdr = new GridDeploymentClassLoader(
                ldrId,
                meta.userVersion(),
                meta.deploymentMode(),
                false,
                ctx,
                U.gridClassLoader(),
                meta.participants(),
                comm,
                ctx.config().getNetworkTimeout(),
                log,
                ctx.config().getPeerClassLoadingLocalClassPathExclude(),
                ctx.config().getPeerClassLoadingMissedResourcesCacheSize(),
                false,
                false);

            if (log.isDebugEnabled())
                log.debug("Created classloader in SHARED mode with participants " +
                    "[ldr=" + clsLdr + ", meta=" + meta + ']');
        }

        // Give this deployment a unique class loader to emphasize that this
        // ID is unique to this shared deployment and is not ID of loader on
        // sender node.
        SharedDeployment dep = new SharedDeployment(meta.deploymentMode(), clsLdr, ldrId,
            meta.userVersion(), meta.alias());

        if (log.isDebugEnabled())
            log.debug("Created new deployment: " + dep);

        if (isCache) {
            List<SharedDeployment> deps = F.addIfAbsent(cache, meta.userVersion(), new LinkedList<SharedDeployment>());

            assert deps != null;

            deps.add(dep);

            if (log.isDebugEnabled())
                log.debug("Added deployment to cache: " + cache);
        }

        return dep;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentPerVersionStore.class, this);
    }

    /**
     *
     */
    private class SharedDeployment extends GridDeployment {
        /** Flag indicating whether this deployment was removed from cache. */
        private boolean rmv;

        /**
         * @param depMode Deployment mode.
         * @param clsLdr Class loader.
         * @param clsLdrId Class loader ID.
         * @param userVer User version.
         * @param sampleClsName Sample class name.
         */
        @SuppressWarnings({"TypeMayBeWeakened"})
        SharedDeployment(DeploymentMode depMode,
            GridDeploymentClassLoader clsLdr, IgniteUuid clsLdrId,
            String userVer, String sampleClsName) {
            super(depMode, clsLdr, clsLdrId, userVer, sampleClsName, false);
        }

        /** {@inheritDoc} */
        @Override public GridDeploymentClassLoader classLoader() {
            return (GridDeploymentClassLoader)super.classLoader();
        }

        /**
         * @param nodeId Grid node ID.
         * @param ldrId Class loader ID.
         * @return Whether actually added or not.
         */
        boolean addParticipant(UUID nodeId, IgniteUuid ldrId) {
            assert nodeId != null;
            assert ldrId != null;

            assert Thread.holdsLock(mux);

            if (!deadClsLdrs.contains(ldrId)) {
                classLoader().register(nodeId, ldrId);

                return true;
            }

            return false;
        }

        /**
         * @param nodeId Node ID to remove.
         */
        void removeParticipant(UUID nodeId) {
            assert nodeId != null;

            assert Thread.holdsLock(mux);

            IgniteUuid ldrId = classLoader().unregister(nodeId);

            if (log.isDebugEnabled())
                log.debug("Registering dead class loader ID: " + ldrId);

            if (ldrId == null)
                return;

            deadClsLdrs.add(ldrId);

            // Remove hits/misses from resource cache as well.
            rsrcCache.remove(ldrId);
        }

        /**
         * @return Set of participating nodes.
         */
        Collection<UUID> getParticipantNodeIds() {
            assert Thread.holdsLock(mux);

            return classLoader().registeredNodeIds();
        }

        /**
         * @param nodeId Node ID.
         * @return Class loader ID for node ID.
         */
        IgniteUuid getClassLoaderId(UUID nodeId) {
            assert nodeId != null;

            assert Thread.holdsLock(mux);

            return classLoader().registeredClassLoaderId(nodeId);
        }

        /**
         * @return Registered class loader IDs.
         */
        Collection<IgniteUuid> getClassLoaderIds() {
            assert Thread.holdsLock(mux);

            return classLoader().registeredClassLoaderIds();
        }


        /**
         * @return {@code True} if deployment has any node participants.
         */
        boolean hasParticipants() {
            assert Thread.holdsLock(mux);

            return classLoader().hasRegisteredNodes();
        }

        /**
         * Checks if node is participating in deployment.
         *
         * @param nodeId Node ID to check.
         * @param ldrId Class loader ID.
         * @return {@code True} if node is participating in deployment.
         */
        boolean hasParticipant(UUID nodeId, IgniteUuid ldrId) {
            assert nodeId != null;
            assert ldrId != null;

            assert Thread.holdsLock(mux);

            return classLoader().hasRegisteredNode(nodeId, ldrId);
        }

        /**
         * Gets property removed.
         *
         * @return Property removed.
         */
        boolean isRemoved() {
            assert Thread.holdsLock(mux);

            return rmv;
        }

        /**
         * Sets property removed.
         */
        void onRemoved() {
            assert Thread.holdsLock(mux);

            rmv = true;

            Collection<IgniteUuid> deadIds = classLoader().registeredClassLoaderIds();

            if (log.isDebugEnabled())
                log.debug("Registering dead class loader IDs: " + deadIds);

            deadClsLdrs.addAll(deadIds);

            // Remove hits/misses from resource cache as well.
            for (IgniteUuid clsLdrId : deadIds)
                rsrcCache.remove(clsLdrId);
        }

        /** {@inheritDoc} */
        @Override public void onDeployed(Class<?> cls) {
            assert !Thread.holdsLock(mux);

            boolean isTask = isTask(cls);

            String msg = (isTask ? "Task" : "Class") + " was deployed in SHARED or CONTINUOUS mode: " + cls;

            int type = isTask ? EVT_TASK_DEPLOYED : EVT_CLASS_DEPLOYED;

            if (ctx.event().isRecordable(type)) {
                DeploymentEvent evt = new DeploymentEvent();

                evt.node(ctx.discovery().localNode());
                evt.message(msg);
                evt.type(type);
                evt.alias(cls.getName());

                ctx.event().record(evt);
            }

            if (log.isInfoEnabled())
                log.info(msg);
        }

        /**
         * Called to record all undeployed classes..
         *
         * @param leftNodeId Left node ID.
         */
        void recordUndeployed(@Nullable UUID leftNodeId) {
            assert !Thread.holdsLock(mux);

            for (Map.Entry<String, Class<?>> depCls : deployedClassMap().entrySet()) {
                boolean isTask = isTask(depCls.getValue());

                String msg = (isTask ? "Task" : "Class") + " was undeployed in SHARED or CONTINUOUS mode " +
                    "[cls=" + depCls.getValue() + ", alias=" + depCls.getKey() + ']';

                int type = isTask ? EVT_TASK_UNDEPLOYED : EVT_CLASS_UNDEPLOYED;

                if (ctx.event().isRecordable(type)) {
                    DeploymentEvent evt = new DeploymentEvent();

                    evt.node(ctx.discovery().localNode());
                    evt.message(msg);
                    evt.type(type);
                    evt.alias(depCls.getKey());

                    ctx.event().record(evt);
                }

                if (log.isInfoEnabled())
                    log.info(msg);
            }

            if (obsolete()) {
                // Resource cleanup.
                ctx.resource().onUndeployed(this);

                ClassLoader ldr = classLoader();

                ctx.cache().onUndeployed(ldr);

                // Clear optimized marshaller's cache.
                if (ctx.config().getMarshaller() instanceof OptimizedMarshaller)
                    ((OptimizedMarshaller)ctx.config().getMarshaller()).onUndeploy(ldr);

                clearSerializationCaches();

                // Class loader cache should be cleared in the last order.
                GridAnnotationsCache.onUndeployed(ldr);
                GridClassLoaderCache.onUndeployed(ldr);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SharedDeployment.class, this, "super", super.toString());
        }
    }
}