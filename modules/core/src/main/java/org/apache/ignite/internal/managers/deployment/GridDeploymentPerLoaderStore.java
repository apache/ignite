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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.events.DeploymentEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.GridAnnotationsCache;
import org.apache.ignite.internal.util.GridClassLoaderCache;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.deployment.DeploymentSpi;

import static org.apache.ignite.events.EventType.EVT_CLASS_DEPLOYED;
import static org.apache.ignite.events.EventType.EVT_CLASS_UNDEPLOYED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_TASK_DEPLOYED;
import static org.apache.ignite.events.EventType.EVT_TASK_UNDEPLOYED;

/**
 * Deployment storage for {@link org.apache.ignite.configuration.DeploymentMode#PRIVATE} and
 * {@link org.apache.ignite.configuration.DeploymentMode#ISOLATED} modes.
 */
public class GridDeploymentPerLoaderStore extends GridDeploymentStoreAdapter {
    /** Cache keyed by class loader ID. */
    private Map<IgniteUuid, IsolatedDeployment> cache = new HashMap<>();

    /** Discovery listener. */
    private GridLocalEventListener discoLsnr;

    /** Context class loader. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private ClassLoader ctxLdr;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * @param spi Underlying SPI.
     * @param ctx Grid kernal context.
     * @param comm Deployment communication.
     */
    GridDeploymentPerLoaderStore(DeploymentSpi spi, GridKernalContext ctx, GridDeploymentCommunication comm) {
        super(spi, ctx, comm);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctxLdr = U.detectClassLoader(getClass());

        discoLsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt instanceof DiscoveryEvent;

                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                if (evt.type() == EVT_NODE_LEFT ||
                    evt.type() == EVT_NODE_FAILED) {
                    Collection<IsolatedDeployment> rmv = new LinkedList<>();

                    synchronized (mux) {
                        for (Iterator<IsolatedDeployment> iter = cache.values().iterator(); iter.hasNext();) {
                            IsolatedDeployment dep = iter.next();

                            if (dep.senderNodeId().equals(nodeId)) {
                                dep.undeploy();

                                iter.remove();

                                rmv.add(dep);
                            }
                        }
                    }

                    for (IsolatedDeployment dep : rmv)
                        dep.recordUndeployed();
                }
            }
        };

        ctx.event().addLocalEventListener(discoLsnr,
            EVT_NODE_FAILED,
            EVT_NODE_LEFT
        );

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        Collection<IsolatedDeployment> cp = new HashSet<>();

        synchronized (mux) {
            for (IsolatedDeployment dep : cache.values()) {
                // Mark undeployed. This way if any event hits after stop,
                // undeployment won't happen twice.
                dep.undeploy();

                cp.add(dep);
            }

            cache.clear();
        }

        for (IsolatedDeployment dep : cp)
            dep.recordUndeployed();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        Collection<IsolatedDeployment> rmv = new LinkedList<>();

        // Check existing deployments for presence of obsolete nodes.
        synchronized (mux) {
            for (Iterator<IsolatedDeployment> iter = cache.values().iterator(); iter.hasNext();) {
                IsolatedDeployment dep = iter.next();

                ClusterNode node = ctx.discovery().node(dep.senderNodeId());

                if (node == null) {
                    dep.undeploy();

                    iter.remove();

                    rmv.add(dep);
                }
            }
        }

        for (IsolatedDeployment dep : rmv)
            dep.recordUndeployed();

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
        synchronized (mux) {
            return new LinkedList<GridDeployment>(cache.values());
        }
    }

    /** {@inheritDoc} */
    @Override public GridDeployment getDeployment(IgniteUuid ldrId) {
        synchronized (mux) {
            return cache.get(ldrId);
        }
    }

    /** {@inheritDoc} */
    @Override public GridDeployment getDeployment(GridDeploymentMetadata meta) {
        assert meta != null;

        assert ctx.config().isPeerClassLoadingEnabled();

        // Validate metadata.
        assert meta.classLoaderId() != null;
        assert meta.senderNodeId() != null;
        assert meta.sequenceNumber() > 0 : "Invalid sequence number (must be positive): " + meta;

        if (log.isDebugEnabled())
            log.debug("Starting to peer-load class based on deployment metadata: " + meta);

        ClusterNode snd = ctx.discovery().node(meta.senderNodeId());

        if (snd == null) {
            U.warn(log, "Failed to create Private or Isolated mode deployment (sender node left grid): " + snd);

            return null;
        }

        IsolatedDeployment dep;

        synchronized (mux) {
            dep = cache.get(meta.classLoaderId());

            if (dep == null) {
                long undeployTimeout = 0;

                // If could not find deployment, make sure to perform clean up.
                // Check if any deployments must be undeployed.
                for (IsolatedDeployment d : cache.values()) {
                    if (d.senderNodeId().equals(meta.senderNodeId()) &&
                        !d.undeployed() && !d.pendingUndeploy()) {
                        if (d.sequenceNumber() < meta.sequenceNumber()) {
                            // Undeploy previous class deployments.
                            if (d.existingDeployedClass(meta.className()) != null) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Received request for a class with newer sequence number " +
                                        "(will schedule current class for undeployment) [cls=" +
                                        meta.className() + ", newSeq=" +
                                        meta.sequenceNumber() + ", oldSeq=" + d.sequenceNumber() +
                                        ", senderNodeId=" + meta.senderNodeId() + ", curClsLdrId=" +
                                        d.classLoaderId() + ", newClsLdrId=" +
                                        meta.classLoaderId() + ']');
                                }

                                scheduleUndeploy(d, ctx.config().getNetworkTimeout());
                            }
                        }
                        // If we received execution request even after we waited for P2P
                        // timeout period, we simply ignore it.
                        else if (d.sequenceNumber() > meta.sequenceNumber()) {
                            if (d.deployedClass(meta.className()) != null) {
                                long time = U.currentTimeMillis() - d.timestamp();

                                if (time < ctx.config().getNetworkTimeout()) {
                                    // Set undeployTimeout, so the class will be scheduled
                                    // for undeployment.
                                    undeployTimeout = ctx.config().getNetworkTimeout() - time;

                                    if (log.isDebugEnabled()) {
                                        log.debug("Received execution request for a stale class (will deploy and " +
                                            "schedule undeployment in " + undeployTimeout + "ms) " +
                                            "[cls=" + meta.className() + ", curSeq=" + d.sequenceNumber() +
                                            ", rcvdSeq=" + meta.sequenceNumber() + ", senderNodeId=" +
                                            meta.senderNodeId() + ", curClsLdrId=" + d.classLoaderId() +
                                            ", rcvdClsLdrId=" + meta.classLoaderId() + ']');
                                    }
                                }
                                else {
                                    U.warn(log, "Received execution request for a class that has been redeployed " +
                                        "(will ignore): " + meta.alias());

                                    return null;
                                }
                            }
                        }
                        else {
                            U.error(log, "Sequence number does not correspond to class loader ID [seqNum=" +
                                meta.sequenceNumber() + ", dep=" + d + ']');

                            return null;
                        }
                    }
                }

                ClassLoader parent = meta.parentLoader() == null ? ctxLdr : meta.parentLoader();

                // Safety.
                if (parent == null)
                    parent = getClass().getClassLoader();

                // Create peer class loader.
                ClassLoader clsLdr = new GridDeploymentClassLoader(
                    meta.classLoaderId(),
                    meta.userVersion(),
                    meta.deploymentMode(),
                    true,
                    ctx,
                    parent,
                    meta.classLoaderId(),
                    meta.senderNodeId(),
                    comm,
                    ctx.config().getNetworkTimeout(),
                    log,
                    ctx.config().getPeerClassLoadingLocalClassPathExclude(),
                    ctx.config().getPeerClassLoadingMissedResourcesCacheSize(),
                    false,
                    false);

                dep = new IsolatedDeployment(meta.deploymentMode(), clsLdr, meta.classLoaderId(),
                    meta.userVersion(), snd, meta.className());

                cache.put(meta.classLoaderId(), dep);

                // In case if deploying stale class.
                if (undeployTimeout > 0)
                    scheduleUndeploy(dep, undeployTimeout);
            }
        }

        // Make sure that requested class is loaded and cached.
        if (dep != null) {
            Class<?> cls = dep.deployedClass(meta.className(), meta.alias());

            if (cls == null) {
                U.warn(log, "Failed to load peer class [alias=" + meta.alias() + ", dep=" + dep + ']');

                return null;
            }
        }

        return dep;
    }

    /** {@inheritDoc} */
    @Override public void addParticipants(Map<UUID, IgniteUuid> allParticipants,
        Map<UUID, IgniteUuid> addedParticipants) {
        assert false;
    }

    /**
     * Schedules existing deployment for future undeployment.
     *
     * @param dep Deployment.
     * @param timeout Timeout for undeployment to occur.
     */
    private void scheduleUndeploy(final IsolatedDeployment dep, long timeout) {
        assert Thread.holdsLock(mux);

        if (!dep.undeployed() && !dep.pendingUndeploy()) {
            dep.onUndeployScheduled();

            ctx.timeout().addTimeoutObject(new GridTimeoutObjectAdapter(dep.classLoaderId(), timeout) {
                @Override public void onTimeout() {
                    boolean rmv = false;

                    // Hot redeployment.
                    synchronized (mux) {
                        if (!dep.undeployed()) {
                            dep.undeploy();

                            cache.remove(dep.classLoaderId());

                            rmv = true;
                        }
                    }

                    if (rmv)
                        dep.recordUndeployed();
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void explicitUndeploy(UUID nodeId, String rsrcName) {
        assert nodeId != null;
        assert rsrcName != null;

        Collection<IsolatedDeployment> undeployed = new LinkedList<>();

        synchronized (mux) {
            for (Iterator<IsolatedDeployment> iter = cache.values().iterator(); iter.hasNext();) {
                IsolatedDeployment dep = iter.next();

                if (dep.senderNodeId().equals(nodeId)) {
                    if (dep.hasName(rsrcName)) {
                        iter.remove();

                        dep.undeploy();

                        undeployed.add(dep);

                        if (log.isInfoEnabled())
                            log.info("Undeployed Private or Isolated deployment: " + dep);
                    }
                }
            }
        }

        for (IsolatedDeployment dep : undeployed)
            dep.recordUndeployed();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentPerLoaderStore.class, this);
    }

    /**
     *
     */
    private class IsolatedDeployment extends GridDeployment {
        /** Sender node ID. */
        private final ClusterNode sndNode;

        /**
         * @param depMode Deployment mode.
         * @param clsLdr Class loader.
         * @param clsLdrId Class loader ID.
         * @param userVer User version.
         * @param sndNode Sender node.
         * @param sampleClsName Sample class name.
         */
        IsolatedDeployment(DeploymentMode depMode, ClassLoader clsLdr, IgniteUuid clsLdrId,
            String userVer, ClusterNode sndNode, String sampleClsName) {
            super(depMode, clsLdr, clsLdrId, userVer, sampleClsName, false);

            this.sndNode = sndNode;
        }

        /**
         * Gets property senderNodeId.
         *
         * @return Property senderNodeId.
         */
        UUID senderNodeId() {
            return sndNode.id();
        }

        /** {@inheritDoc} */
        @Override public void onDeployed(Class<?> cls) {
            recordDeployed(cls, true);
        }

        /**
         * Called for every deployed class.
         *
         * @param cls Deployed class.
         * @param recordEvt Flag indicating whether to record events.
         */
        void recordDeployed(Class<?> cls, boolean recordEvt) {
            assert !Thread.holdsLock(mux);

            boolean isTask = isTask(cls);

            String msg = (isTask ? "Task" : "Class") + " was deployed in Private or Isolated mode: " + cls;

            if (recordEvt && ctx.event().isRecordable(isTask(cls) ? EVT_TASK_DEPLOYED : EVT_CLASS_DEPLOYED)) {
                DeploymentEvent evt = new DeploymentEvent();

                // Record task event.
                evt.type(isTask ? EVT_TASK_DEPLOYED : EVT_CLASS_DEPLOYED);
                evt.node(sndNode);
                evt.message(msg);
                evt.alias(cls.getName());

                ctx.event().record(evt);
            }

            if (log.isInfoEnabled())
                log.info(msg);
        }

        /**
         * Called to record all undeployed classes.
         */
        void recordUndeployed() {
            assert !Thread.holdsLock(mux);

            GridEventStorageManager evts = ctx.event();

            if (evts.isRecordable(EVT_CLASS_UNDEPLOYED) || evts.isRecordable(EVT_TASK_UNDEPLOYED)) {
                for (Map.Entry<String, Class<?>> depCls : deployedClassMap().entrySet()) {
                    boolean isTask = isTask(depCls.getValue());

                    String msg = (isTask ? "Task" : "Class") + " was undeployed in Private or Isolated mode " +
                        "[cls=" + depCls.getValue() + ", alias=" + depCls.getKey() + ']';

                    if (evts.isRecordable(!isTask ? EVT_CLASS_UNDEPLOYED : EVT_TASK_UNDEPLOYED)) {
                        DeploymentEvent evt = new DeploymentEvent();

                        evt.node(sndNode);
                        evt.message(msg);
                        evt.type(!isTask ? EVT_CLASS_UNDEPLOYED : EVT_TASK_UNDEPLOYED);
                        evt.alias(depCls.getKey());

                        evts.record(evt);
                    }

                    if (log.isInfoEnabled())
                        log.info(msg);
                }
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
            return S.toString(IsolatedDeployment.class, this, super.toString());
        }
    }
}