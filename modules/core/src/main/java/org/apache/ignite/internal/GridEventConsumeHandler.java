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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheDeploymentManager;
import org.apache.ignite.internal.processors.continuous.GridContinuousBatch;
import org.apache.ignite.internal.processors.continuous.GridContinuousBatchAdapter;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.platform.PlatformEventFilterListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVTS_ALL;

/**
 * Continuous routine handler for remote event listening.
 */
class GridEventConsumeHandler implements GridContinuousHandler {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default callback. */
    private static final IgniteBiPredicate<UUID,Event> DFLT_CALLBACK = new P2<UUID, Event>() {
        @Override public boolean apply(UUID uuid, Event e) {
            return true;
        }
    };

    /** Local callback. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private IgniteBiPredicate<UUID, Event> cb;

    /** Filter. */
    private IgnitePredicate<Event> filter;

    /** Serialized filter. */
    private byte[] filterBytes;

    /** Deployment class name. */
    private String clsName;

    /** Deployment info. */
    private GridDeploymentInfo depInfo;

    /** Types. */
    private int[] types;

    /** Listener. */
    private GridLocalEventListener lsnr;

    /**
     * Required by {@link Externalizable}.
     */
    public GridEventConsumeHandler() {
        // No-op.
    }

    /**
     * @param cb Local callback.
     * @param filter Filter.
     * @param types Types.
     */
    GridEventConsumeHandler(@Nullable IgniteBiPredicate<UUID, Event> cb, @Nullable IgnitePredicate<Event> filter,
        @Nullable int[] types) {
        this.cb = cb == null ? DFLT_CALLBACK : cb;
        this.filter = filter;
        this.types = types;
    }

    /** {@inheritDoc} */
    @Override public boolean isEvents() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isMessaging() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuery() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void updateCounters(AffinityTopologyVersion topVer, Map<UUID, Map<Integer, Long>> cntrsPerNode,
        Map<Integer, Long> cntrs) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public RegisterStatus register(final UUID nodeId, final UUID routineId, final GridKernalContext ctx)
        throws IgniteCheckedException {
        assert nodeId != null;
        assert routineId != null;
        assert ctx != null;

        if (cb != null)
            ctx.resource().injectGeneric(cb);

        if (filter != null)
            ctx.resource().injectGeneric(filter);

        if (filter instanceof PlatformEventFilterListener)
            ((PlatformEventFilterListener)filter).initialize(ctx);

        final boolean loc = nodeId.equals(ctx.localNodeId());

        lsnr = new GridLocalEventListener() {
            /** node ID, routine ID, event */
            private final Queue<T3<UUID, UUID, Event>> notificationQueue = new LinkedList<>();

            private boolean notificationInProgress;

            @Override public void onEvent(Event evt) {
                if (filter != null && !filter.apply(evt))
                    return;

                if (loc) {
                    if (!cb.apply(nodeId, evt))
                        ctx.continuous().stopRoutine(routineId);
                }
                else {
                    if (ctx.discovery().node(nodeId) == null)
                        return;

                    synchronized (notificationQueue) {
                        notificationQueue.add(new T3<>(nodeId, routineId, evt));

                        if (!notificationInProgress) {
                            ctx.getSystemExecutorService().execute(new Runnable() {
                                @Override public void run() {
                                    if (!ctx.continuous().lockStopping())
                                        return;

                                    try {
                                        while (true) {
                                            T3<UUID, UUID, Event> t3;

                                            synchronized (notificationQueue) {
                                                t3 = notificationQueue.poll();

                                                if (t3 == null) {
                                                    notificationInProgress = false;

                                                    return;
                                                }
                                            }

                                            try {
                                                Event evt = t3.get3();

                                                EventWrapper wrapper = new EventWrapper(evt);

                                                if (evt instanceof CacheEvent) {
                                                    String cacheName = ((CacheEvent)evt).cacheName();

                                                    ClusterNode node = ctx.discovery().node(t3.get1());

                                                    if (node == null)
                                                        continue;

                                                    if (ctx.config().isPeerClassLoadingEnabled()) {
                                                        GridCacheContext cctx =
                                                            ctx.cache().internalCache(cacheName).context();

                                                        if (cctx.deploymentEnabled() &&
                                                            ctx.discovery().cacheNode(node, cacheName)) {
                                                            wrapper.p2pMarshal(ctx.config().getMarshaller());

                                                            wrapper.cacheName = cacheName;

                                                            cctx.deploy().prepare(wrapper);
                                                        }
                                                    }
                                                }

                                                ctx.continuous().addNotification(t3.get1(), t3.get2(), wrapper, null,
                                                    false, false);
                                            }
                                            catch (ClusterTopologyCheckedException ignored) {
                                                // No-op.
                                            }
                                            catch (Throwable e) {
                                                U.error(ctx.log(GridEventConsumeHandler.class),
                                                    "Failed to send event notification to node: " + nodeId, e);
                                            }
                                        }
                                    }
                                    finally {
                                        ctx.continuous().unlockStopping();
                                    }
                                }
                            });

                            notificationInProgress = true;
                        }
                    }
                }
            }
        };

        if (F.isEmpty(types))
            types = EVTS_ALL;

        ctx.event().addLocalEventListener(lsnr, types);

        return RegisterStatus.REGISTERED;
    }

    /** {@inheritDoc} */
    @Override public void unregister(UUID routineId, GridKernalContext ctx) {
        assert routineId != null;
        assert ctx != null;

        if (lsnr != null)
            ctx.event().removeLocalEventListener(lsnr, types);

        RuntimeException err = null;

        try {
            if (filter instanceof PlatformEventFilterListener)
                ((PlatformEventFilterListener)filter).onClose();
        }
        catch(RuntimeException ex) {
            err = ex;
        }

        try {
            if (cb instanceof PlatformEventFilterListener)
                ((PlatformEventFilterListener)cb).onClose();
        }
        catch (RuntimeException ex) {
            if (err == null)
                err = ex;
        }

        if (err != null)
            throw err;
    }

    /**
     * @param nodeId Node ID.
     * @param objs Notification objects.
     */
    @Override public void notifyCallback(UUID nodeId, UUID routineId, Collection<?> objs, GridKernalContext ctx) {
        assert nodeId != null;
        assert routineId != null;
        assert objs != null;
        assert ctx != null;

        for (Object obj : objs) {
            assert obj instanceof EventWrapper;

            EventWrapper wrapper = (EventWrapper)obj;

            if (wrapper.bytes != null) {
                assert ctx.config().isPeerClassLoadingEnabled();

                GridCacheAdapter cache = ctx.cache().internalCache(wrapper.cacheName);

                ClassLoader ldr = null;

                if (cache != null) {
                    GridCacheDeploymentManager depMgr = cache.context().deploy();

                    GridDeploymentInfo depInfo = wrapper.depInfo;

                    if (depInfo != null) {
                        depMgr.p2pContext(nodeId, depInfo.classLoaderId(), depInfo.userVersion(), depInfo.deployMode(),
                            depInfo.participants(), depInfo.localDeploymentOwner());
                    }

                    ldr = depMgr.globalLoader();
                }
                else {
                    U.warn(ctx.log(getClass()), "Received cache event for cache that is not configured locally " +
                        "when peer class loading is enabled: " + wrapper.cacheName + ". Will try to unmarshal " +
                        "with default class loader.");
                }

                try {
                    wrapper.p2pUnmarshal(ctx.config().getMarshaller(), U.resolveClassLoader(ldr, ctx.config()));
                }
                catch (IgniteCheckedException e) {
                    U.error(ctx.log(getClass()), "Failed to unmarshal event.", e);
                }
            }

            if (!cb.apply(nodeId, wrapper.evt)) {
                ctx.continuous().stopRoutine(routineId);

                break;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;
        assert ctx.config().isPeerClassLoadingEnabled();

        if (filter != null) {
            Class cls = U.detectClass(filter);

            clsName = cls.getName();

            GridDeployment dep = ctx.deploy().deploy(cls, U.detectClassLoader(cls));

            if (dep == null)
                throw new IgniteDeploymentCheckedException("Failed to deploy event filter: " + filter);

            depInfo = new GridDeploymentInfoBean(dep);

            filterBytes = U.marshal(ctx.config().getMarshaller(), filter);
        }
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(UUID nodeId, GridKernalContext ctx) throws IgniteCheckedException {
        assert nodeId != null;
        assert ctx != null;
        assert ctx.config().isPeerClassLoadingEnabled();

        if (filterBytes != null) {
            GridDeployment dep = ctx.deploy().getGlobalDeployment(depInfo.deployMode(), clsName, clsName,
                depInfo.userVersion(), nodeId, depInfo.classLoaderId(), depInfo.participants(), null);

            if (dep == null)
                throw new IgniteDeploymentCheckedException("Failed to obtain deployment for class: " + clsName);

            filter = U.unmarshal(ctx, filterBytes, U.resolveClassLoader(dep.classLoader(), ctx.config()));
        }
    }

    /** {@inheritDoc} */
    @Override public GridContinuousBatch createBatch() {
        return new GridContinuousBatchAdapter();
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnected() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onBatchAcknowledged(UUID routineId, GridContinuousBatch batch, GridKernalContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onNodeLeft() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object orderedTopic() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridContinuousHandler clone() {
        try {
            return (GridContinuousHandler)super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        boolean b = filterBytes != null;

        out.writeBoolean(b);

        if (b) {
            U.writeByteArray(out, filterBytes);
            U.writeString(out, clsName);
            out.writeObject(depInfo);
        }
        else
            out.writeObject(filter);

        out.writeObject(types);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean b = in.readBoolean();

        if (b) {
            filterBytes = U.readByteArray(in);
            clsName = U.readString(in);
            depInfo = (GridDeploymentInfo)in.readObject();
        }
        else
            filter = (IgnitePredicate<Event>)in.readObject();

        types = (int[])in.readObject();
    }

    /**
     * Event wrapper.
     */
    private static class EventWrapper implements GridCacheDeployable, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Event. */
        private Event evt;

        /** Serialized event. */
        private byte[] bytes;

        /** Cache name (for cache events only). */
        private String cacheName;

        /** Deployment info. */
        private GridDeploymentInfo depInfo;

        /**
         * Required by {@link Externalizable}.
         */
        public EventWrapper() {
            // No-op.
        }

        /**
         * @param evt Event.
         */
        EventWrapper(Event evt) {
            assert evt != null;

            this.evt = evt;
        }

        /**
         * @param marsh Marshaller.
         * @throws IgniteCheckedException In case of error.
         */
        void p2pMarshal(Marshaller marsh) throws IgniteCheckedException {
            assert marsh != null;

            bytes = U.marshal(marsh, evt);
        }

        /**
         * @param marsh Marshaller.
         * @param ldr Class loader.
         * @throws IgniteCheckedException In case of error.
         */
        void p2pUnmarshal(Marshaller marsh, @Nullable ClassLoader ldr) throws IgniteCheckedException {
            assert marsh != null;

            assert evt == null;
            assert bytes != null;

            evt = U.unmarshal(marsh, bytes, ldr);
        }

        /** {@inheritDoc} */
        @Override public void prepare(GridDeploymentInfo depInfo) {
            assert evt instanceof CacheEvent;

            this.depInfo = depInfo;
        }

        /** {@inheritDoc} */
        @Override public GridDeploymentInfo deployInfo() {
            return depInfo;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            boolean b = bytes != null;

            out.writeBoolean(b);

            if (b) {
                U.writeByteArray(out, bytes);
                U.writeString(out, cacheName);
                out.writeObject(depInfo);
            }
            else
                out.writeObject(evt);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            boolean b = in.readBoolean();

            if (b) {
                bytes = U.readByteArray(in);
                cacheName = U.readString(in);
                depInfo = (GridDeploymentInfo)in.readObject();
            }
            else
                evt = (Event)in.readObject();
        }
    }
}
