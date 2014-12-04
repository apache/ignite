/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.continuous.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Continuous routine handler for remote event listening.
 */
class GridEventConsumeHandler implements GridContinuousHandler {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default callback. */
    private static final P2<UUID, GridEvent> DFLT_CALLBACK = new P2<UUID, GridEvent>() {
        @Override public boolean apply(UUID uuid, GridEvent e) {
            return true;
        }
    };

    /** Local callback. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private GridBiPredicate<UUID, GridEvent> cb;

    /** Filter. */
    private GridPredicate<GridEvent> filter;

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
    GridEventConsumeHandler(@Nullable GridBiPredicate<UUID, GridEvent> cb, @Nullable GridPredicate<GridEvent> filter,
        @Nullable int[] types) {
        this.cb = cb == null ? DFLT_CALLBACK : cb;
        this.filter = filter;
        this.types = types;
    }

    /** {@inheritDoc} */
    @Override public boolean isForEvents() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isForMessaging() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isForQuery() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean register(final UUID nodeId, final UUID routineId, final GridKernalContext ctx)
        throws GridException {
        assert nodeId != null;
        assert routineId != null;
        assert ctx != null;

        if (cb != null)
            ctx.resource().injectGeneric(cb);

        if (filter != null)
            ctx.resource().injectGeneric(filter);

        final boolean loc = nodeId.equals(ctx.localNodeId());

        lsnr = new GridLocalEventListener() {
            @Override public void onEvent(GridEvent evt) {
                if (filter == null || filter.apply(evt)) {
                    if (loc) {
                        if (!cb.apply(nodeId, evt))
                            ctx.continuous().stopRoutine(routineId);
                    }
                    else {
                        ClusterNode node = ctx.discovery().node(nodeId);

                        if (node != null) {
                            try {
                                EventWrapper wrapper = new EventWrapper(evt);

                                if (evt instanceof GridCacheEvent) {
                                    String cacheName = ((GridCacheEvent)evt).cacheName();

                                    if (ctx.config().isPeerClassLoadingEnabled() && U.hasCache(node, cacheName)) {
                                        wrapper.p2pMarshal(ctx.config().getMarshaller());

                                        wrapper.cacheName = cacheName;

                                        GridCacheDeploymentManager depMgr =
                                            ctx.cache().internalCache(cacheName).context().deploy();

                                        depMgr.prepare(wrapper);
                                    }
                                }

                                ctx.continuous().addNotification(nodeId, routineId, wrapper, null);
                            }
                            catch (GridException e) {
                                U.error(ctx.log(getClass()), "Failed to send event notification to node: " + nodeId, e);
                            }
                        }
                    }
                }
            }
        };

        if (F.isEmpty(types))
            types = EVTS_ALL;

        ctx.event().addLocalEventListener(lsnr, types);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void onListenerRegistered(UUID routineId, GridKernalContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void unregister(UUID routineId, GridKernalContext ctx) {
        assert routineId != null;
        assert ctx != null;

        if (lsnr != null)
            ctx.event().removeLocalEventListener(lsnr, types);
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
                    wrapper.p2pUnmarshal(ctx.config().getMarshaller(), ldr);
                }
                catch (GridException e) {
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
    @Override public void p2pMarshal(GridKernalContext ctx) throws GridException {
        assert ctx != null;
        assert ctx.config().isPeerClassLoadingEnabled();

        if (filter != null) {
            Class cls = U.detectClass(filter);

            clsName = cls.getName();

            GridDeployment dep = ctx.deploy().deploy(cls, U.detectClassLoader(cls));

            if (dep == null)
                throw new GridDeploymentException("Failed to deploy event filter: " + filter);

            depInfo = new GridDeploymentInfoBean(dep);

            filterBytes = ctx.config().getMarshaller().marshal(filter);
        }
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(UUID nodeId, GridKernalContext ctx) throws GridException {
        assert nodeId != null;
        assert ctx != null;
        assert ctx.config().isPeerClassLoadingEnabled();

        if (filterBytes != null) {
            GridDeployment dep = ctx.deploy().getGlobalDeployment(depInfo.deployMode(), clsName, clsName,
                depInfo.userVersion(), nodeId, depInfo.classLoaderId(), depInfo.participants(), null);

            if (dep == null)
                throw new GridDeploymentException("Failed to obtain deployment for class: " + clsName);

            filter = ctx.config().getMarshaller().unmarshal(filterBytes, dep.classLoader());
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object orderedTopic() {
        return null;
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
            filter = (GridPredicate<GridEvent>)in.readObject();

        types = (int[])in.readObject();
    }

    /**
     * Event wrapper.
     */
    private static class EventWrapper implements GridCacheDeployable, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Event. */
        private GridEvent evt;

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
        EventWrapper(GridEvent evt) {
            assert evt != null;

            this.evt = evt;
        }

        /**
         * @param marsh Marshaller.
         * @throws GridException In case of error.
         */
        void p2pMarshal(GridMarshaller marsh) throws GridException {
            assert marsh != null;

            bytes = marsh.marshal(evt);
        }

        /**
         * @param marsh Marshaller.
         * @param ldr Class loader.
         * @throws GridException In case of error.
         */
        void p2pUnmarshal(GridMarshaller marsh, @Nullable ClassLoader ldr) throws GridException {
            assert marsh != null;

            assert evt == null;
            assert bytes != null;

            evt = marsh.unmarshal(bytes, ldr);
        }

        /** {@inheritDoc} */
        @Override public void prepare(GridDeploymentInfo depInfo) {
            assert evt instanceof GridCacheEvent;

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
                evt = (GridEvent)in.readObject();
        }
    }
}
