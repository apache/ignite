// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * @author @java.author
 * @version @java.version
 */
public class GridProjectionAdapter extends GridMetadataAwareAdapter implements GridProjection, Externalizable {
    /** */
    protected transient GridKernalContext ctx;

    /** */
    private transient GridProjection parent;

    /** */
    private transient GridComputeImpl compute;

    /** */
    private transient GridMessagingImpl messaging;

    /** */
    private transient GridEvents evts;

    /** */
    private String gridName;

    /** */
    private GridPredicate<GridNode> p;

    /** */
    private Set<UUID> ids;

    /**
     * Required by {@link Externalizable}.
     */
    public GridProjectionAdapter() {
        // No-op.
    }

    /**
     * @param parent Parent of this projection.
     * @param ctx Grid kernal context.
     * @param p Predicate.
     */
    protected GridProjectionAdapter(@Nullable GridProjection parent, @Nullable GridKernalContext ctx,
        @Nullable GridPredicate<GridNode> p) {
        this.parent = parent;

        if (ctx != null)
            setKernalContext(ctx);

        this.p = p;

        ids = null;
    }

    /**
     * @param parent Parent of this projection.
     * @param ctx Grid kernal context.
     * @param ids Node IDs.
     */
    protected GridProjectionAdapter(@Nullable GridProjection parent, @Nullable GridKernalContext ctx,
        Set<UUID> ids) {
        this.parent = parent;

        if (ctx != null)
            setKernalContext(ctx);

        assert ids != null;

        this.ids = ids;

        p = F.nodeForNodeIds(ids);
    }

    /**
     * <tt>ctx.gateway().readLock()</tt>
     */
    protected void guard() {
        assert ctx != null;

        ctx.gateway().readLock();
    }

    /**
     * <tt>ctx.gateway().readUnlock()</tt>
     */
    protected void unguard() {
        assert ctx != null;

        ctx.gateway().readUnlock();
    }

    /**
     * <tt>ctx.gateway().lightCheck()</tt>
     */
    protected void lightCheck() {
        assert ctx != null;

        ctx.gateway().lightCheck();
    }

    /**
     * Sets kernal context.
     *
     * @param ctx Kernal context to set.
     */
    protected void setKernalContext(GridKernalContext ctx) {
        assert ctx != null;
        assert this.ctx == null;

        this.ctx = ctx;

        if (parent == null)
            parent = ctx.grid();

        gridName = ctx.gridName();
    }

    /** {@inheritDoc} */
    @Override public Grid grid() {
        assert ctx != null;

        guard();

        try {
            return ctx.grid();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCompute compute() {
        if (compute == null) {
            assert ctx != null;

            compute = new GridComputeImpl(ctx, this);
        }

        return compute;
    }

    /** {@inheritDoc} */
    @Override public GridMessaging message() {
        if (messaging == null) {
            assert ctx != null;

            messaging = new GridMessagingImpl(ctx, this);
        }

        return messaging;
    }

    /** {@inheritDoc} */
    @Override public GridEvents events() {
        if (evts == null) {
            assert ctx != null;

            evts = new GridEventsImpl(ctx, this);
        }

        return evts;
    }

    /** {@inheritDoc} */
    @Override public GridProjectionMetrics metrics() throws GridException {
        guard();

        try {
            if (nodes().isEmpty())
                throw U.emptyTopologyException();

            return new GridProjectionMetricsImpl(this);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> nodes() {
        guard();

        try {
            if (ids != null) {
                Collection<GridNode> nodes = new ArrayList<>(ids.size());

                for (UUID id : ids) {
                    GridNode node = ctx.discovery().node(id);

                    if (node != null)
                        nodes.add(node);
                }

                return nodes;
            }
            else {
                Collection<GridNode> all = ctx.discovery().nodes();

                return p != null ? F.view(all, p) : all;
            }
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridNode node(UUID id) {
        A.notNull(id, "id");

        guard();

        try {
            if (ids != null)
                return ids.contains(id) ? ctx.discovery().node(id) : null;
            else {
                GridNode node = ctx.discovery().node(id);

                return node != null && (p == null || p.apply(node)) ? node : null;
            }
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridPredicate<GridNode> predicate() {
        return p != null ? p : F.<GridNode>alwaysTrue();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forPredicate(GridPredicate<GridNode> p) {
        A.notNull(p, "p");

        guard();

        try {
            return new GridProjectionAdapter(this, ctx, this.p != null ? F.and(p, this.p) : p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridProjection forAttribute(String n, @Nullable final String v) {
        A.notNull(n, "n");

        return forPredicate(new AttributeFilter(n, v));
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNode(GridNode node, GridNode... nodes) {
        A.notNull(node, "node");

        guard();

        try {
            Set<UUID> nodeIds;

            if (F.isEmpty(nodes))
                nodeIds = contains(node) ? Collections.singleton(node.id()) : Collections.<UUID>emptySet();
            else {
                nodeIds = new HashSet<>(nodes.length + 1);

                for (GridNode n : nodes)
                    if (contains(n))
                        nodeIds.add(n.id());

                if (contains(node))
                    nodeIds.add(node.id());
            }

            return new GridProjectionAdapter(this, ctx, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodes(Collection<? extends GridNode> nodes) {
        A.notEmpty(nodes, "nodes");

        guard();

        try {
            Set<UUID> nodeIds = new HashSet<>(nodes.size());

            for (GridNode n : nodes)
                if (contains(n))
                    nodeIds.add(n.id());

            return new GridProjectionAdapter(this, ctx, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodeId(UUID id, UUID... ids) {
        A.notNull(id, "id");

        guard();

        try {
            Set<UUID> nodeIds;

            if (F.isEmpty(ids))
                nodeIds = contains(id) ? Collections.singleton(id) : Collections.<UUID>emptySet();
            else {
                nodeIds = new HashSet<>(ids.length + 1);

                for (UUID id0 : ids) {
                    if (contains(id))
                        nodeIds.add(id0);
                }

                if (contains(id))
                    nodeIds.add(id);
            }

            return new GridProjectionAdapter(this, ctx, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodeIds(Collection<UUID> ids) {
        A.notEmpty(ids, "ids");

        guard();

        try {
            Set<UUID> nodeIds = new HashSet<>(ids.size());

            for (UUID id : ids) {
                if (contains(id))
                    nodeIds.add(id);
            }

            return new GridProjectionAdapter(this, ctx, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOthers(GridNode node) {
        A.notNull(node, "node");

        return forOthers(node.id());
    }

    /** {@inheritDoc} */
    @Override public GridProjection forRemotes() {
        return forOthers(ctx.localNodeId());
    }

    /**
     * @param nodeId Node ID.
     * @return New projection.
     */
    private GridProjection forOthers(UUID nodeId) {
        assert nodeId != null;

        if (ids != null) {
            guard();

            try {
                Set<UUID> nodeIds = new HashSet<>(ids.size());

                for (UUID id : ids) {
                    if (!nodeId.equals(id))
                        nodeIds.add(id);
                }

                return new GridProjectionAdapter(this, ctx, nodeIds);
            }
            finally {
                unguard();
            }
        }
        else
            return forPredicate(new OthersFilter(nodeId));
    }

    /** {@inheritDoc} */
    @Override public GridProjection forCache(@Nullable String cacheName, @Nullable String... cacheNames) {
        return forPredicate(new CachesFilter(cacheName, cacheNames));
    }

    /** {@inheritDoc} */
    @Override public GridProjection forStreamer(@Nullable String streamerName, @Nullable String... streamerNames) {
        return forPredicate(new StreamersFilter(streamerName, streamerNames));
    }

    /** {@inheritDoc} */
    @Override public GridProjection forHost(GridNode node) {
        A.notNull(node, "node");

        String macs = node.attribute(ATTR_MACS);

        assert macs != null;

        return forAttribute(ATTR_MACS, macs);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forDaemons() {
        return forPredicate(new DaemonFilter());
    }

    /** {@inheritDoc} */
    @Override public GridProjection forRandom() {
        return ids != null ? forNodeId(F.rand(ids)) : forNode(F.rand(nodes()));
    }

    /**
     * @param n Node.
     * @return Whether node belongs to this projection.
     */
    private boolean contains(GridNode n) {
        assert n != null;

        return ids != null ? ids.contains(n.id()) : p == null || p.apply(n);
    }

    /**
     * @param id Node ID.
     * @return Whether node belongs to this projection.
     */
    private boolean contains(UUID id) {
        assert id != null;

        if (ids != null)
            return ids.contains(id);
        else {
            GridNode n = ctx.discovery().node(id);

            return n != null && (p == null || p.apply(n));
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, gridName);

        out.writeBoolean(ids != null);

        if (ids != null)
            out.writeObject(ids);
        else
            out.writeObject(p);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        gridName = U.readString(in);

        if (in.readBoolean())
            ids = (Set<UUID>)in.readObject();
        else
            p = (GridPredicate<GridNode>)in.readObject();
    }

    /**
     * Reconstructs object on demarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of demarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            GridKernal g = GridFactoryEx.gridx(gridName);

            return ids != null ? new GridProjectionAdapter(g, g.context(), ids) :
                p != null ? new GridProjectionAdapter(g, g.context(), p) : g;
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
    }

    /**
     */
    private static class CachesFilter extends GridPredicate<GridNode> {
        /** Cache name. */
        private final String cacheName;

        /** Cache names. */
        private final String[] cacheNames;

        /**
         * @param cacheName Cache name.
         * @param cacheNames Cache names.
         */
        private CachesFilter(@Nullable String cacheName, @Nullable String[] cacheNames) {
            this.cacheName = cacheName;
            this.cacheNames = cacheNames;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridNode n) {
            if (!U.hasCache(n, cacheName))
                return false;

            if (!F.isEmpty(cacheNames))
                for (String cn : cacheNames)
                    if (!U.hasCache(n, cn))
                        return false;

            return true;
        }
    }

    /**
     */
    private static class StreamersFilter extends GridPredicate<GridNode> {
        /** Streamer name. */
        private final String streamerName;

        /** Streamer names. */
        private final String[] streamerNames;

        /**
         * @param streamerName Streamer name.
         * @param streamerNames Streamer names.
         */
        private StreamersFilter(@Nullable String streamerName, @Nullable String[] streamerNames) {
            this.streamerName = streamerName;
            this.streamerNames = streamerNames;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridNode n) {
            if (!U.hasStreamer(n, streamerName))
                 return false;

            if (!F.isEmpty(streamerNames))
                for (String sn : streamerNames)
                    if (!U.hasStreamer(n, sn))
                        return false;

            return true;
        }
    }

    /**
     */
    private static class AttributeFilter extends GridPredicate<GridNode> {
        /** Name. */
        private final String name;

        /** Value. */
        private final String val;

        /**
         * @param name Name.
         * @param val Value.
         */
        private AttributeFilter(String name, String val) {
            this.name = name;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridNode n) {
            return val == null ? n.attributes().containsKey(name) : val.equals(n.attribute(name));
        }
    }

    /**
     */
    private static class DaemonFilter extends GridPredicate<GridNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(GridNode n) {
            return n.isDaemon();
        }
    }

    /**
     */
    private static class OthersFilter extends GridPredicate<GridNode> {
        /** */
        private final UUID nodeId;

        /**
         * @param nodeId Node ID.
         */
        private OthersFilter(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridNode n) {
            return !nodeId.equals(n.id());
        }
    }
}
