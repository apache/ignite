/* @java.file.header */

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
import org.gridgain.grid.service.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 *
 */
public class GridProjectionAdapter implements GridProjectionEx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Kernal context. */
    protected transient GridKernalContext ctx;

    /** Parent projection. */
    private transient GridProjection parent;

    /** Compute. */
    private transient GridComputeImpl compute;

    /** Messaging. */
    private transient GridMessagingImpl messaging;

    /** Events. */
    private transient GridEvents evts;

    /** Services. */
    private transient GridServices svcs;

    /** Grid name. */
    private String gridName;

    /** Subject ID. */
    private UUID subjId;

    /** Projection predicate. */
    protected GridPredicate<GridNode> p;

    /** Node IDs. */
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
        @Nullable UUID subjId, @Nullable GridPredicate<GridNode> p) {
        this.parent = parent;

        if (ctx != null)
            setKernalContext(ctx);

        this.subjId = subjId;
        this.p = p;

        ids = null;
    }

    /**
     * @param parent Parent of this projection.
     * @param ctx Grid kernal context.
     * @param ids Node IDs.
     */
    protected GridProjectionAdapter(@Nullable GridProjection parent, @Nullable GridKernalContext ctx,
        @Nullable UUID subjId, Set<UUID> ids) {
        this.parent = parent;

        if (ctx != null)
            setKernalContext(ctx);

        assert ids != null;

        this.subjId = subjId;
        this.ids = ids;

        p = F.nodeForNodeIds(ids);
    }

    /**
     * @param parent Parent of this projection.
     * @param ctx Grid kernal context.
     * @param p Predicate.
     * @param ids Node IDs.
     */
    private GridProjectionAdapter(@Nullable GridProjection parent, @Nullable GridKernalContext ctx,
        @Nullable UUID subjId, @Nullable GridPredicate<GridNode> p, Set<UUID> ids) {
        this.parent = parent;

        if (ctx != null)
            setKernalContext(ctx);

        this.subjId = subjId;
        this.p = p;
        this.ids = ids;

        if (p == null && ids != null)
            this.p = F.nodeForNodeIds(ids);
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
    @Override public final Grid grid() {
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
    @Override public final GridCompute compute() {
        if (compute == null) {
            assert ctx != null;

            compute = new GridComputeImpl(ctx, this, subjId, false);
        }

        return compute;
    }

    /** {@inheritDoc} */
    @Override public final GridMessaging message() {
        if (messaging == null) {
            assert ctx != null;

            messaging = new GridMessagingImpl(ctx, this);
        }

        return messaging;
    }

    /** {@inheritDoc} */
    @Override public final GridEvents events() {
        if (evts == null) {
            assert ctx != null;

            evts = new GridEventsImpl(ctx, this);
        }

        return evts;
    }

    /** {@inheritDoc} */
    @Override public GridServices services() {
        if (svcs == null) {
            assert ctx != null;

            svcs = new GridServicesImpl(ctx, this);
        }

        return svcs;
    }

    /** {@inheritDoc} */
    @Override public final GridProjectionMetrics metrics() throws GridException {
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
                if (ids.isEmpty())
                    return Collections.emptyList();
                else if (ids.size() == 1) {
                    GridNode node = ctx.discovery().node(F.first(ids));

                    return node != null ? Collections.singleton(node) : Collections.<GridNode>emptyList();
                }
                else {
                    Collection<GridNode> nodes = new ArrayList<>(ids.size());

                    for (UUID id : ids) {
                        GridNode node = ctx.discovery().node(id);

                        if (node != null)
                            nodes.add(node);
                    }

                    return nodes;
                }
            }
            else {
                Collection<GridNode> all = ctx.discovery().allNodes();

                return p != null ? F.view(all, p) : all;
            }
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final GridNode node(UUID id) {
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
    @Override public GridNode node() {
        return F.first(nodes());
    }

    /** {@inheritDoc} */
    @Override public final GridPredicate<GridNode> predicate() {
        return p != null ? p : F.<GridNode>alwaysTrue();
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forPredicate(GridPredicate<GridNode> p) {
        A.notNull(p, "p");

        guard();

        try {
            return new GridProjectionAdapter(this, ctx, subjId, this.p != null ? F.and(p, this.p) : p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forAttribute(String name, @Nullable final String val) {
        A.notNull(name, "n");

        return forPredicate(new AttributeFilter(name, val));
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forNode(GridNode node, GridNode... nodes) {
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

            return new GridProjectionAdapter(this, ctx, subjId, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forNodes(Collection<? extends GridNode> nodes) {
        A.notEmpty(nodes, "nodes");

        guard();

        try {
            Set<UUID> nodeIds = new HashSet<>(nodes.size());

            for (GridNode n : nodes)
                if (contains(n))
                    nodeIds.add(n.id());

            return new GridProjectionAdapter(this, ctx, subjId, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forNodeId(UUID id, UUID... ids) {
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

            return new GridProjectionAdapter(this, ctx, subjId, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forNodeIds(Collection<UUID> ids) {
        A.notEmpty(ids, "ids");

        guard();

        try {
            Set<UUID> nodeIds = new HashSet<>(ids.size());

            for (UUID id : ids) {
                if (contains(id))
                    nodeIds.add(id);
            }

            return new GridProjectionAdapter(this, ctx, subjId, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forOthers(GridNode node, GridNode... nodes) {
        A.notNull(node, "node");

        return forOthers(F.concat(false, node.id(), F.nodeIds(Arrays.asList(nodes))));
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOthers(GridProjection prj) {
        A.notNull(prj, "prj");

        if (ids != null) {
            guard();

            try {
                Set<UUID> nodeIds = new HashSet<>(ids.size());

                for (UUID id : ids) {
                    GridNode n = node(id);

                    if (n != null && !prj.predicate().apply(n))
                        nodeIds.add(id);
                }

                return new GridProjectionAdapter(this, ctx, subjId, nodeIds);
            }
            finally {
                unguard();
            }
        }
        else
            return forPredicate(F.not(prj.predicate()));
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forRemotes() {
        return forOthers(Collections.singleton(ctx.localNodeId()));
    }

    /**
     * @param excludeIds Node IDs.
     * @return New projection.
     */
    private GridProjection forOthers(Collection<UUID> excludeIds) {
        assert excludeIds != null;

        if (ids != null) {
            guard();

            try {
                Set<UUID> nodeIds = new HashSet<>(ids.size());

                for (UUID id : ids) {
                    if (!excludeIds.contains(id))
                        nodeIds.add(id);
                }

                return new GridProjectionAdapter(this, ctx, subjId, nodeIds);
            }
            finally {
                unguard();
            }
        }
        else
            return forPredicate(new OthersFilter(excludeIds));
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forCache(@Nullable String cacheName, @Nullable String... cacheNames) {
        return forPredicate(new CachesFilter(cacheName, cacheNames));
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forStreamer(@Nullable String streamerName, @Nullable String... streamerNames) {
        return forPredicate(new StreamersFilter(streamerName, streamerNames));
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forHost(GridNode node) {
        A.notNull(node, "node");

        String macs = node.attribute(ATTR_MACS);

        assert macs != null;

        return forAttribute(ATTR_MACS, macs);
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forDaemons() {
        return forPredicate(new DaemonFilter());
    }

    /** {@inheritDoc} */
    @Override public final GridProjection forRandom() {
        return ids != null ? forNodeId(F.rand(ids)) : forNode(F.rand(nodes()));
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOldest() {
        return new AgeProjection(this, true);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forYoungest() {
        return new AgeProjection(this, false);
    }

    /** {@inheritDoc} */
    @Override public GridProjectionEx forSubjectId(UUID subjId) {
        if (subjId == null)
            return this;

        guard();

        try {
            return ids != null ? new GridProjectionAdapter(this, ctx, subjId, ids) :
                new GridProjectionAdapter(this, ctx, subjId, p);
        }
        finally {
            unguard();
        }
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
        U.writeUuid(out, subjId);

        out.writeBoolean(ids != null);

        if (ids != null)
            out.writeObject(ids);
        else
            out.writeObject(p);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        gridName = U.readString(in);
        subjId = U.readUuid(in);

        if (in.readBoolean())
            ids = (Set<UUID>)in.readObject();
        else
            p = (GridPredicate<GridNode>)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            GridKernal g = GridGainEx.gridx(gridName);

            return ids != null ? new GridProjectionAdapter(g, g.context(), subjId, ids) :
                p != null ? new GridProjectionAdapter(g, g.context(), subjId, p) : g;
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
    }

    /**
     */
    private static class CachesFilter implements GridPredicate<GridNode> {
        /** */
        private static final long serialVersionUID = 0L;

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
    private static class StreamersFilter implements GridPredicate<GridNode> {
        /** */
        private static final long serialVersionUID = 0L;

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
    private static class AttributeFilter implements GridPredicate<GridNode> {
        /** */
        private static final long serialVersionUID = 0L;

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
    private static class DaemonFilter implements GridPredicate<GridNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(GridNode n) {
            return n.isDaemon();
        }
    }

    /**
     */
    private static class OthersFilter implements GridPredicate<GridNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Collection<UUID> nodeIds;

        /**
         * @param nodeIds Node IDs.
         */
        private OthersFilter(Collection<UUID> nodeIds) {
            this.nodeIds = nodeIds;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridNode n) {
            return !nodeIds.contains(n.id());
        }
    }

    /**
     * Age-based projection.
     */
    private static class AgeProjection extends GridProjectionAdapter {
        /** Serialization version. */
        private static final long serialVersionUID = 0L;

        /** Oldest flag. */
        private boolean isOldest;

        /** Selected node. */
        private volatile GridNode node;

        /** Last topology version. */
        private volatile long lastTopVer;

        /**
         * Required for {@link Externalizable}.
         */
        public AgeProjection() {
            // No-op.
        }

        /**
         * @param prj Parent projection.
         * @param isOldest Oldest flag.
         */
        private AgeProjection(GridProjectionAdapter prj, boolean isOldest) {
            super(prj.parent, prj.ctx, prj.subjId, prj.p, prj.ids);

            this.isOldest = isOldest;

            reset();
        }

        /**
         * Resets node.
         */
        private synchronized void reset() {
            guard();

            try {
                lastTopVer = ctx.discovery().topologyVersion();

                this.node = isOldest ? U.oldest(super.nodes(), null) : U.youngest(super.nodes(), null);
            }
            finally {
                unguard();
            }
        }

        /** {@inheritDoc} */
        @Override public GridNode node() {
            if (ctx.discovery().topologyVersion() != lastTopVer)
                reset();

            return node;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridNode> nodes() {
            if (ctx.discovery().topologyVersion() != lastTopVer)
                reset();

            GridNode node = this.node;

            return node == null ? Collections.<GridNode>emptyList() : Collections.singletonList(node);
        }
    }
}
