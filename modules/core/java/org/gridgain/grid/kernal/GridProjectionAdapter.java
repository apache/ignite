// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.ATTR_MACS;

/**
 * @author @java.author
 * @version @java.version
 */
abstract class GridProjectionAdapter extends GridMetadataAwareAdapter implements GridProjection {
    /** Log reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    protected transient GridKernalContext ctx;

    /** */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private transient GridLogger log;

    /** */
    private GridProjection parent;

    /** */
    private GridComputeImpl compute;

    /** */
    private GridMessagingImpl messaging;

    /** */
    private GridEvents evts;

    /**
     *
     * @param parent Parent of this projection.
     * @param ctx Grid kernal context.
     */
    protected GridProjectionAdapter(@Nullable GridProjection parent, GridKernalContext ctx) {
        this(parent);

        assert ctx != null;

        setKernalContext(ctx);
    }

    /**
     *
     * @param parent Parent of this projection.
     */
    protected GridProjectionAdapter(@Nullable GridProjection parent) {
        this.parent = parent;
    }

    /**
     * Gets logger.
     *
     * @return Logger.
     */
    protected GridLogger log() {
        return log;
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

        log = U.logger(ctx, logRef, GridProjectionAdapter.class);
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
            return F.view(ctx.discovery().nodes(), predicate());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridNode node(UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        guard();

        try {
            return F.find(F.concat(false, nodes(), ctx.discovery().daemonNodes()), null,
                F.<GridNode>nodeForNodeId(nodeId));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridProjection forPredicate(GridPredicate<GridNode> p) {
        A.notNull(p, "p");

        guard();

        try {
            // New projection will be dynamic.
            return new GridProjectionImpl(this, ctx, F.and(p, predicate()));
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

        Set<UUID> nodeIds;

        GridPredicate<GridNode> p = predicate();

        if (F.isEmpty(nodes))
            nodeIds = p.apply(node) ? Collections.singleton(node.id()) : Collections.<UUID>emptySet();
        else {
            nodeIds = new HashSet<>(nodes.length + 1);

            for (GridNode n : nodes)
                if (p.apply(n))
                    nodeIds.add(n.id());

            if (p.apply(node))
                nodeIds.add(node.id());
        }

        return newProjection(nodeIds, false);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodes(Collection<? extends GridNode> nodes) {
        A.ensure(!F.isEmpty(nodes), "nodes must not be empty.");

        Set<UUID> nodeIds = new HashSet<>(nodes.size());

        GridPredicate<GridNode> p = predicate();

        for (GridNode n : nodes)
            if (p.apply(n))
                nodeIds.add(n.id());

        // Static projection.
        return newProjection(nodeIds, false);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOthers(GridNode node) {
        A.notNull(node, "node");

        return forPredicate(new OthersFilter(node.id()));
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
    @Override public GridProjection forNodeIds(Collection<UUID> ids) {
        A.ensure(!F.isEmpty(ids), "ids must not be empty.");

        // Static projection.
        return newProjection(new HashSet<>(ids), true);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodeId(UUID nodeId, UUID... nodeIds) {
        A.notNull(nodeId, "nodeId");

        Set<UUID> ids;

        if (F.isEmpty(nodeIds))
            ids = new GridLeanSet<>(1);
        else {
            ids = new HashSet<>(nodeIds.length + 1);

            Collections.addAll(ids, nodeIds);
        }

        ids.add(nodeId);

        return newProjection(ids, true);
    }

    /**
     * Utility method that creates new grid projection with necessary short-circuit logic.
     *
     * @param nodeIds Node IDs to create projection with.
     * @param filterIds If given set must be filtered with this projection predicate.
     * @return Newly created projection.
     */
    @SuppressWarnings({"ConstantConditions"})
    protected GridProjection newProjection(Set<UUID> nodeIds, boolean filterIds) {
        assert nodeIds != null;

        guard();

        try {
            if (filterIds)
                filterNodeIds(nodeIds);

            return new GridProjectionImpl(this, ctx, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /**
     * @param nodeIds Node IDs.
     */
    protected void filterNodeIds(Set<UUID> nodeIds) {
        GridPredicate<GridNode> p = predicate();

        if (F.isAlwaysTrue(p))
            return;

        Iterator<UUID> iter = nodeIds.iterator();

        while (iter.hasNext()) {
            UUID nodeId = iter.next();

            if (nodeId == null)
                throw new NullPointerException("nodeId is null");

            GridNode n = ctx.discovery().node(nodeId);

            if (n == null || !p.apply(n))
                iter.remove();
        }
    }

    /**
     * Utility method.
     *
     * @param p Predicate for the array.
     * @return One-element array.
     */
    @SuppressWarnings("unchecked")
    protected <T> GridPredicate<T>[] asArray(GridPredicate<T> p) {
        return (GridPredicate<T>[])new GridPredicate[] { p };
    }

    /** {@inheritDoc} */
    @Override public GridProjection forRemotes() {
        guard();

        try {
            return new GridProjectionImpl(this, ctx, F.and(predicate(), F.not(F.nodeForNodeId(ctx.localNodeId()))));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridProjection forHost(GridNode node) {
        A.notNull(node, "node");

        String macs = node.attribute(ATTR_MACS);

        assert macs != null;

        return forPredicate(new AttributeFilter(ATTR_MACS, macs));
    }

    /** {@inheritDoc} */
    @Override public GridProjection forDaemons() {
        guard();

        try {
            return new GridProjectionImpl(this, ctx, F.and(predicate(), new DaemonFilter()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridProjection forRandom() {
        return forNode(F.rand(nodes()));
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
    private static class KeysFilter extends GridPredicate<GridNode> {
        /** Context. */
        private final GridKernalContext ctx;

        /** Logger. */
        private final GridLogger log;

        /** Cache name. */
        private final String cacheName;

        /** Keys. */
        private final Collection<?> keys;

        /**
         * @param ctx Context.
         * @param log Logger.
         * @param cacheName Cache name.
         * @param keys Keys.
         */
        private KeysFilter(GridKernalContext ctx, GridLogger log, String cacheName, Collection<?> keys) {
            this.ctx = ctx;
            this.log = log;
            this.cacheName = cacheName;
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridNode n) {
            try {
                return ctx.affinity().mapKeysToNodes(cacheName, keys).keySet().contains(n);
            }
            catch (GridException e) {
                LT.warn(log, e, "Failed to map keys to nodes [cacheName=" + cacheName + ", keys=" + keys + ']');

                return false;
            }
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
