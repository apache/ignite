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

package org.apache.ignite.internal.cluster;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.executor.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.internal.IgniteNodeAttributes.*;

/**
 *
 */
public class ClusterGroupAdapter implements ClusterGroupEx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Kernal context. */
    protected transient GridKernalContext ctx;

    /** Compute. */
    private transient IgniteComputeImpl compute;

    /** Messaging. */
    private transient IgniteMessagingImpl messaging;

    /** Events. */
    private transient IgniteEvents evts;

    /** Services. */
    private transient IgniteServices svcs;

    /** Grid name. */
    private String gridName;

    /** Subject ID. */
    private UUID subjId;

    /** Cluster group predicate. */
    protected IgnitePredicate<ClusterNode> p;

    /** Node IDs. */
    private Set<UUID> ids;

    /**
     * Required by {@link Externalizable}.
     */
    public ClusterGroupAdapter() {
        // No-op.
    }

    /**
     * @param subjId Subject ID.
     * @param ctx Kernal context.
     * @param p Predicate.
     */
    protected ClusterGroupAdapter(@Nullable GridKernalContext ctx,
        @Nullable UUID subjId,
        @Nullable IgnitePredicate<ClusterNode> p)
    {
        if (ctx != null)
            setKernalContext(ctx);

        this.subjId = subjId;
        this.p = p;

        ids = null;
    }

    /**
     * @param ctx Kernal context.
     * @param subjId Subject ID.
     * @param ids Node IDs.
     */
    protected ClusterGroupAdapter(@Nullable GridKernalContext ctx,
        @Nullable UUID subjId,
        Set<UUID> ids)
    {
        if (ctx != null)
            setKernalContext(ctx);

        assert ids != null;

        this.subjId = subjId;
        this.ids = ids;

        p = F.nodeForNodeIds(ids);
    }

    /**
     * @param subjId Subject ID.
     * @param ctx Grid kernal context.
     * @param p Predicate.
     * @param ids Node IDs.
     */
    private ClusterGroupAdapter(@Nullable GridKernalContext ctx,
        @Nullable UUID subjId,
        @Nullable IgnitePredicate<ClusterNode> p,
        Set<UUID> ids)
    {
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
     * Sets kernal context.
     *
     * @param ctx Kernal context to set.
     */
    public void setKernalContext(GridKernalContext ctx) {
        assert ctx != null;
        assert this.ctx == null;

        this.ctx = ctx;

        gridName = ctx.gridName();
    }

    /** {@inheritDoc} */
    @Override public final Ignite ignite() {
        assert ctx != null;

        guard();

        try {
            return ctx.grid();
        }
        finally {
            unguard();
        }
    }

    /**
     * @return {@link IgniteCompute} for this cluster group.
     */
    public final IgniteCompute compute() {
        if (compute == null) {
            assert ctx != null;

            compute = new IgniteComputeImpl(ctx, this, subjId, false);
        }

        return compute;
    }

    /**
     * @return {@link IgniteMessaging} for this cluster group.
     */
    public final IgniteMessaging message() {
        if (messaging == null) {
            assert ctx != null;

            messaging = new IgniteMessagingImpl(ctx, this, false);
        }

        return messaging;
    }

    /**
     * @return {@link IgniteEvents} for this cluster group.
     */
    public final IgniteEvents events() {
        if (evts == null) {
            assert ctx != null;

            evts = new IgniteEventsImpl(ctx, this, false);
        }

        return evts;
    }

    /**
     * @return {@link IgniteServices} for this cluster group.
     */
    public IgniteServices services() {
        if (svcs == null) {
            assert ctx != null;

            svcs = new IgniteServicesImpl(ctx, this, false);
        }

        return svcs;
    }

    /**
     * @return {@link ExecutorService} for this cluster group.
     */
    public ExecutorService executorService() {
        assert ctx != null;

        return new GridExecutorService(this, ctx);
    }

    /** {@inheritDoc} */
    @Override public final ClusterMetrics metrics() {
        guard();

        try {
            if (nodes().isEmpty())
                throw U.convertException(U.emptyTopologyException());

            return new ClusterMetricsSnapshot(this);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        guard();

        try {
            if (ids != null) {
                if (ids.isEmpty())
                    return Collections.emptyList();
                else if (ids.size() == 1) {
                    ClusterNode node = ctx.discovery().node(F.first(ids));

                    return node != null ? Collections.singleton(node) : Collections.<ClusterNode>emptyList();
                }
                else {
                    Collection<ClusterNode> nodes = new ArrayList<>(ids.size());

                    for (UUID id : ids) {
                        ClusterNode node = ctx.discovery().node(id);

                        if (node != null)
                            nodes.add(node);
                    }

                    return nodes;
                }
            }
            else {
                Collection<ClusterNode> all = ctx.discovery().allNodes();

                return p != null ? F.view(all, p) : all;
            }
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final ClusterNode node(UUID id) {
        A.notNull(id, "id");

        guard();

        try {
            if (ids != null)
                return ids.contains(id) ? ctx.discovery().node(id) : null;
            else {
                ClusterNode node = ctx.discovery().node(id);

                return node != null && (p == null || p.apply(node)) ? node : null;
            }
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterNode node() {
        return F.first(nodes());
    }

    /** {@inheritDoc} */
    @Override public final IgnitePredicate<ClusterNode> predicate() {
        return p != null ? p : F.<ClusterNode>alwaysTrue();
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forPredicate(IgnitePredicate<ClusterNode> p) {
        A.notNull(p, "p");

        guard();

        try {
            if (p != null)
                ctx.resource().injectGeneric(p);

            return new ClusterGroupAdapter(ctx, subjId, this.p != null ? F.and(p, this.p) : p);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forAttribute(String name, @Nullable final String val) {
        A.notNull(name, "n");

        return forPredicate(new AttributeFilter(name, val));
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forServers() {
        return forPredicate(new AttributeFilter(IgniteNodeAttributes.ATTR_CLIENT_MODE, false));
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forClients() {
        return forPredicate(new AttributeFilter(IgniteNodeAttributes.ATTR_CLIENT_MODE, true));
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forNode(ClusterNode node, ClusterNode... nodes) {
        A.notNull(node, "node");

        guard();

        try {
            Set<UUID> nodeIds;

            if (F.isEmpty(nodes))
                nodeIds = contains(node) ? Collections.singleton(node.id()) : Collections.<UUID>emptySet();
            else {
                nodeIds = U.newHashSet(nodes.length + 1);

                for (ClusterNode n : nodes)
                    if (contains(n))
                        nodeIds.add(n.id());

                if (contains(node))
                    nodeIds.add(node.id());
            }

            return new ClusterGroupAdapter(ctx, subjId, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forNodes(Collection<? extends ClusterNode> nodes) {
        A.notEmpty(nodes, "nodes");

        guard();

        try {
            Set<UUID> nodeIds = U.newHashSet(nodes.size());

            for (ClusterNode n : nodes)
                if (contains(n))
                    nodeIds.add(n.id());

            return new ClusterGroupAdapter(ctx, subjId, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forNodeId(UUID id, UUID... ids) {
        A.notNull(id, "id");

        guard();

        try {
            Set<UUID> nodeIds;

            if (F.isEmpty(ids))
                nodeIds = contains(id) ? Collections.singleton(id) : Collections.<UUID>emptySet();
            else {
                nodeIds = U.newHashSet(ids.length + 1);

                for (UUID id0 : ids) {
                    if (contains(id))
                        nodeIds.add(id0);
                }

                if (contains(id))
                    nodeIds.add(id);
            }

            return new ClusterGroupAdapter(ctx, subjId, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forNodeIds(Collection<UUID> ids) {
        A.notEmpty(ids, "ids");

        guard();

        try {
            Set<UUID> nodeIds = U.newHashSet(ids.size());

            for (UUID id : ids) {
                if (contains(id))
                    nodeIds.add(id);
            }

            return new ClusterGroupAdapter(ctx, subjId, nodeIds);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forOthers(ClusterNode node, ClusterNode... nodes) {
        A.notNull(node, "node");

        return forOthers(F.concat(false, node.id(), F.nodeIds(Arrays.asList(nodes))));
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forOthers(ClusterGroup grp) {
        A.notNull(grp, "grp");

        if (ids != null) {
            guard();

            try {
                Set<UUID> nodeIds = U.newHashSet(ids.size());

                for (UUID id : ids) {
                    ClusterNode n = node(id);

                    if (n != null && !grp.predicate().apply(n))
                        nodeIds.add(id);
                }

                return new ClusterGroupAdapter(ctx, subjId, nodeIds);
            }
            finally {
                unguard();
            }
        }
        else
            return forPredicate(F.not(grp.predicate()));
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forRemotes() {
        return forOthers(Collections.singleton(ctx.localNodeId()));
    }

    /**
     * @param excludeIds Node IDs.
     * @return New cluster group.
     */
    private ClusterGroup forOthers(Collection<UUID> excludeIds) {
        assert excludeIds != null;

        if (ids != null) {
            guard();

            try {
                Set<UUID> nodeIds = U.newHashSet(ids.size());

                for (UUID id : ids) {
                    if (!excludeIds.contains(id))
                        nodeIds.add(id);
                }

                return new ClusterGroupAdapter(ctx, subjId, nodeIds);
            }
            finally {
                unguard();
            }
        }
        else
            return forPredicate(new OthersFilter(excludeIds));
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forCacheNodes(@Nullable String cacheName) {
        return forPredicate(new CachesFilter(cacheName, true, true, true));
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forDataNodes(@Nullable String cacheName) {
        return forPredicate(new CachesFilter(cacheName, true, false, false));
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forClientNodes(@Nullable String cacheName) {
        return forPredicate(new CachesFilter(cacheName, false, true, true));
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forCacheNodes(@Nullable String cacheName, boolean affNodes, boolean nearNodes,
        boolean clientNodes) {
        return forPredicate(new CachesFilter(cacheName, affNodes, nearNodes, clientNodes));
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forHost(ClusterNode node) {
        A.notNull(node, "node");

        String macs = node.attribute(ATTR_MACS);

        assert macs != null;

        return forAttribute(ATTR_MACS, macs);
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forDaemons() {
        return forPredicate(new DaemonFilter());
    }

    /** {@inheritDoc} */
    @Override public final ClusterGroup forRandom() {
        return ids != null ? forNodeId(F.rand(ids)) : forNode(F.rand(nodes()));
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forOldest() {
        return new AgeClusterGroup(this, true);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forYoungest() {
        return new AgeClusterGroup(this, false);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroupEx forSubjectId(UUID subjId) {
        if (subjId == null)
            return this;

        guard();

        try {
            return ids != null ? new ClusterGroupAdapter(ctx, subjId, ids) :
                new ClusterGroupAdapter(ctx, subjId, p);
        }
        finally {
            unguard();
        }
    }

    /**
     * @param n Node.
     * @return Whether node belongs to this cluster group.
     */
    private boolean contains(ClusterNode n) {
        assert n != null;

        return ids != null ? ids.contains(n.id()) : p == null || p.apply(n);
    }

    /**
     * @param id Node ID.
     * @return Whether node belongs to this cluster group.
     */
    private boolean contains(UUID id) {
        assert id != null;

        if (ids != null)
            return ids.contains(id);
        else {
            ClusterNode n = ctx.discovery().node(id);

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
            p = (IgnitePredicate<ClusterNode>)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            IgniteKernal g = IgnitionEx.gridx(gridName);

            return ids != null ? new ClusterGroupAdapter(g.context(), subjId, ids) :
                p != null ? new ClusterGroupAdapter(g.context(), subjId, p) : g;
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
    }

    /**
     */
    private static class CachesFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private final String cacheName;

        /** Affinity nodes. */
        private boolean affNodes;

        /** Near nodes. */
        private boolean nearNodes;

        /** Client nodes. */
        private boolean clients;

        /** Injected Ignite instance. */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /**
         * @param cacheName Cache name.
         */
        private CachesFilter(@Nullable String cacheName, boolean affNodes, boolean nearNodes, boolean clients) {
            this.cacheName = cacheName;
            this.affNodes = affNodes;
            this.nearNodes = nearNodes;
            this.clients = clients;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("RedundantIfStatement")
        @Override public boolean apply(ClusterNode n) {
            GridDiscoveryManager disco = ((IgniteKernal)ignite).context().discovery();

            if (affNodes && disco.cacheAffinityNode(n, cacheName))
                return true;

            if (!affNodes && disco.cacheAffinityNode(n, cacheName))
                return false;

            if (nearNodes && disco.cacheNearNode(n, cacheName))
                return true;

            if (clients && disco.cacheClientNode(n, cacheName))
                return true;

            return false;
        }
    }

    /**
     */
    private static class AttributeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Name. */
        private final String name;

        /** Value. */
        private final Object val;

        /**
         * @param name Name.
         * @param val Value.
         */
        private AttributeFilter(String name, Object val) {
            this.name = name;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode n) {
            return val == null ? n.attributes().containsKey(name) : val.equals(n.attribute(name));
        }
    }

    /**
     */
    private static class DaemonFilter implements IgnitePredicate<ClusterNode> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode n) {
            return n.isDaemon();
        }
    }

    /**
     */
    private static class OthersFilter implements IgnitePredicate<ClusterNode> {
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
        @Override public boolean apply(ClusterNode n) {
            return !nodeIds.contains(n.id());
        }
    }

    /**
     * Age-based cluster group.
     */
    private static class AgeClusterGroup extends ClusterGroupAdapter {
        /** Serialization version. */
        private static final long serialVersionUID = 0L;

        /** Oldest flag. */
        private boolean isOldest;

        /** Selected node. */
        private volatile ClusterNode node;

        /** Last topology version. */
        private volatile long lastTopVer;

        /**
         * Required for {@link Externalizable}.
         */
        public AgeClusterGroup() {
            // No-op.
        }

        /**
         * @param parent Parent cluster group.
         * @param isOldest Oldest flag.
         */
        private AgeClusterGroup(ClusterGroupAdapter parent, boolean isOldest) {
            super(parent.ctx, parent.subjId, parent.p, parent.ids);

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
        @Override public ClusterNode node() {
            if (ctx.discovery().topologyVersion() != lastTopVer)
                reset();

            return node;
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> nodes() {
            if (ctx.discovery().topologyVersion() != lastTopVer)
                reset();

            ClusterNode node = this.node;

            return node == null ? Collections.<ClusterNode>emptyList() : Collections.singletonList(node);
        }
    }
}
