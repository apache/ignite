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

package org.apache.ignite.internal.client.thin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFeatureNotSupportedByServerException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Implementation of {@link ClientClusterGroup}.
 */
class ClientClusterGroupImpl implements ClientClusterGroup {
    /** Channel. */
    protected final ReliableChannel ch;

    /** Marshaller utils. */
    protected final ClientUtils utils;

    /** Projection filters. */
    private final ProjectionFilters projectionFilters;

    /** Cached topology version. */
    private long cachedTopVer;

    /** Cached node IDs. Should be changed atomically with cachedTopVer under lock on ClientClusterGroupImpl instance. */
    private Collection<UUID> cachedNodeIds;

    /** Cached nodes. */
    private final Map<UUID, ClusterNode> cachedNodes = new ConcurrentHashMap<>();

    /**
     *
     */
    ClientClusterGroupImpl(ReliableChannel ch, ClientBinaryMarshaller marsh) {
        this.ch = ch;

        utils = new ClientUtils(marsh);

        projectionFilters = ProjectionFilters.FULL_PROJECTION;
    }

    /**
     *
     */
    private ClientClusterGroupImpl(ReliableChannel ch, ClientUtils utils,
        ProjectionFilters projectionFilters) {
        this.ch = ch;
        this.utils = utils;
        this.projectionFilters = projectionFilters;
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forNodes(Collection<? extends ClusterNode> nodes) {
        A.notNull(nodes, "nodes");

        ClientClusterGroupImpl grp = forProjectionFilters(projectionFilters.forNodeIds(new HashSet<>(F.nodeIds(nodes))));

        for (ClusterNode node : nodes)
            grp.cachedNodes.put(node.id(), node);

        return grp;
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forNode(ClusterNode node, ClusterNode... nodes) {
        A.notNull(node, "node");

        return forNodes(collection(ArrayList::new, node, nodes));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forOthers(ClusterNode node, ClusterNode... nodes) {
        A.notNull(node, "node");

        Collection<ClusterNode> nodes0 = collection(U::newHashSet, node, nodes);

        return forPredicate(n -> !nodes0.contains(n));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forOthers(ClientClusterGroup prj) {
        A.notNull(prj, "prj");

        Collection<ClusterNode> nodes0 = prj.nodes();

        return forPredicate(n -> !nodes0.contains(n));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forNodeIds(Collection<UUID> ids) {
        A.notNull(ids, "ids");

        return forProjectionFilters(projectionFilters.forNodeIds(new HashSet<>(ids)));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forNodeId(UUID id, UUID... ids) {
        A.notNull(id, "id");

        return forProjectionFilters(projectionFilters.forNodeIds(collection(ArrayList::new, id, ids)));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forPredicate(Predicate<ClusterNode> p) {
        A.notNull(p, "p");

        return forProjectionFilters(projectionFilters.forPredicate(p));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forAttribute(String name, @Nullable Object val) {
        A.notNull(name, "name");

        return forProjectionFilters(projectionFilters.forAttribute(name, val));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forServers() {
        return forProjectionFilters(projectionFilters == ProjectionFilters.FULL_PROJECTION ?
            ProjectionFilters.DEFAULT_PROJECTION : projectionFilters.forNodeType(true));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forClients() {
        return forProjectionFilters(projectionFilters.forNodeType(false));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forRandom() {
        return forProjectionFilters(projectionFilters.forResultFilter(nodes -> {
            if (F.isEmpty(nodes))
                return nodes;

            return Collections.singletonList(nodes.get(ThreadLocalRandom.current().nextInt(nodes.size())));
        }));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forOldest() {
        return forProjectionFilters(projectionFilters.forResultFilter(nodes -> {
            if (F.isEmpty(nodes))
                return nodes;

            return nodes.stream().min(Comparator.comparing(ClusterNode::order)).map(Collections::singletonList)
                .orElse(Collections.emptyList());
        }));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forYoungest() {
        return forProjectionFilters(projectionFilters.forResultFilter(nodes -> {
            if (F.isEmpty(nodes))
                return nodes;

            return nodes.stream().max(Comparator.comparing(ClusterNode::order)).map(Collections::singletonList)
                .orElse(Collections.emptyList());
        }));
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forHost(ClusterNode node) {
        A.notNull(node, "node");

        String macs = node.attribute(ATTR_MACS);

        assert macs != null;

        return forAttribute(ATTR_MACS, macs);
    }

    /** {@inheritDoc} */
    @Override public ClientClusterGroup forHost(String host, String... hosts) {
        A.notNull(host, "host");

        Collection<String> hosts0 = collection(U::newHashSet, host, hosts);

        return forPredicate(n -> {
            for (String hostName : n.hostNames()) {
                if (hosts0.contains(hostName))
                    return true;
            }

            return false;
        });
    }

    /**
     * @param projectionFilters Projection filters.
     */
    private ClientClusterGroupImpl forProjectionFilters(ProjectionFilters projectionFilters) {
        return this.projectionFilters == projectionFilters ? this :
            new ClientClusterGroupImpl(ch, utils, projectionFilters);
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        return Collections.unmodifiableCollection(nodes0());
    }

    /**
     * {@inheritDoc}
     */
    @Override public ClusterNode node(UUID nid) {
        ClusterNode node = cachedNodes.get(nid);

        if (projectionFilters.resultFilter == null) {
            if (node == null)
                node = F.first(nodesByIds(Collections.singleton(nid)));

            return node != null && projectionFilters.testAllPredicates(node) ? node : null;
        }
        else
            return F.find(nodes0(), null, F.nodeForNodeId(nid));
    }

    /** {@inheritDoc} */
    @Override public ClusterNode node() {
        return F.first(nodes0());
    }

    /**
     * Gets node id's.
     *
     * Note: This method is for internal use only. For optimization purposes it can return not existing node IDs if
     * only filter by node IDs was explicitly set.
     * Method also returns null for default projection (for server nodes).
     */
    public Collection<UUID> nodeIds() {
        if (projectionFilters == ProjectionFilters.DEFAULT_PROJECTION)
            return null;
        else if (projectionFilters.hasOnlyNodeIdsFilters())
            return Collections.unmodifiableCollection(projectionFilters.nodeIds);
        else if (projectionFilters.hasOnlyServerSideFilters()) {
            Collection<UUID> nodeIds = requestNodeIds();

            if (projectionFilters.nodeIds != null)
                nodeIds.retainAll(projectionFilters.nodeIds);

            return nodeIds;
        } else
            return F.nodeIds(nodes0());
    }

    /**
     *
     */
    private Collection<ClusterNode> nodes0() {
        return projectionFilters.hasOnlyNodeIdsFilters() ? nodesByIds(projectionFilters.nodeIds) :
            nodesByIds(requestNodeIds());
    }

    /**
     * Requests node IDs from the server.
     */
    private synchronized Collection<UUID> requestNodeIds() {
        try {
            return ch.service(ClientOperation.CLUSTER_GROUP_GET_NODE_IDS,
                req -> {
                    if (!req.clientChannel().protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.CLUSTER_GROUPS))
                        throw new ClientFeatureNotSupportedByServerException(ProtocolBitmaskFeature.CLUSTER_GROUPS);

                    try (BinaryRawWriterEx writer = utils.createBinaryWriter(req.out())) {
                        writer.writeLong(cachedTopVer);

                        projectionFilters.write(writer);
                    }
                },
                res -> {
                    if (!res.in().readBoolean())
                        return new ArrayList<>(cachedNodeIds); // There were no changes since last request.

                    long topVer = res.in().readLong(); // Topology version.

                    int nodesCnt = res.in().readInt();

                    Collection<UUID> nodeIds = new ArrayList<>(nodesCnt);

                    for (int i = 0; i < nodesCnt; i++)
                        nodeIds.add(new UUID(res.in().readLong(), res.in().readLong()));

                    cachedNodes.keySet().retainAll(nodeIds);

                    cachedTopVer = topVer;

                    cachedNodeIds = nodeIds;

                    return new ArrayList<>(nodeIds);
                });
        }
        catch (ClientError e) {
            throw new ClientException(e);
        }
    }

    /**
     * Get nodes by IDs. Try to get them from cache first, if some of them not found in cache, request them from
     * the server.
     *
     * @param nodeIds Node ids.
     */
    private Collection<ClusterNode> nodesByIds(Collection<UUID> nodeIds) {
        List<ClusterNode> nodes = new ArrayList<>();
        Collection<UUID> nodesToReq = null;

        for (UUID nodeId : nodeIds) {
            ClusterNode node = cachedNodes.get(nodeId);

            if (node != null) {
                if (projectionFilters.testClientSidePredicates(node))
                    nodes.add(node);
            }
            else {
                if (nodesToReq == null)
                    nodesToReq = new ArrayList<>(nodeIds.size());

                nodesToReq.add(nodeId);
            }
        }

        if (!F.isEmpty(nodesToReq))
            nodes.addAll(requestNodesByIds(nodesToReq));

        return projectionFilters.applyResultFilter(nodes);
    }

    /**
     * Requests nodes from the server.
     *
     * @param nodeIds Node ids.
     */
    private Collection<ClusterNode> requestNodesByIds(Collection<UUID> nodeIds) {
        try {
            return ch.service(ClientOperation.CLUSTER_GROUP_GET_NODE_INFO,
                req -> {
                    if (!req.clientChannel().protocolCtx().isFeatureSupported(ProtocolBitmaskFeature.CLUSTER_GROUPS))
                        throw new ClientFeatureNotSupportedByServerException(ProtocolBitmaskFeature.CLUSTER_GROUPS);

                    req.out().writeInt(nodeIds.size());

                    for (UUID nodeId : nodeIds) {
                        req.out().writeLong(nodeId.getMostSignificantBits());
                        req.out().writeLong(nodeId.getLeastSignificantBits());
                    }
                },
                res -> {
                    try (BinaryReaderExImpl reader = utils.createBinaryReader(res.in())) {
                        int nodesCnt = reader.readInt();

                        Collection<ClusterNode> nodes = new ArrayList<>();

                        for (int i = 0; i < nodesCnt; i++) {
                            ClusterNode node = readClusterNode(reader);

                            cachedNodes.put(node.id(), node);

                            if (projectionFilters.testClientSidePredicates(node))
                                nodes.add(node);
                        }

                        return nodes;
                    }
                    catch (IOException e) {
                        throw new ClientError(e);
                    }
                });
        }
        catch (ClientError e) {
            throw new ClientException(e);
        }
    }

    /**
     * @param reader Reader.
     */
    private ClusterNode readClusterNode(BinaryReaderExImpl reader) {
        return new ClientClusterNodeImpl(
            reader.readUuid(),
            readNodeAttributes(reader),
            reader.readCollection(), // Addresses.
            reader.readCollection(), // Host names.
            reader.readLong(), // Order.
            reader.readBoolean(), // Is local.
            reader.readBoolean(), // Is daemon.
            reader.readBoolean(), // Is client.
            reader.readObjectDetached(), // Consistent ID.
            readProductVersion(reader)
        );
    }

    /**
     * @param reader Reader.
     */
    private Map<String, Object> readNodeAttributes(BinaryReaderExImpl reader) {
        int attrCnt = reader.readInt();

        Map<String, Object> attrs = new HashMap<>(attrCnt);

        for (int i = 0; i < attrCnt; i++)
            attrs.put(reader.readString(), reader.readObjectDetached());

        return attrs;
    }

    /**
     * @param reader Reader.
     */
    private IgniteProductVersion readProductVersion(BinaryReaderExImpl reader) {
        return new IgniteProductVersion(
            reader.readByte(), // Major.
            reader.readByte(), // Minor.
            reader.readByte(), // Maintenance.
            reader.readString(), // Stage.
            reader.readLong(), // Revision timestamp.
            reader.readByteArray() // Revision hash.
        );
    }

    /**
     * Build collection from the first element and array of elements.
     *
     * @param factory Collection factory.
     * @param item First item.
     * @param items Other items.
     */
    private static <T> Collection<T> collection(IntFunction<Collection<T>> factory, T item, T... items) {
        Collection<T> col = factory.apply((item != null ? 1 : 0) + (items == null ? 0 : items.length));

        if (item != null)
            col.add(item);

        if (!F.isEmpty(items))
            col.addAll(Arrays.asList(items));

        return col;
    }

    /**
     * Class to store cluster group projection filters.
     */
    private static class ProjectionFilters {
        /** Projection without any filters for full set of nodes. */
        public static final ProjectionFilters FULL_PROJECTION = new ProjectionFilters();

        /**
         * Projection for server nodes, used by default for compute and service operations when cluster group is not
         * defined explicitly.
         */
        public static final ProjectionFilters DEFAULT_PROJECTION =
            new ProjectionFilters(null, null, Boolean.TRUE, null, null);

        /** Filter for empty projection. Will be returned if there are mutually exclusive filters detected. */
        public static final ProjectionFilters EMPTY_PROJECTION =
            new ProjectionFilters(Collections.emptySet(), null, null, null, null);

        /** Filter by attribute name and value. */
        private static final short ATTRIBUTE_FILTER = 1;

        /** Filter by client or server nodes. */
        private static final short NODE_TYPE_FILTER = 2;

        /** Node id's. */
        private final Collection<UUID> nodeIds;

        /** Attributes. */
        private final Map<String, Object> attrs;

        /** Node type filter. {@code true} for server nodes, {@code false} for client nodes. */
        private final Boolean nodeType;

        /** Client-side predicate. */
        private final Predicate<ClusterNode> predicate;

        /**
         * Filter for result collection of nodes, applies when nodes has been selected on server side and filtered by
         * client-side predicates.
         *
         * Note: After first result filter is set all subsequent filters (both server-side and client-side) should be
         * applied only locally on list of nodes produced by result filter. These filters will be chainded to the result
         * filter.
         * For example, if we have next filter chain: forServers().forRandom().forAttribute(...) we can't apply
         * forServers and forAttribute on server-side and then apply forRandom filter, the result will be wrong.
         * We should filter server nodes (apply all filters before "random" result filter), get random node and after
         * random node filter is applied check that the node matching the attribute. */
        private final Function<List<ClusterNode>, List<ClusterNode>> resultFilter;

        /**
         * Default constructor.
         */
        ProjectionFilters() {
            nodeIds = null;
            attrs = null;
            nodeType = null;
            predicate = null;
            resultFilter = null;
        }

        /**
         * Constructor with null result filter.
         */
        ProjectionFilters(
            Collection<UUID> nodeIds,
            Map<String, Object> attrs,
            Boolean nodeType,
            Predicate<ClusterNode> predicate
        ) {
            this.nodeIds = nodeIds;
            this.attrs = attrs;
            this.nodeType = nodeType;
            this.predicate = predicate;
            resultFilter = null;
        }

        /**
         * Full constructor.
         */
        ProjectionFilters(
            Collection<UUID> nodeIds,
            Map<String, Object> attrs,
            Boolean nodeType,
            Predicate<ClusterNode> predicate,
            Function<List<ClusterNode>, List<ClusterNode>> resultFilter
        ) {
            this.nodeIds = nodeIds;
            this.attrs = attrs;
            this.nodeType = nodeType;
            this.predicate = predicate;
            this.resultFilter = resultFilter;
        }

        /**
         * @param nodeIds Node ids.
         */
        ProjectionFilters forNodeIds(Collection<UUID> nodeIds) {
            if (this == EMPTY_PROJECTION || nodeIds == null)
                return this;

            if (resultFilter != null)
                return forResultFilter(resultFilterForPredicate(predicateForNodeIds(nodeIds)));

            if (this.nodeIds != null) {
                if (this.nodeIds.equals(nodeIds))
                    return this;

                Set<UUID> nodeIdsIntersect = new HashSet<>(this.nodeIds);

                nodeIdsIntersect.retainAll(nodeIds);

                nodeIds = nodeIdsIntersect;
            }

            if (nodeIds.isEmpty())
                return EMPTY_PROJECTION;

            return new ProjectionFilters(nodeIds, attrs, nodeType, predicate);
        }

        /**
         * @param nodeType Node type ({@code true} for server, {@code false} for client).
         */
        ProjectionFilters forNodeType(boolean nodeType) {
            if (this == EMPTY_PROJECTION)
                return this;

            if (resultFilter != null)
                return forResultFilter(resultFilterForPredicate(predicateForNodeType(nodeType)));

            if (this.nodeType != null)
                return this.nodeType == nodeType ? this : EMPTY_PROJECTION;

            return new ProjectionFilters(nodeIds, attrs, nodeType, predicate);
        }

        /**
         * @param name Attribute name.
         * @param val Attribute value (or {@code null}).
         */
        ProjectionFilters forAttribute(String name, @Nullable Object val) {
            if (this == EMPTY_PROJECTION)
                return this;

            if (resultFilter != null)
                return forResultFilter(resultFilterForPredicate(predicateForAttribute(name, val)));

            if (attrs == null)
                return new ProjectionFilters(nodeIds, Collections.singletonMap(name, val), nodeType, predicate);

            Map<String, Object> attrsIntersect = new HashMap<>(attrs);

            Object oldVal = attrsIntersect.putIfAbsent(name, val);

            if (F.eq(val, oldVal))
                return this;

            return oldVal != null && val != null ? EMPTY_PROJECTION :
                new ProjectionFilters(nodeIds, attrsIntersect, nodeType, predicate);
        }

        /**
         * @param p Client-side node predicate.
         */
        ProjectionFilters forPredicate(Predicate<ClusterNode> p) {
            if (this == EMPTY_PROJECTION || p == null)
                return this;

            if (resultFilter != null)
                return forResultFilter(resultFilterForPredicate(p));

            return new ProjectionFilters(nodeIds, attrs, nodeType, predicate == null ? p : predicate.and(p));
        }

        /**
         * @param resultFilter Result filter.
         */
        ProjectionFilters forResultFilter(Function<List<ClusterNode>, List<ClusterNode>> resultFilter) {
            if (this == EMPTY_PROJECTION || resultFilter == null)
                return this;

            return new ProjectionFilters(nodeIds, attrs, nodeType, predicate, this.resultFilter == null ?
                resultFilter : this.resultFilter.andThen(resultFilter));
        }

        /**
         *
         */
        boolean hasOnlyNodeIdsFilters() {
            return nodeIds != null && nodeType == null && attrs == null && predicate == null && resultFilter == null;
        }

        /**
         *
         */
        boolean hasOnlyServerSideFilters() {
            return (nodeType != null || attrs != null) && predicate == null && resultFilter == null;
        }

        /**
         * Test node for client-side predicates. Assuming server-side predicates already tested on server-side.
         *
         * @param node Node.
         */
        boolean testClientSidePredicates(ClusterNode node) {
            if (nodeIds != null && !nodeIds.contains(node.id()))
                return false;

            return predicate == null || predicate.test(node);
        }

        /**
         * Test node for all predicates (including testing locally for server-side predicates).
         *
         * @param node Node.
         */
        boolean testAllPredicates(ClusterNode node) {
            if (nodeType != null && !predicateForNodeType(nodeType).test(node))
                return false;

            if (!F.isEmpty(attrs)) {
                for (Map.Entry<String, Object> attr : attrs.entrySet()) {
                    if (!predicateForAttribute(attr.getKey(), attr.getValue()).test(node))
                        return false;
                }
            }

            return testClientSidePredicates(node);
        }

        /**
         * @param nodes Nodes.
         */
        List<ClusterNode> applyResultFilter(List<ClusterNode> nodes) {
            return resultFilter != null ? resultFilter.apply(nodes) : nodes;
        }

        /**
         * Creates filter using given predicate.
         *
         * @param p Predicate.
         */
        static Function<List<ClusterNode>, List<ClusterNode>> resultFilterForPredicate(Predicate<ClusterNode> p) {
            return nodes -> {
                if (F.isEmpty(nodes))
                    return nodes;

                ArrayList<ClusterNode> nodes0 = new ArrayList<>();

                for (ClusterNode node : nodes) {
                    if (p.test(node))
                        nodes0.add(node);
                }

                return nodes0;
            };
        }

        /**
         * Creates client-side predicate for {@code forNodeIds} filter.
         *
         * @param nodeIds Node IDs.
         */
        static Predicate<ClusterNode> predicateForNodeIds(Collection<UUID> nodeIds) {
            return node -> nodeIds.contains(node.id());
        }

        /**
         * Creates client-side predicate for {@code forNodeType} filter.
         *
         * @param nodeType Node type ({@code true} for server, {@code false} for client).
         */
        static Predicate<ClusterNode> predicateForNodeType(boolean nodeType) {
            return node -> nodeType == !node.isClient();
        }

        /**
         * Creates client-side predicate for {@code forAttribute} filter.
         *
         * @param name Attribute name.
         * @param val Attribute value (or {@code null}).
         */
        static Predicate<ClusterNode> predicateForAttribute(String name, @Nullable Object val) {
            return val == null ? node -> node.attributes().containsKey(name) : node -> val.equals(node.attribute(name));
        }

        /**
         * @param writer Writer.
         */
        void write(BinaryRawWriterEx writer) {
            int size = (attrs == null ? 0 : attrs.size()) + (nodeType == null ? 0 : 1);

            writer.writeInt(size);

            if (!F.isEmpty(attrs)) {
                for (Map.Entry<String, Object> entry : attrs.entrySet()) {
                    writer.writeShort(ATTRIBUTE_FILTER);
                    writer.writeString(entry.getKey());
                    writer.writeObject(entry.getValue());
                }
            }

            if (nodeType != null) {
                writer.writeShort(NODE_TYPE_FILTER);
                writer.writeBoolean(nodeType);
            }
        }
    }
}
